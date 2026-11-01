using System.Collections.Frozen;
using System.Runtime.CompilerServices;
using System.Threading.Channels;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Relayer.Transports;

namespace Relayer.Outbox.PostgreSql;

internal sealed class PostgreSqlDirectDispatcher : BackgroundService
{
    private readonly PostgreSqlOutboxStore _store;
    private readonly PostgreSqlDirectDispatchOptions _options;
    private readonly ILogger<PostgreSqlDirectDispatcher> _logger;
    private readonly Channel<OutboxDispatchBatch> _queue;
    private readonly OutboxDispatchBatch?[] _pool;
    private readonly FrozenDictionary<string, ITransport> _transports;
    private readonly Dictionary<(string Transport, string Destination), TransportCapabilities> _capabilities = [];
    private readonly OutgoingMessage[] _messages;
    private readonly OutboxClaim[] _claims;
    private readonly string _schema;
    private readonly string _prefix;

    internal PostgreSqlDirectDispatcher(Npgsql.NpgsqlDataSource dataSource, PostgreSqlOutboxOptions storeOptions,
        PostgreSqlDirectDispatchOptions options, IEnumerable<ITransport> transports, ILogger<PostgreSqlDirectDispatcher> logger)
    {
        _store = new(dataSource, storeOptions);
        _schema = storeOptions.Schema ?? "public";
        _prefix = storeOptions.TablePrefix;
        _options = options;
        _logger = logger;
        _transports = transports.ToFrozenDictionary(static transport => transport.Name, StringComparer.Ordinal);
        _queue = Channel.CreateBounded<OutboxDispatchBatch>(new BoundedChannelOptions(options.QueueCapacity)
        {
            SingleReader = true,
            SingleWriter = false,
            AllowSynchronousContinuations = false,
            FullMode = BoundedChannelFullMode.Wait
        });
        _pool = new OutboxDispatchBatch?[options.QueueCapacity + 1];
        _messages = new OutgoingMessage[options.BatchSize];
        _claims = new OutboxClaim[options.BatchSize];
    }

    internal void ValidateStore(string? schema, string prefix)
    {
        if (_schema != (schema ?? "public") || _prefix != prefix)
            throw new InvalidOperationException("The EF Core outbox and its direct dispatcher must use identical PostgreSQL schema and table prefix settings.");
    }

    // Called only after durable commit. Queue pressure and shutdown cannot make a committed transaction fail.
    internal void Committed(List<BufferedOutboxMessage> rows, int count)
    {
        OutboxDispatchBatch? batch = null;
        for (var i = 0; i < _pool.Length; i++)
            if ((batch = Interlocked.Exchange(ref _pool[i], null)) is not null) break;
        batch ??= new();
        batch.Take(rows, count, DateTimeOffset.UtcNow);
        if (batch.Count == 0) { Return(batch); return; }
        if (_queue.Writer.TryWrite(batch)) return;
        DirectDispatchLog.NotQueued(_logger, batch.Count);
        Return(batch);
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        // The queue carries payloads alone: no committing Activity or scoped service travels with a batch.
        try
        {
            while (await _queue.Reader.WaitToReadAsync(stoppingToken).ConfigureAwait(false))
                while (_queue.Reader.TryRead(out var batch))
                {
                    try { await DispatchAsync(batch, stoppingToken).ConfigureAwait(false); }
                    catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested) { return; }
                    catch (Exception error) { DirectDispatchLog.Failed(_logger, error); }
                    finally { Return(batch); }
                }
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested) { }
        finally
        {
            _queue.Writer.TryComplete();
            while (_queue.Reader.TryRead(out var batch)) Return(batch);
        }
    }

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        _queue.Writer.TryComplete();
        try
        {
            if (ExecuteTask is { } task) await task.WaitAsync(cancellationToken).ConfigureAwait(false);
        }
        finally { await base.StopAsync(cancellationToken).ConfigureAwait(false); }
    }

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    private async ValueTask DispatchAsync(OutboxDispatchBatch batch, CancellationToken cancellationToken)
    {
        batch.Sort();
        var offset = 0;
        while (offset < batch.Count)
        {
            var first = batch.Rows[offset];
            _transports.TryGetValue(first.Transport, out var transport);
            var limit = 1;
            if (transport is not null)
            {
                var key = (first.Transport, first.Destination);
                if (!_capabilities.TryGetValue(key, out var capabilities))
                {
                    capabilities = transport.GetCapabilities(first.Destination);
                    if (capabilities.MaxBatchSize <= 0)
                        throw new InvalidOperationException("Outbox dispatch requires a positive native MaxBatchSize.");
                    _capabilities.Add(key, capabilities);
                }
                limit = Math.Min(_options.BatchSize, capabilities.MaxBatchSize);
            }
            var count = 0;
            try
            {
                while (offset < batch.Count && count < limit)
                {
                    var row = batch.Rows[offset];
                    if (row.Transport != first.Transport || row.Destination != first.Destination) break;
                    offset++;
                    var claim = new OutboxClaim(row.Id, Guid.NewGuid());
                    var now = DateTimeOffset.UtcNow;
                    if (!await _store.TryClaimAsync(claim, now, now + _options.ClaimDuration, cancellationToken).ConfigureAwait(false)) continue;
                    _claims[count] = claim;
                    _messages[count++] = row.ToMessage();
                }
                // Claiming a large batch can take time. Do not send rows whose ownership expired meanwhile.
                var live = 0;
                for (var i = 0; i < count; i++)
                {
                    var now = DateTimeOffset.UtcNow;
                    if (!await _store.RenewClaimAsync(_claims[i], now, now + _options.ClaimDuration, cancellationToken).ConfigureAwait(false)) continue;
                    _claims[live] = _claims[i];
                    _messages[live++] = _messages[i];
                }
                // Remove stale slots even if the send or completion fails.
                Array.Clear(_messages, live, count - live);
                Array.Clear(_claims, live, count - live);
                count = live;
                if (count == 0) continue;
                if (transport is null)
                    throw new InvalidOperationException($"Outbox row references unregistered transport '{first.Transport}'. Register its named ITransport.");
                if (count == 1) await transport.SendAsync(_messages[0], cancellationToken).ConfigureAwait(false);
                else await transport.SendBatchAsync(_messages.AsMemory(0, count), cancellationToken).ConfigureAwait(false);
                await _store.CompleteBatchAsync(_claims.AsMemory(0, count), DateTimeOffset.UtcNow, cancellationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested) { throw; }
            catch (Exception error)
            {
                DirectDispatchLog.Failed(_logger, error);
                for (var i = 0; i < count; i++)
                {
                    try
                    {
                        await _store.RecordFailureAsync(_claims[i], error.Message, DateTimeOffset.UtcNow, cancellationToken).ConfigureAwait(false);
                    }
                    catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested) { throw; }
                    catch (Exception failure) { DirectDispatchLog.Failed(_logger, failure); }
                }
            }
            finally
            {
                Array.Clear(_messages, 0, count);
                Array.Clear(_claims, 0, count);
            }
        }
    }

    private void Return(OutboxDispatchBatch batch)
    {
        batch.Clear();
        for (var i = 0; i < _pool.Length; i++)
            if (Interlocked.CompareExchange(ref _pool[i], batch, null) is null) return;
    }
}

internal static partial class DirectDispatchLog
{
    [LoggerMessage(1, LogLevel.Error, "Direct outbox dispatch queue is full or stopped. {Count} committed rows remain pending; recovery dispatch is required.")]
    internal static partial void NotQueued(ILogger logger, int count);

    [LoggerMessage(2, LogLevel.Error, "Direct outbox dispatch failed. Durable rows remain available for recovery after claim expiry or failure recording.")]
    internal static partial void Failed(ILogger logger, Exception exception);
}
