using System.Collections.Frozen;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using System.Threading.Channels;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Relayer.Transports;

namespace Relayer.Outbox.SqlServer;

internal sealed class SqlServerDirectDispatcher : BackgroundService
{
    private readonly SqlServerOutboxStore _store;
    private readonly SqlServerDirectDispatchOptions _options;
    private readonly ILogger<SqlServerDirectDispatcher> _logger;
    private readonly Channel<OutboxDispatchBatch> _queue;
    private readonly OutboxDispatchBatch?[] _pool;
    private readonly FrozenDictionary<string, ITransport> _transports;
    private readonly DirectDispatchCapabilityCache _capabilities = new();
    private readonly OutgoingMessage[] _messages;
    private readonly OutboxClaim[] _claims;
    private readonly bool[] _claimed;
    private readonly DateTimeOffset[] _created;
    private readonly RelayerInstrumentation? _instrumentation;
    private long _recoveredCompletions;
    private readonly string _schema;
    private readonly string _prefix;
    private readonly string _dataSource;
    private readonly string _database;

    [RequiresUnreferencedCode("SqlClient 7.1.1 runtime trimming is unsupported; see https://github.com/dotnet/SqlClient/issues/1947.")]
    [RequiresDynamicCode("SqlClient 7.1.1 runtime Native AOT is unsupported; see https://github.com/dotnet/SqlClient/issues/1947.")]
    internal SqlServerDirectDispatcher(string connectionString, SqlServerOutboxOptions storeOptions,
        SqlServerDirectDispatchOptions options, IEnumerable<ITransport> transports, ILogger<SqlServerDirectDispatcher> logger,
        SqlServerOutboxStore? store = null, RelayerInstrumentation? instrumentation = null)
    {
        _store = store ?? new(connectionString, storeOptions);
        var target = ValidateConnectionString(connectionString);
        _dataSource = target.DataSource;
        _database = target.InitialCatalog;
        _schema = storeOptions.Schema ?? "dbo";
        _prefix = storeOptions.TablePrefix;
        _options = options;
        _instrumentation = instrumentation;
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
        _claimed = new bool[options.BatchSize];
        _created = new DateTimeOffset[options.BatchSize];
    }

    internal void ValidateStore(string? schema, string prefix)
    {
        if (_schema != (schema ?? "dbo") || _prefix != prefix)
            throw new InvalidOperationException("The SQL Server outbox and its direct dispatcher must use identical schema and table prefix settings.");
    }

    internal static SqlConnectionStringBuilder ValidateConnectionString(string connectionString)
    {
        var target = new SqlConnectionStringBuilder(connectionString);
        if (string.IsNullOrWhiteSpace(target.InitialCatalog))
            throw new ArgumentException("SQL Server direct dispatch requires an explicit Initial Catalog (Database) in its connection string so writer and dispatcher targets can be validated without opening a second pooled connection.", nameof(connectionString));
        return target;
    }

    internal void ValidateConnection(SqlConnection writer)
    {
        if (!StringComparer.OrdinalIgnoreCase.Equals(writer.DataSource, _dataSource) ||
            !StringComparer.Ordinal.Equals(writer.Database, _database))
            throw new InvalidOperationException("The SQL Server outbox transaction and its direct dispatcher must use the same server and database.");
    }

    // Called only after durable commit. Queue pressure and shutdown cannot make a committed transaction fail.
    internal void Committed(List<SqlServerBufferedMessage> rows, int count)
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

    [RuntimeAsyncMethodGeneration(false)]
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    private async ValueTask DispatchAsync(OutboxDispatchBatch batch, CancellationToken cancellationToken, bool recordInvalid = false)
    {
        batch.Sort();
        var offset = 0;
        while (offset < batch.Count)
        {
            var first = batch.Rows[offset];
            ITransport transport;
            int limit;
            try
            {
                if (!_transports.TryGetValue(first.Transport, out transport!))
                    throw new InvalidOperationException($"Outbox row references unregistered transport '{first.Transport}'. Register its named ITransport.");
                var capabilities = _capabilities.Get(transport, first.Destination);
                limit = Math.Min(_options.BatchSize, capabilities.MaxBatchSize);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested) { throw; }
            catch (Exception error)
            {
                DirectDispatchLog.Failed(_logger, error);
                // Fence and record invalid destinations independently, then continue unrelated groups.
                do
                {
                    var id = batch.Rows[offset++].Id;
                    if (!recordInvalid) continue;
                    var claim = new OutboxClaim(id, Guid.NewGuid());
                    var now = DateTimeOffset.UtcNow;
                    if (await _store.TryClaimAsync(claim, now, now + _options.ClaimDuration, cancellationToken).ConfigureAwait(false))
                        await _store.RecordFailureAsync(claim, error.Message, DateTimeOffset.UtcNow, cancellationToken).ConfigureAwait(false);
                }
                while (offset < batch.Count && batch.Rows[offset].Transport == first.Transport &&
                    batch.Rows[offset].Destination == first.Destination);
                continue;
            }
            var count = 0;
            try
            {
                while (offset < batch.Count && count < limit)
                {
                    var row = batch.Rows[offset];
                    if (row.Transport != first.Transport || row.Destination != first.Destination) break;
                    offset++;
                    _claims[count] = new(row.Id, Guid.NewGuid());
                    _created[count] = row.CreatedAt;
                    _messages[count++] = row.ToMessage();
                }
                var now = DateTimeOffset.UtcNow;
                var expires = now + _options.ClaimDuration;
                await _store.ClaimBatchAsync(_claims.AsMemory(0, count), _claimed.AsMemory(0, count), now, expires, cancellationToken).ConfigureAwait(false);
                var live = CompactClaims(_messages.AsSpan(0, count), _claims.AsSpan(0, count), _claimed.AsSpan(0, count),
                    _created.AsSpan(0, count));
                if (live == 0) continue;
                // A slow database response can consume the lease. Never start a send with known expired ownership.
                if (expires <= DateTimeOffset.UtcNow)
                {
                    DirectDispatchLog.ClaimExpired(_logger, live);
                    continue;
                }
                try
                {
                    if (_instrumentation is not null)
                    {
                        var dispatchTime = DateTimeOffset.UtcNow;
                        for (var i = 0; i < live; i++) _instrumentation.RecordOutboxDispatchLag(dispatchTime - _created[i]);
                    }
                    if (live == 1) await transport.SendAsync(_messages[0], cancellationToken).ConfigureAwait(false);
                    else await transport.SendBatchAsync(_messages.AsMemory(0, live), cancellationToken).ConfigureAwait(false);
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested) { throw; }
                catch (Exception error)
                {
                    DirectDispatchLog.Failed(_logger, error);
                    for (var i = 0; i < live; i++)
                    {
                        try
                        {
                            await _store.RecordFailureAsync(_claims[i], error.Message, DateTimeOffset.UtcNow, cancellationToken).ConfigureAwait(false);
                        }
                        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested) { throw; }
                        catch (Exception failure) { DirectDispatchLog.Failed(_logger, failure); }
                    }
                    continue;
                }
                // Once the broker succeeds, database errors must not consume send attempts or release claims.
                try
                {
                    var completed = await _store.CompleteBatchAsync(_claims.AsMemory(0, live), DateTimeOffset.UtcNow, cancellationToken).ConfigureAwait(false);
                    if (recordInvalid) _recoveredCompletions += completed;
                    else _instrumentation?.AddOutboxPending(-completed);
                    if (completed != live) DirectDispatchLog.Incomplete(_logger, completed, live);
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested) { throw; }
                catch (Exception error) { DirectDispatchLog.CompletionFailed(_logger, error); }
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested) { throw; }
            catch (Exception error) { DirectDispatchLog.Failed(_logger, error); }
            finally
            {
                Array.Clear(_messages, 0, count);
                Array.Clear(_claims, 0, count);
            }
        }
    }

    // The maintenance worker owns this dispatcher and calls this once per transaction.
    internal void FinishRecovery(bool committed)
    {
        var completed = _recoveredCompletions;
        _recoveredCompletions = 0;
        if (committed && completed != 0) _instrumentation?.AddOutboxPending(-completed);
    }

    internal ValueTask DispatchRecoveredAsync(OutboxDispatchBatch batch, CancellationToken cancellationToken) =>
        DispatchAsync(batch, cancellationToken, recordInvalid: true);

    internal static int CompactClaims(Span<OutgoingMessage> messages, Span<OutboxClaim> claims, ReadOnlySpan<bool> claimed,
        Span<DateTimeOffset> created = default)
    {
        var live = 0;
        for (var i = 0; i < claimed.Length; i++)
        {
            if (!claimed[i]) continue;
            if (!created.IsEmpty) created[live] = created[i];
            claims[live] = claims[i];
            messages[live++] = messages[i];
        }
        return live;
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

    [LoggerMessage(3, LogLevel.Error, "Direct outbox completion failed after a successful send. Claims remain held until expiry; recovery may duplicate delivery. Send attempts are unchanged.")]
    internal static partial void CompletionFailed(ILogger logger, Exception exception);

    [LoggerMessage(4, LogLevel.Warning, "Direct outbox completion deleted {Completed} of {Sent} sent rows. Claims expired or changed; recovery may duplicate delivery. Increase ClaimDuration if native send latency exceeds the lease.")]
    internal static partial void Incomplete(ILogger logger, long completed, int sent);

    [LoggerMessage(5, LogLevel.Warning, "Direct outbox claims expired before sending {Count} rows. Rows remain pending for recovery. Increase ClaimDuration if database latency exceeds the lease.")]
    internal static partial void ClaimExpired(ILogger logger, int count);
}
