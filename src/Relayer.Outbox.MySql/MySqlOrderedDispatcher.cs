using System.Runtime.CompilerServices;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using MySqlConnector;
using Relayer.Transports;

namespace Relayer.Outbox.MySql;

internal sealed partial class MySqlOrderedDispatcher : BackgroundService
{
    private readonly MySqlDataSource _source;
    private readonly MySqlOrderedDispatchOptions _options;
    private readonly MySqlLaneCoordinator _coordinator;
    private readonly MySqlOrderedLane _lane;
    private readonly Dictionary<string, ITransport> _transports = new(StringComparer.Ordinal);
    private readonly OutboxRecord[] _rows;
    private readonly bool[] _ready;
    private readonly OrderedDispatchCapabilityCache _capabilities = new();
    private readonly ILogger<MySqlOrderedDispatcher> _logger;
    private readonly TimeProvider _timeProvider;
    private readonly RelayerInstrumentation? _instrumentation;

    internal MySqlOrderedDispatcher(MySqlDataSource source, MySqlOrderedDispatchOptions options,
        IEnumerable<ITransport> transports, ILogger<MySqlOrderedDispatcher> logger, TimeProvider? timeProvider = null, RelayerInstrumentation? instrumentation = null)
    {
        MySqlOutboxStore.ValidateDataSource(source);
        _source = source;
        _instrumentation = instrumentation;
        _options = options;
        var store = options.Validate();
        _coordinator = new(source, options);
        _lane = new(source, store, options.LeaseDuration, options.MaxDispatchAttempts);
        _rows = new OutboxRecord[options.BatchSize];
        _ready = new bool[options.Lanes];
        _logger = logger;
        _timeProvider = timeProvider ?? TimeProvider.System;
        foreach (var transport in transports)
            if (!_transports.TryAdd(transport.Name, transport))
                throw new InvalidOperationException($"Transport '{transport.Name}' is registered more than once.");
        if (_transports.Count == 0)
            throw new InvalidOperationException("Ordered outbox dispatch requires at least one named ITransport adapter.");
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        // Reuse the deadline source across successful batches. Cancellation is an exceptional path.
        var deadline = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);
        long? lastRebalance = null;
        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                var madeProgress = false;
                try
                {
                    await RebalanceIfDueAsync().ConfigureAwait(false);
                    await _lane.FindReadyAsync(_coordinator.Assigned, _ready, stoppingToken).ConfigureAwait(false);
                    for (var lane = 0; lane < _options.Lanes; lane++)
                    {
                        if (!_ready[lane] || !_coordinator.Assigned[lane]) continue;
                        // A preceding slow batch can consume the heartbeat interval. Refresh before renewing another lane.
                        await RebalanceIfDueAsync().ConfigureAwait(false);
                        if (!_coordinator.Assigned[lane]) continue;
                        deadline.CancelAfter(_options.LeaseDuration / 2);
                        try
                        {
                            madeProgress |= await DispatchLaneAsync(lane, deadline.Token).ConfigureAwait(false) > 0;
                        }
                        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested) { throw; }
                        catch (Exception error) { DispatchFailed(_logger, error); }
                        finally
                        {
                            deadline.CancelAfter(Timeout.InfiniteTimeSpan);
                            if (!deadline.TryReset())
                            {
                                deadline.Dispose();
                                deadline = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);
                            }
                        }
                    }
                }
                catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested) { break; }
                catch (Exception error) { DispatchFailed(_logger, error); }
                if (!madeProgress)
                    await Task.Delay(_options.PollInterval, _timeProvider, stoppingToken).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested) { }
        finally
        {
            deadline.Dispose();
            using var cleanup = new CancellationTokenSource(_options.LeaseDuration / 2);
            try { await _coordinator.ReleaseAsync(cleanup.Token).ConfigureAwait(false); }
            catch (Exception error) { ReleaseFailed(_logger, error); }
        }

        [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
        async ValueTask RebalanceIfDueAsync()
        {
            if (lastRebalance is { } last && _timeProvider.GetElapsedTime(last) < _options.LeaseDuration / 3) return;
            var started = _timeProvider.GetTimestamp();
            await _coordinator.RebalanceAsync(stoppingToken).ConfigureAwait(false);
            lastRebalance = started;
        }
    }

    public override void Dispose()
    {
        base.Dispose();
        _lane.Dispose();
        _coordinator.Dispose();
    }

    // One connection and lease-row transaction lock per batch. Never run concurrent sends within a lane.
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    internal async ValueTask<int> DispatchLaneAsync(int lane, CancellationToken cancellationToken)
    {
        var started = _timeProvider.GetTimestamp();
        // Reserve half the hard deadline for the final admitted send and its database commit.
        var admissionBudget = _options.LeaseDuration / 4;
        await using var connection = await _source.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var transaction = await connection.BeginTransactionAsync(System.Data.IsolationLevel.ReadCommitted, cancellationToken).ConfigureAwait(false);
        if (!await _coordinator.AcquireAsync(connection, transaction, lane, cancellationToken).ConfigureAwait(false)) return 0;
        var count = 0;
        var sent = 0;
        var poisoned = false;
        try
        {
            count = await _lane.ReadAsync(connection, transaction, lane, _rows, cancellationToken).ConfigureAwait(false);
            for (var index = 0; index < count; index++)
            {
                cancellationToken.ThrowIfCancellationRequested();
                if (_timeProvider.GetElapsedTime(started) >= admissionBudget) break;
                var row = _rows[index];
                var claim = new OutboxClaim(row.Id, Guid.NewGuid());
                if (!await _lane.ClaimAsync(connection, transaction, claim, cancellationToken).ConfigureAwait(false)) break;
                try
                {
                    var prepared = Prepare(row, _transports, _capabilities);
                    _instrumentation?.RecordOutboxDispatchLag(_timeProvider.GetUtcNow() - row.CreatedAt);
                    await prepared.Transport.SendAsync(prepared.Message, cancellationToken).ConfigureAwait(false);
                }
                catch (Exception error) when (!cancellationToken.IsCancellationRequested)
                {
                    if (!await _lane.FailAsync(connection, transaction, claim, error.Message, cancellationToken).ConfigureAwait(false))
                        throw new InvalidOperationException("Ordered outbox claim expired while recording a dispatch failure.", error);
                    poisoned = row.Attempts + 1 >= _options.MaxDispatchAttempts;
                    DispatchFailed(_logger, error);
                    break;
                }
                // Do not advance to another row after an uncertain completion. Rollback safely permits duplicates.
                if (!await _lane.CompleteAsync(connection, transaction, claim, cancellationToken).ConfigureAwait(false))
                    throw new InvalidOperationException("Ordered outbox claim expired after native send. The batch remains available for recovery.");
                sent++;
            }
            await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
            _instrumentation?.AddOutboxPending(-sent);
            if (poisoned) _instrumentation?.RecordOutboxPoison();
            return sent;
        }
        finally { Array.Clear(_rows); }
    }

    internal static (ITransport Transport, OutgoingMessage Message) Prepare(OutboxRecord row,
        Dictionary<string, ITransport> transports,
        OrderedDispatchCapabilityCache capabilities)
    {
        if (!transports.TryGetValue(row.TransportName, out var transport))
            throw new InvalidOperationException($"Outbox row references unknown transport '{row.TransportName}'. Register its named ITransport adapter.");
        if (row.Message.OrderingKey is null)
            throw new InvalidOperationException("An ordered outbox row must contain its original ordering key.");
        _ = capabilities.Get(transport, row.Message.Destination);
        // The database already enforces deliver_at. Do not schedule again in the native client.
        var message = row.Message;
        return (transport, new OutgoingMessage(message.Destination, message.Body, message.Headers, message.OrderingKey).ForOutboxDispatch());
    }

    [LoggerMessage(Level = LogLevel.Error, Message = "Ordered outbox dispatch failed; durable rows remain available for retry or, after attempt exhaustion, dead-letter management.")]
    private static partial void DispatchFailed(ILogger logger, Exception error);
    [LoggerMessage(Level = LogLevel.Warning, Message = "Could not release ordered outbox leases; they expire automatically.")]
    private static partial void ReleaseFailed(ILogger logger, Exception error);
}
