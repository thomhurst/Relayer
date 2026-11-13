using System.Runtime.CompilerServices;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Npgsql;
using Relayer.Transports;

namespace Relayer.Outbox.PostgreSql;

internal sealed partial class PostgreSqlOrderedDispatcher : BackgroundService
{
    private readonly NpgsqlDataSource _source;
    private readonly PostgreSqlDispatchFailures _failures;
    private readonly PostgreSqlOrderedDispatchOptions _options;
    private readonly PostgreSqlLaneCoordinator _coordinator;
    private readonly PostgreSqlOrderedLane _lane;
    private readonly Dictionary<string, ITransport> _transports = new(StringComparer.Ordinal);
    private readonly OutboxRecord[] _rows;
    private readonly bool[] _ready;
    private int _nextLane;
    private int _localStop;
    private readonly CancellationTokenSource _idleStop = new();
    private readonly bool _hasLocal;
    private readonly OrderedDispatchCapabilityCache _capabilities = new();
    private readonly ILogger<PostgreSqlOrderedDispatcher> _logger;
    private readonly TimeProvider _timeProvider;
    private readonly RelayerInstrumentation? _instrumentation;

    internal PostgreSqlOrderedDispatcher(NpgsqlDataSource source, PostgreSqlOrderedDispatchOptions options,
        IEnumerable<ITransport> transports, ILogger<PostgreSqlOrderedDispatcher> logger, TimeProvider? timeProvider = null, RelayerInstrumentation? instrumentation = null, IServiceScopeFactory? scopes = null)
    {
        _failures = new(scopes);
        _source = source;
        _instrumentation = instrumentation;
        _options = options;
        var store = options.Validate();
        _coordinator = new(source, options);
        _lane = new(source, store, options.LeaseDuration);
        _rows = new OutboxRecord[options.BatchSize];
        _ready = new bool[options.Lanes];
        _logger = logger;
        _timeProvider = timeProvider ?? TimeProvider.System;
        foreach (var transport in transports)
            if (!_transports.TryAdd(transport.Name, transport))
                throw new InvalidOperationException($"Transport '{transport.Name}' is registered more than once.");
        _hasLocal = _transports.Values.Any(static transport => PostgreSqlLocalTransport.Find(transport) is not null);
        if (_transports.Count == 0)
            throw new InvalidOperationException("Ordered outbox dispatch requires at least one named ITransport adapter.");
    }

    public override async Task StartAsync(CancellationToken cancellationToken)
    {
        await PostgreSqlSentSchema.ValidateAsync(_source, _options.RetainSentMessages, _options.Schema,
            _options.TablePrefix, cancellationToken).ConfigureAwait(false);
        await base.StartAsync(cancellationToken).ConfigureAwait(false);
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        // Reuse the deadline source across successful batches. Cancellation is an exceptional path.
        var deadline = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);
        using var idleStop = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken, _idleStop.Token);
        long? lastRebalance = null;
        try
        {
            while (!stoppingToken.IsCancellationRequested && Volatile.Read(ref _localStop) == 0)
            {
                var madeProgress = false;
                try
                {
                    await RebalanceIfDueAsync().ConfigureAwait(false);
                    await _lane.FindReadyAsync(_coordinator.Assigned, _ready, stoppingToken).ConfigureAwait(false);
                    for (var lane = 0; lane < _options.Lanes; lane++)
                    {
                        if (Volatile.Read(ref _localStop) != 0) break;
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
                    await Task.Delay(_options.PollInterval, _timeProvider, idleStop.Token).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException) when (idleStop.IsCancellationRequested) { }
        finally
        {
            deadline.Dispose();
            using var cleanup = new CancellationTokenSource(_options.LeaseDuration / 2);
            try { await _coordinator.ReleaseAsync(cleanup.Token).ConfigureAwait(false); }
            catch (Exception error) { ReleaseFailed(_logger, error); }
        }

        async ValueTask RebalanceIfDueAsync()
        {
            if (lastRebalance is { } last && _timeProvider.GetElapsedTime(last) < _options.LeaseDuration / 3) return;
            var started = _timeProvider.GetTimestamp();
            await _coordinator.RebalanceAsync(stoppingToken).ConfigureAwait(false);
            lastRebalance = started;
        }
    }

    // Reuse native membership, readiness probes and fenced lane transactions in one catalog turn.
    [RuntimeAsyncMethodGeneration(false)]
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    internal async ValueTask DispatchPassAsync(TimeSpan remainingTurn, CancellationToken cancellationToken)
    {
        var started = _timeProvider.GetTimestamp();
        await _coordinator.RebalanceAsync(cancellationToken).ConfigureAwait(false);
        await _lane.FindReadyAsync(_coordinator.Assigned, _ready, cancellationToken).ConfigureAwait(false);
        var deadline = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        try
        {
            for (var visited = 0; visited < _options.Lanes; visited++)
            {
                cancellationToken.ThrowIfCancellationRequested();
                var lane = _nextLane;
                // Advance before dispatch: an expired tenant turn resumes at the following lane.
                _nextLane = (lane + 1) % _options.Lanes;
                if (!_ready[lane] || !_coordinator.Assigned[lane]) continue;
                deadline.CancelAfter(_options.LeaseDuration / 2);
                var remaining = remainingTurn - _timeProvider.GetElapsedTime(started);
                if (remaining <= TimeSpan.Zero) return;
                try { await DispatchLaneAsync(lane, deadline.Token, remaining / 2).ConfigureAwait(false); }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested) { throw; }
                catch (Exception error) { DispatchFailed(_logger, error); }
                finally
                {
                    deadline.CancelAfter(Timeout.InfiniteTimeSpan);
                    if (!deadline.TryReset())
                    {
                        deadline.Dispose();
                        deadline = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
                    }
                }
            }
        }
        finally { deadline.Dispose(); }
    }

    internal ValueTask ReleaseAsync(CancellationToken cancellationToken) => _coordinator.ReleaseAsync(cancellationToken);

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        if (!_hasLocal) { await base.StopAsync(cancellationToken).ConfigureAwait(false); return; }
        Interlocked.Exchange(ref _localStop, 1);
        // Wake an idle scan without cancelling the token draining active local deliveries.
        await _idleStop.CancelAsync().ConfigureAwait(false);
        try { if (ExecuteTask is { } task) await task.WaitAsync(cancellationToken).ConfigureAwait(false); }
        finally { await base.StopAsync(cancellationToken).ConfigureAwait(false); }
    }

    public override void Dispose()
    {
        base.Dispose();
        _idleStop.Dispose();
        _lane.Dispose();
    }

    // One connection and lease-row transaction lock per batch. Never run concurrent sends within a lane.
    [RuntimeAsyncMethodGeneration(false)]
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    internal async ValueTask<int> DispatchLaneAsync(int lane, CancellationToken cancellationToken, TimeSpan? turnAdmissionBudget = null)
    {
        var started = _timeProvider.GetTimestamp();
        // Reserve half the hard deadline for the final admitted send and its database commit.
        var admissionBudget = _options.LeaseDuration / 4;
        if (turnAdmissionBudget is { } turnBudget && turnBudget < admissionBudget)
            admissionBudget = turnBudget;
        await using var connection = await _source.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);
        if (!await _coordinator.AcquireAsync(connection, transaction, lane, cancellationToken).ConfigureAwait(false)) return 0;
        var count = 0;
        var sent = 0;
        var poisoned = false;
        try
        {
            count = await _lane.ReadAsync(connection, transaction, lane, _rows, cancellationToken).ConfigureAwait(false);
            for (var index = 0; index < count && Volatile.Read(ref _localStop) == 0; index++)
            {
                cancellationToken.ThrowIfCancellationRequested();
                if (_timeProvider.GetElapsedTime(started) >= admissionBudget) break;
                var row = _rows[index];
                var localTransport = _transports.TryGetValue(row.TransportName, out var registered) ? PostgreSqlLocalTransport.Find(registered) : null;
                var localAdmission = localTransport?.TryEnter(row.Message.Destination);
                if (localTransport is not null && localAdmission is null) break;
                try
                {
                    var claim = new OutboxClaim(row.Id, Guid.NewGuid());
                    if (!await _lane.ClaimAsync(connection, transaction, claim, cancellationToken).ConfigureAwait(false)) break;
                    var preparedForSend = false;
                    var nativeSend = false;
                    try
                    {
                        var prepared = Prepare(row, _transports, _capabilities);
                        _instrumentation?.RecordOutboxDispatchLag(_timeProvider.GetUtcNow() - row.CreatedAt);
                        if (PostgreSqlLocalTransport.Find(prepared.Transport) is { } local)
                        {
                            local.ValidateStore(_source, _options.Schema, _options.TablePrefix, _options.RetainSentMessages, _options.MaxDispatchAttempts);
                            preparedForSend = true;
                            await local.ConsumeAsync(prepared.Message, claim, row.Attempts + 1, transaction, cancellationToken).ConfigureAwait(false);
                        }
                        else
                        {
                            preparedForSend = true;
                            nativeSend = true;
                            await prepared.Transport.SendAsync(prepared.Message, cancellationToken).ConfigureAwait(false);
                        }
                    }
                    catch (Exception error) when (!cancellationToken.IsCancellationRequested)
                    {
                        var action = preparedForSend
                            ? await _failures.ClassifyAsync(error, row.Id, row.MessageType, row.Attempts + 1, row.TenantId, row.Message, cancellationToken).ConfigureAwait(false)
                            : FailureAction.Retry;
                        if (action == FailureAction.Discard)
                        {
                            if (!await _lane.CompleteAsync(connection, transaction, claim, cancellationToken).ConfigureAwait(false))
                                throw new InvalidOperationException("Ordered outbox claim expired while discarding a failed send.", error);
                            sent++;
                            continue;
                        }
                        if (action is not (FailureAction.Retry or FailureAction.DeadLetter))
                            throw new InvalidOperationException("The dispatch failure classifier returned an unknown action.");
                        var permanent = action == FailureAction.DeadLetter;
                        if (!await _lane.FailAsync(connection, transaction, claim, error.Message, cancellationToken, permanent, retryable: nativeSend).ConfigureAwait(false))
                            throw new InvalidOperationException("Ordered outbox claim expired while recording a dispatch failure.", error);
                        poisoned = permanent || (!nativeSend && row.Attempts + 1 >= _options.MaxDispatchAttempts);
                        DispatchFailed(_logger, error);
                        break;
                    }
                    // Do not advance to another row after an uncertain completion. Rollback safely permits duplicates.
                    if (PostgreSqlLocalTransport.Find(_transports[row.TransportName]) is null &&
                        !await _lane.CompleteAsync(connection, transaction, claim, cancellationToken).ConfigureAwait(false))
                        throw new InvalidOperationException("Ordered outbox claim expired after native send. The batch remains available for recovery.");
                    sent++;
                }
                finally { localAdmission?.Exit(); }
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

    [LoggerMessage(Level = LogLevel.Error, Message = "Ordered outbox dispatch failed; retryable sends retain durable backoff; permanent failures require dead-letter management.")]
    private static partial void DispatchFailed(ILogger logger, Exception error);
    [LoggerMessage(Level = LogLevel.Warning, Message = "Could not release ordered outbox leases; they expire automatically.")]
    private static partial void ReleaseFailed(ILogger logger, Exception error);
}
