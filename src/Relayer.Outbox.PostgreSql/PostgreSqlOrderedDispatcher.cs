using System.Runtime.CompilerServices;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Npgsql;
using Relayer.Transports;

namespace Relayer.Outbox.PostgreSql;

internal sealed partial class PostgreSqlOrderedDispatcher : BackgroundService
{
    private readonly NpgsqlDataSource _source;
    private readonly PostgreSqlOrderedDispatchOptions _options;
    private readonly PostgreSqlLaneCoordinator _coordinator;
    private readonly PostgreSqlOrderedLane _lane;
    private readonly Dictionary<string, ITransport> _transports = new(StringComparer.Ordinal);
    private readonly OutboxRecord[] _rows;
    private readonly Dictionary<(ITransport Transport, string Destination), TransportCapabilities> _capabilities = [];
    private readonly ILogger<PostgreSqlOrderedDispatcher> _logger;
    private readonly TimeProvider _timeProvider;

    internal PostgreSqlOrderedDispatcher(NpgsqlDataSource source, PostgreSqlOrderedDispatchOptions options,
        IEnumerable<ITransport> transports, ILogger<PostgreSqlOrderedDispatcher> logger, TimeProvider? timeProvider = null)
    {
        _source = source;
        _options = options;
        var store = options.Validate();
        _coordinator = new(source, options);
        _lane = new(source, store, options.LeaseDuration);
        _rows = new OutboxRecord[options.BatchSize];
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
        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                var madeProgress = false;
                try
                {
                    await _coordinator.RebalanceAsync(stoppingToken).ConfigureAwait(false);
                    for (var lane = 0; lane < _options.Lanes; lane++)
                    {
                        if (!_coordinator.Assigned[lane]) continue;
                        await _coordinator.RebalanceAsync(stoppingToken).ConfigureAwait(false);
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
    }

    // One connection and lease-row transaction lock per batch. Never run concurrent sends within a lane.
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    internal async ValueTask<int> DispatchLaneAsync(int lane, CancellationToken cancellationToken)
    {
        var started = _timeProvider.GetTimestamp();
        // Reserve half the hard deadline for the final admitted send and its database commit.
        var admissionBudget = _options.LeaseDuration / 4;
        await using var connection = await _source.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);
        if (!await _coordinator.AcquireAsync(connection, transaction, lane, cancellationToken).ConfigureAwait(false)) return 0;
        var count = 0;
        var sent = 0;
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
                var prepared = Prepare(row, _transports, _capabilities);
                try { await prepared.Transport.SendAsync(prepared.Message, cancellationToken).ConfigureAwait(false); }
                catch (Exception error) when (!cancellationToken.IsCancellationRequested)
                {
                    if (!await _lane.FailAsync(connection, transaction, claim, error.Message, cancellationToken).ConfigureAwait(false))
                        throw new InvalidOperationException("Ordered outbox claim expired while recording a native send failure.", error);
                    DispatchFailed(_logger, error);
                    break;
                }
                // Do not advance to another row after an uncertain completion. Rollback safely permits duplicates.
                if (!await _lane.CompleteAsync(connection, transaction, claim, cancellationToken).ConfigureAwait(false))
                    throw new InvalidOperationException("Ordered outbox claim expired after native send. The batch remains available for recovery.");
                sent++;
            }
            await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
            return sent;
        }
        finally { Array.Clear(_rows); }
    }

    internal static (ITransport Transport, OutgoingMessage Message) Prepare(OutboxRecord row,
        Dictionary<string, ITransport> transports,
        Dictionary<(ITransport Transport, string Destination), TransportCapabilities> capabilities)
    {
        if (!transports.TryGetValue(row.TransportName, out var transport))
            throw new InvalidOperationException($"Outbox row references unknown transport '{row.TransportName}'. Register its named ITransport adapter.");
        if (row.Message.OrderingKey is null)
            throw new InvalidOperationException("An ordered outbox row must contain its original ordering key.");
        var endpoint = (transport, row.Message.Destination);
        if (!capabilities.TryGetValue(endpoint, out var capability))
        {
            capability = transport.GetCapabilities(row.Message.Destination);
            capabilities.Add(endpoint, capability);
        }
        if (!capability.OrderedByKey)
            throw new InvalidOperationException($"Ordered outbox destination '{row.Message.Destination}' on '{transport.Name}' does not support native ordering by key.");
        // The database already enforces deliver_at. Do not schedule again in the native client.
        var message = row.Message;
        return (transport, new(message.Destination, message.Body, message.Headers, message.OrderingKey));
    }

    [LoggerMessage(Level = LogLevel.Error, Message = "Ordered outbox dispatch failed; durable rows remain available for retry.")]
    private static partial void DispatchFailed(ILogger logger, Exception error);
    [LoggerMessage(Level = LogLevel.Warning, Message = "Could not release ordered outbox leases; they expire automatically.")]
    private static partial void ReleaseFailed(ILogger logger, Exception error);
}
