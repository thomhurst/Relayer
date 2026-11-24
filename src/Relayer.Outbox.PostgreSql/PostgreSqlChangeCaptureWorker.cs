using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Npgsql;
using Npgsql.Replication;
using Npgsql.Replication.PgOutput.Messages;
using Relayer.Transports;

namespace Relayer.Outbox.PostgreSql;

internal sealed partial class PostgreSqlChangeCaptureWorker : BackgroundService
{
    private readonly NpgsqlDataSource _source;
    private readonly Func<LogicalReplicationConnection> _connectionFactory;
    private readonly PostgreSqlChangeCaptureOptions _options;
    private readonly PostgreSqlMaintenanceWorker _recovery;
    private readonly ILogger<PostgreSqlChangeCaptureWorker> _logger;
    private readonly IHostApplicationLifetime? _hostLifetime;
    private readonly TimeProvider _timeProvider;
    private readonly TimeSpan _reconnectDelay;
    private readonly TimeSpan _idleCheckpointInterval;
    private readonly string _schema;
    private readonly string _table;

    internal PostgreSqlChangeCaptureWorker(NpgsqlDataSource source, Func<LogicalReplicationConnection> connectionFactory,
        PostgreSqlChangeCaptureOptions options, IEnumerable<ITransport> transports,
        ILogger<PostgreSqlChangeCaptureWorker> logger, ILogger<PostgreSqlMaintenanceWorker> recoveryLogger,
        ILogger<PostgreSqlDirectDispatcher> dispatchLogger, RelayerInstrumentation? instrumentation = null,
        IServiceScopeFactory? scopes = null, IHostApplicationLifetime? hostLifetime = null, TimeProvider? timeProvider = null)
    {
        _source = source;
        _connectionFactory = connectionFactory;
        _options = options;
        _logger = logger;
        _hostLifetime = hostLifetime;
        _timeProvider = timeProvider ?? TimeProvider.System;
        _schema = options.Schema ?? "public";
        _table = options.TablePrefix + "outbox";
        _recovery = new(source, options.Validate(), transports, recoveryLogger, dispatchLogger, instrumentation, scopes: scopes);
        _reconnectDelay = RoundUpToMilliseconds(options.ReconnectDelay);
        _idleCheckpointInterval = RoundUpToMilliseconds(options.IdleCheckpointInterval);
    }

    private static TimeSpan RoundUpToMilliseconds(TimeSpan duration)
    {
        var (milliseconds, remainingTicks) = Math.DivRem(duration.Ticks, TimeSpan.TicksPerMillisecond);
        return TimeSpan.FromMilliseconds(milliseconds + (remainingTicks > 0 ? 1 : 0));
    }

    public override async Task StartAsync(CancellationToken cancellationToken)
    {
        await PostgreSqlSentSchema.ValidateAsync(_source, _options.RetainSentMessages, _options.Schema,
            _options.TablePrefix, cancellationToken).ConfigureAwait(false);
        await ValidateSlotAsync(cancellationToken).ConfigureAwait(false);

        foreach (var publication in _options.Replication.PublicationNames)
        {
            await using var command = _source.CreateCommand("SELECT EXISTS (SELECT 1 FROM pg_publication p JOIN pg_publication_tables t ON t.pubname = p.pubname WHERE p.pubname = @publication AND p.pubinsert AND NOT p.pubupdate AND NOT p.pubdelete AND NOT p.pubtruncate AND t.schemaname = @schema AND t.tablename = @table AND t.rowfilter IS NULL)");
            command.Parameters.AddWithValue("publication", publication);
            command.Parameters.AddWithValue("schema", _schema);
            command.Parameters.AddWithValue("table", _table);

            if (await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false) is not true)
            {
                throw new InvalidOperationException($"Outbox CDC publication '{publication}' must include the outbox table without a row filter and publish INSERT only.");
            }
        }

        await base.StartAsync(cancellationToken).ConfigureAwait(false);
    }

    private async Task ValidateSlotAsync(CancellationToken cancellationToken)
    {
        // PostgreSQL 17 added invalidation_reason. Older servers report removed WAL as a lost slot.
        await using var slot = _source.CreateCommand("""
            SELECT COALESCE(to_jsonb(s)->>'invalidation_reason', CASE WHEN wal_status = 'lost' THEN 'wal_removed' END, '')
            FROM pg_replication_slots s
            WHERE slot_name = @slot AND plugin = 'pgoutput' AND slot_type = 'logical'
                AND database = current_database() AND NOT temporary
            """);
        slot.Parameters.AddWithValue("slot", _options.Slot.Name);

        if (await slot.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false) is not string reason)
        {
            throw new InvalidOperationException("Outbox CDC requires an existing persistent pgoutput slot in the writer's database.");
        }

        if (reason.Length != 0)
        {
            throw new InvalidOperationException(
                $"Outbox CDC slot '{_options.Slot.Name}' was invalidated ({reason}). Restore the slot before starting capture.");
        }
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (!await OutboxHostStartup.WaitAsync(_hostLifetime, stoppingToken).ConfigureAwait(false))
        {
            return;
        }

        try
        {
            var reconnecting = false;

            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    if (reconnecting)
                    {
                        // Npgsql's transient SQLSTATE 55000 also covers permanently invalidated slots.
                        await ValidateSlotAsync(stoppingToken).ConfigureAwait(false);
                    }

                    reconnecting = true;
                    await using var connection = _connectionFactory();
                    await connection.Open(stoppingToken).ConfigureAwait(false);
                    // The slot already retains commits made during this bounded startup traversal.
                    await _recovery.DispatchCommittedAsync(stoppingToken).ConfigureAwait(false);

                    if (_idleCheckpointInterval == TimeSpan.Zero)
                    {
                        await ReadReplicationAsync(connection, stoppingToken).ConfigureAwait(false);
                    }
                    else
                    {
                        await ReplicateWithCheckpointsAsync(connection, stoppingToken).ConfigureAwait(false);
                    }
                }
                catch (NpgsqlException error) when (error.IsTransient && !stoppingToken.IsCancellationRequested)
                {
                    Reconnecting(_logger, error);
                    await Task.Delay(_reconnectDelay, _timeProvider, stoppingToken).ConfigureAwait(false);
                }
            }
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
        }
    }

    private async Task ReadReplicationAsync(LogicalReplicationConnection connection, CancellationToken cancellationToken)
    {
        var changed = false;

        await foreach (var message in connection.StartReplication(_options.Slot, _options.Replication, cancellationToken).ConfigureAwait(false))
        {
            if (message is InsertMessage insert && insert.Relation.Namespace == _schema &&
                insert.Relation.RelationName == _table)
            {
                changed = true;
            }

            if (message is not CommitMessage commit)
            {
                continue;
            }

            if (changed)
            {
                await _recovery.DispatchCommittedAsync(cancellationToken).ConfigureAwait(false);
            }

            changed = false;
            // Marker-only commits follow the same ordered stream. Only the reader reports progress,
            // after dispatch has committed or left failed sends durable for maintenance.
            connection.SetReplicationStatus(commit.TransactionEndLsn);
        }
    }

    private async Task ReplicateWithCheckpointsAsync(LogicalReplicationConnection connection, CancellationToken stoppingToken)
    {
        using var attempt = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);
        var replication = ReadReplicationAsync(connection, attempt.Token);
        var checkpoints = EmitCheckpointsAsync(attempt.Token);
        var first = await Task.WhenAny(replication, checkpoints).ConfigureAwait(false);
        await attempt.CancelAsync().ConfigureAwait(false);

        try
        {
            await Task.WhenAll(replication, checkpoints).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (first.IsCompletedSuccessfully)
        {
            // A normal server-side stream end cancels the marker writer before reconnecting.
        }
    }

    private async Task EmitCheckpointsAsync(CancellationToken cancellationToken)
    {
        using var timer = new PeriodicTimer(_idleCheckpointInterval, _timeProvider);
        // Flush locally so the marker never waits for feedback from its own reader.
        await using var marker = _source.CreateCommand("""
            SELECT pg_catalog.set_config('synchronous_commit', 'local', true),
                   pg_catalog.pg_logical_emit_message(true, 'relayer.outbox.checkpoint', '')
            """);

        while (await timer.WaitForNextTickAsync(cancellationToken).ConfigureAwait(false))
        {
            await marker.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }
    }

    public override void Dispose()
    {
        base.Dispose();
        _recovery.Dispose();
    }

    [LoggerMessage(Level = LogLevel.Error, Message = "PostgreSQL outbox CDC connection failed. Reconnecting to the persistent slot; committed rows remain durable.")]
    private static partial void Reconnecting(ILogger logger, Exception error);
}
