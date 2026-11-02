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
    private readonly string _schema;
    private readonly string _table;

    internal PostgreSqlChangeCaptureWorker(NpgsqlDataSource source, Func<LogicalReplicationConnection> connectionFactory,
        PostgreSqlChangeCaptureOptions options, IEnumerable<ITransport> transports,
        ILogger<PostgreSqlChangeCaptureWorker> logger, ILogger<PostgreSqlMaintenanceWorker> recoveryLogger,
        ILogger<PostgreSqlDirectDispatcher> dispatchLogger, RelayerInstrumentation? instrumentation = null)
    {
        _source = source;
        _connectionFactory = connectionFactory;
        _options = options;
        _logger = logger;
        _schema = options.Schema ?? "public";
        _table = options.TablePrefix + "outbox";
        _recovery = new(source, options.Validate(), transports, recoveryLogger, dispatchLogger, instrumentation);
    }

    public override async Task StartAsync(CancellationToken cancellationToken)
    {
        // Provisioning belongs to the application, never to the privileged replication worker.
        await using var slot = _source.CreateCommand("SELECT EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = @slot AND plugin = 'pgoutput' AND slot_type = 'logical' AND database = current_database() AND NOT temporary)");
        slot.Parameters.AddWithValue("slot", _options.Slot.Name);
        if (await slot.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false) is not true)
            throw new InvalidOperationException("Outbox CDC requires an existing persistent pgoutput slot in the writer's database.");
        foreach (var publication in _options.Replication.PublicationNames)
        {
            await using var command = _source.CreateCommand("SELECT EXISTS (SELECT 1 FROM pg_publication p JOIN pg_publication_tables t ON t.pubname = p.pubname WHERE p.pubname = @publication AND p.pubinsert AND NOT p.pubupdate AND NOT p.pubdelete AND NOT p.pubtruncate AND t.schemaname = @schema AND t.tablename = @table AND t.rowfilter IS NULL)");
            command.Parameters.AddWithValue("publication", publication);
            command.Parameters.AddWithValue("schema", _schema);
            command.Parameters.AddWithValue("table", _table);
            if (await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false) is not true)
                throw new InvalidOperationException($"Outbox CDC publication '{publication}' must include the outbox table without a row filter and publish INSERT only.");
        }
        await base.StartAsync(cancellationToken).ConfigureAwait(false);
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    await using var connection = _connectionFactory();
                    await connection.Open(stoppingToken).ConfigureAwait(false);
                    // The slot already retains commits made during this bounded startup traversal.
                    await _recovery.DispatchCommittedAsync(stoppingToken).ConfigureAwait(false);
                    var changed = false;
                    await foreach (var message in connection.StartReplication(_options.Slot, _options.Replication, stoppingToken).ConfigureAwait(false))
                    {
                        if (message is InsertMessage insert && insert.Relation.Namespace == _schema &&
                            insert.Relation.RelationName == _table)
                            changed = true;
                        if (message is not CommitMessage commit) continue;
                        if (changed) await _recovery.DispatchCommittedAsync(stoppingToken).ConfigureAwait(false);
                        changed = false;
                        // Failed sends remain durable in the table for maintenance. Never acknowledge
                        // a database failure or cancellation before the dispatch transaction commits.
                        connection.SetReplicationStatus(commit.TransactionEndLsn);
                    }
                }
                catch (NpgsqlException error) when (error.IsTransient && !stoppingToken.IsCancellationRequested)
                {
                    Reconnecting(_logger, error);
                    await Task.Delay(_options.ReconnectDelay, stoppingToken).ConfigureAwait(false);
                }
            }
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested) { }
    }

    public override void Dispose()
    {
        base.Dispose();
        _recovery.Dispose();
    }

    [LoggerMessage(Level = LogLevel.Error, Message = "PostgreSQL outbox CDC connection failed. Reconnecting to the persistent slot; committed rows remain durable.")]
    private static partial void Reconnecting(ILogger logger, Exception error);
}
