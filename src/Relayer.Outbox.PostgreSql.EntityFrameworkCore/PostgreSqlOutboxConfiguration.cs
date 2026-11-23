using System.Data;
using System.Data.Common;
using System.Runtime.CompilerServices;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Storage;
using Npgsql;
using Npgsql.EntityFrameworkCore.PostgreSQL.Infrastructure.Internal;
using NpgsqlTypes;
using Relayer.Outbox.PostgreSql;

namespace Relayer.Outbox.EntityFrameworkCore;

internal sealed class PostgreSqlOutboxConfiguration : OutboxConfiguration
{
    private PostgreSqlDirectDispatcher? Dispatcher
    {
        get;
    }

    internal string InboxInsertSql
    {
        get;
    }

    internal (bool Unordered, bool Ordered) RegisteredScheduling
    {
        get;
    }

    private readonly string _inboxInsertWithTransactionIdSql;
    private readonly string _verifyInboxCommitSql;
    private readonly string? _schema;
    private readonly string _tablePrefix;
    private readonly PostgreSqlLocalStore[] _localStores;
    private readonly IEnumerable<PostgreSqlSchedulingRegistration> _workers;
    private string? _validatedWriterConnectionString;

    internal PostgreSqlOutboxConfiguration(EntityFrameworkCoreOutboxOptions options, PostgreSqlDirectDispatcher? dispatcher = null,
        IEnumerable<PostgreSqlSchedulingRegistration>? workers = null, PostgreSqlLocalStore[]? localStores = null)
    {
        TenantBinding = options.TenantBinding;
        TenantBinding?.Validate();
        // Reuse the schema's validation, including generated identifier lengths, at registration time.
        _ = PostgreSqlOutboxSchema.CreateScript(new()
        {
            Schema = options.Schema,
            TablePrefix = options.TablePrefix
        });

        if (options.Lanes <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(options), "Lanes must be positive.");
        }

        Table = PostgreSqlOutboxSchema.QuoteIdentifier(options.Schema ?? "public") + "." +
            PostgreSqlOutboxSchema.QuoteIdentifier(options.TablePrefix + "outbox");
        Lanes = options.Lanes;
        _schema = options.Schema;
        _tablePrefix = options.TablePrefix;
        _localStores = localStores ?? [];

        if (options.DispatchStoreName is not null)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(options.DispatchStoreName);
        }

        dispatcher?.ValidateStore(options.Schema, options.TablePrefix);
        Dispatcher = dispatcher;

        _workers = workers ?? [];
        RegisteredScheduling = PostgreSqlSchedulingRegistration.GetCapabilities(_workers, _schema, _tablePrefix, Lanes);

        var inbox = PostgreSqlOutboxSchema.QuoteIdentifier(options.Schema ?? "public") + "." +
            PostgreSqlOutboxSchema.QuoteIdentifier(options.TablePrefix + "inbox");
        InboxInsertSql = $"INSERT INTO {inbox} (consumer, message_id, processed_at) VALUES (@consumer, @id, clock_timestamp()) ON CONFLICT (consumer, message_id) DO NOTHING";
        _inboxInsertWithTransactionIdSql = InboxInsertSql + " RETURNING pg_catalog.pg_current_xact_id()";
        // Check the full transaction ID so a reused 32-bit xmin cannot verify an old attempt.
        _verifyInboxCommitSql = $"SELECT EXISTS (SELECT 1 FROM {inbox} WHERE consumer = @consumer AND message_id = @id " +
            "AND xmin = CAST(@transaction_id AS xid) AND pg_catalog.pg_xact_status(@transaction_id) = 'committed')";
        InsertSql = PostgreSqlOutboxBatch.CreateInsertSql(Table);
    }

    internal override bool HasTransactionEnded(DbTransaction transaction)
    {
        if (transaction is not NpgsqlTransaction nativeTransaction)
        {
            return false;
        }

        // Npgsql checks whether the transaction has completed or been disposed when reading
        // IsolationLevel. Connection can still be non-null after PostgreSQL aborts COMMIT.
        try
        {
            _ = nativeTransaction.IsolationLevel;
            return false;
        }
        catch (InvalidOperationException)
        {
            return true;
        }
    }

    internal override void ValidateProvider(string? providerName)
    {
        if (providerName != "Npgsql.EntityFrameworkCore.PostgreSQL")
        {
            throw new InvalidOperationException($"Relayer.Outbox.PostgreSql.EntityFrameworkCore requires the Npgsql.EntityFrameworkCore.PostgreSQL provider; configured provider: '{providerName ?? "none"}'. Configure UseNpgsql on this DbContext. SQL Server and MySQL EF Core integrations are planned; use their ADO.NET integrations instead.");
        }
    }

    internal override void ValidateWriter(DbContext context)
    {
        base.ValidateWriter(context);

        PostgreSqlLocalStore.ValidateWriterConnection(_localStores, _schema, _tablePrefix, Lanes,
            context, static writer => writer.Database.GetDbConnection().ConnectionString);
    }

    internal override void ValidateProvider(IDbContextOptions options)
    {
        // Native options identify the configured provider without reflection or creating a DbContext.
#pragma warning disable EF1001 // Npgsql exposes its native provider options extension from its Infrastructure.Internal namespace.
        if (options.FindExtension<NpgsqlOptionsExtension>() is null)
        {
            ValidateProvider((string?)null);
        }
#pragma warning restore EF1001
    }

    internal override OutboxConfiguration? FindConfiguration(IDbContextOptions options)
    {
        return options.FindExtension<PostgreSqlOutboxConfiguration>();
    }

    internal override (bool Unordered, bool Ordered) GetScheduling(DbConnection writer)
    {
        if (RegisteredScheduling == default)
        {
            return default;
        }

        if (writer is not NpgsqlConnection connection)
        {
            throw new InvalidOperationException("Scheduled sends through the PostgreSQL EF Core outbox require an NpgsqlConnection. Configure UseNpgsql with an Npgsql connection string, data source or connection.");
        }

        return PostgreSqlSchedulingRegistration.GetCapabilities(_workers, _schema, _tablePrefix, Lanes, connection);
    }

    internal override void ValidateTransaction(DbTransaction transaction)
    {
        if (Dispatcher is null)
        {
            return;
        }

        var writer = ((NpgsqlTransaction)transaction).Connection!;
        var connectionString = writer.ConnectionString;

        if (connectionString != _validatedWriterConnectionString)
        {
            Dispatcher.ValidateConnection(writer);
            _validatedWriterConnectionString = connectionString;
        }
    }

    internal override IOutboxBatch CreateBatch()
    {
        return new PostgreSqlOutboxBatch();
    }

    internal override IExecutionStrategy CreateConflictStrategy(DbContext context)
    {
        return new PostgreSqlConflictExecutionStrategy(context);
    }

    internal override void Committed(List<BufferedOutboxMessage> rows, int count)
    {
        Dispatcher?.Committed(rows, count);
    }

    [RuntimeAsyncMethodGeneration(false)]
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    internal override async ValueTask<(bool Inserted, ulong TransactionId)> InsertInboxAsync(DbConnection connection,
        DbTransaction transaction, string consumer, Guid id, bool captureTransactionId, CancellationToken cancellationToken)
    {
        var sql = captureTransactionId ? _inboxInsertWithTransactionIdSql : InboxInsertSql;
        await using var command = new NpgsqlCommand(sql, (NpgsqlConnection)connection, (NpgsqlTransaction)transaction);
        command.Parameters.Add(new NpgsqlParameter<string>("consumer", consumer));
        command.Parameters.Add(new NpgsqlParameter<Guid>("id", id));

        if (!captureTransactionId)
        {
            return (await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false) != 0, 0);
        }

        var reader = await command.ExecuteReaderAsync(CommandBehavior.SingleRow, cancellationToken).ConfigureAwait(false);

        await using (reader.ConfigureAwait(false))
        {
            return await reader.ReadAsync(cancellationToken).ConfigureAwait(false)
                ? (true, reader.GetFieldValue<ulong>(0))
                : (false, 0);
        }
    }

    [RuntimeAsyncMethodGeneration(false)]
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    internal override async ValueTask<bool> InboxCommitVerifiedAsync(DbConnection connection, string consumer, Guid id,
        ulong transactionId, CancellationToken cancellationToken)
    {
        await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
        await using var command = new NpgsqlCommand(_verifyInboxCommitSql, (NpgsqlConnection)connection);
        command.Parameters.Add(new NpgsqlParameter<string>("consumer", consumer));
        command.Parameters.Add(new NpgsqlParameter<Guid>("id", id));
        command.Parameters.Add(new NpgsqlParameter<ulong>("transaction_id", NpgsqlDbType.Xid8) { TypedValue = transactionId });
        return (bool)(await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false))!;
    }
}
