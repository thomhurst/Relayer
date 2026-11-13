using System.Data.Common;
using System.Runtime.CompilerServices;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Storage;
using Npgsql;
using Npgsql.EntityFrameworkCore.PostgreSQL.Infrastructure.Internal;
using Relayer.Outbox.PostgreSql;

namespace Relayer.Outbox.EntityFrameworkCore;

internal sealed class PostgreSqlOutboxConfiguration : OutboxConfiguration
{
    private PostgreSqlDirectDispatcher? Dispatcher { get; }
    internal string InboxInsertSql { get; }
    internal string InboxExistsSql { get; }

    internal PostgreSqlOutboxConfiguration(EntityFrameworkCoreOutboxOptions options, PostgreSqlDirectDispatcher? dispatcher = null, IEnumerable<PostgreSqlSchedulingRegistration>? workers = null)
    {
        TenantBinding = options.TenantBinding;
        TenantBinding?.Validate();
        // Reuse the schema's validation, including generated identifier lengths, at registration time.
        _ = PostgreSqlOutboxSchema.CreateScript(new() { Schema = options.Schema, TablePrefix = options.TablePrefix });
        if (options.Lanes <= 0) throw new ArgumentOutOfRangeException(nameof(options), "Lanes must be positive.");
        Table = PostgreSqlOutboxSchema.QuoteIdentifier(options.Schema ?? "public") + "." +
            PostgreSqlOutboxSchema.QuoteIdentifier(options.TablePrefix + "outbox");
        Lanes = options.Lanes;
        if (options.DispatchStoreName is not null) ArgumentException.ThrowIfNullOrWhiteSpace(options.DispatchStoreName);
        dispatcher?.ValidateStore(options.Schema, options.TablePrefix);
        Dispatcher = dispatcher;
        if (workers is not null)
        {
            SupportsScheduling = PostgreSqlSchedulingRegistration.Supports(workers, options.Schema, options.TablePrefix);
            SupportsOrderedScheduling = PostgreSqlSchedulingRegistration.Supports(workers, options.Schema, options.TablePrefix, options.Lanes);
        }
        var inbox = PostgreSqlOutboxSchema.QuoteIdentifier(options.Schema ?? "public") + "." +
            PostgreSqlOutboxSchema.QuoteIdentifier(options.TablePrefix + "inbox");
        InboxExistsSql = $"SELECT EXISTS (SELECT 1 FROM {inbox} WHERE consumer = @consumer AND message_id = @id)";
        InboxInsertSql = $"INSERT INTO {inbox} (consumer, message_id, processed_at) VALUES (@consumer, @id, clock_timestamp()) ON CONFLICT (consumer, message_id) DO NOTHING";
        InsertSql = PostgreSqlOutboxBatch.CreateInsertSql(Table);
    }

    internal override void ValidateProvider(string? providerName)
    {
        if (providerName != "Npgsql.EntityFrameworkCore.PostgreSQL")
            throw new InvalidOperationException($"Relayer.Outbox.PostgreSql.EntityFrameworkCore requires the Npgsql.EntityFrameworkCore.PostgreSQL provider; configured provider: '{providerName ?? "none"}'. Configure UseNpgsql on this DbContext. SQL Server and MySQL EF Core integrations are planned; use their ADO.NET integrations instead.");
    }

    internal override void ValidateProvider(IDbContextOptions options)
    {
        // Native options identify the configured provider without reflection or creating a DbContext.
#pragma warning disable EF1001 // Npgsql exposes its native provider options extension from its Infrastructure.Internal namespace.
        if (options.FindExtension<NpgsqlOptionsExtension>() is null) ValidateProvider((string?)null);
#pragma warning restore EF1001
    }

    internal override OutboxConfiguration? FindConfiguration(IDbContextOptions options) => options.FindExtension<PostgreSqlOutboxConfiguration>();

    internal override IOutboxBatch CreateBatch() => new PostgreSqlOutboxBatch();
    internal override IExecutionStrategy CreateConflictStrategy(DbContext context) => new PostgreSqlConflictExecutionStrategy(context);
    internal override void Committed(List<BufferedOutboxMessage> rows, int count) => Dispatcher?.Committed(rows, count);

    [RuntimeAsyncMethodGeneration(false)]
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    internal override async ValueTask<int> InsertInboxAsync(DbConnection connection, DbTransaction transaction,
        string consumer, Guid id, CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand(InboxInsertSql, (NpgsqlConnection)connection, (NpgsqlTransaction)transaction);
        command.Parameters.Add(new NpgsqlParameter<string>("consumer", consumer));
        command.Parameters.Add(new NpgsqlParameter<Guid>("id", id));
        return await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    [RuntimeAsyncMethodGeneration(false)]
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    internal override async ValueTask<bool> InboxExistsAsync(DbConnection connection, string consumer, Guid id,
        CancellationToken cancellationToken)
    {
        await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
        await using var command = new NpgsqlCommand(InboxExistsSql, (NpgsqlConnection)connection);
        command.Parameters.Add(new NpgsqlParameter<string>("consumer", consumer));
        command.Parameters.Add(new NpgsqlParameter<Guid>("id", id));
        return (bool)(await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false))!;
    }
}
