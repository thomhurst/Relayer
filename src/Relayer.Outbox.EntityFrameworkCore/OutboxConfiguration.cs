using Relayer.Outbox.PostgreSql;

namespace Relayer.Outbox.EntityFrameworkCore;

internal sealed class OutboxConfiguration
{
    internal string Table { get; }
    internal int Lanes { get; }
    internal string InsertSql { get; }
    internal PostgreSqlDirectDispatcher? Dispatcher { get; }
    internal bool SupportsScheduling { get; }
    internal bool SupportsOrderedScheduling { get; }
    internal string InboxInsertSql { get; }

    internal OutboxConfiguration(EntityFrameworkCoreOutboxOptions options, PostgreSqlDirectDispatcher? dispatcher = null, IEnumerable<PostgreSqlSchedulingRegistration>? workers = null)
    {
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
        InboxInsertSql = $"INSERT INTO {inbox} (consumer, message_id, processed_at) VALUES (@consumer, @id, clock_timestamp()) ON CONFLICT (consumer, message_id) DO NOTHING";
        InsertSql = PostgreSqlOutboxBatch.CreateInsertSql(Table);
    }
}
