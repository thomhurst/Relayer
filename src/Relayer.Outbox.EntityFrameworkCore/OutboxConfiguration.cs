using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.Extensions.DependencyInjection;
using Relayer.Outbox.PostgreSql;

namespace Relayer.Outbox.EntityFrameworkCore;

internal sealed class OutboxConfiguration : IDbContextOptionsExtension
{
    private DbContextOptionsExtensionInfo? _info;

    // Native options carry the already-created configuration. Delivery validation must not enumerate
    // CoreOptionsExtension.Interceptors: its IEnumerable can allocate an iterator on every attempt.
    public DbContextOptionsExtensionInfo Info => _info ??= new ConfigurationInfo(this);
    public void ApplyServices(IServiceCollection services) { }
    public void Validate(IDbContextOptions options) => ValidatePooling(options);

    internal static void ValidatePooling(IDbContextOptions options)
    {
        if (options.FindExtension<CoreOptionsExtension>()?.MaxPoolSize is not null)
            throw new InvalidOperationException("Relayer EF Core integration does not support pooled DbContexts: " +
                "pooled options cannot bind interceptors to the current ISender scope. " +
                "Use AddDbContext instead of AddDbContextPool or AddPooledDbContextFactory.");
    }

    private sealed class ConfigurationInfo(IDbContextOptionsExtension extension) : DbContextOptionsExtensionInfo(extension)
    {
        public override bool IsDatabaseProvider => false;
        public override string LogFragment => "";
        public override int GetServiceProviderHashCode() => 0;
        public override void PopulateDebugInfo(IDictionary<string, string> debugInfo) { }
        // Configuration affects commands, not EF's internal service registrations. Do not create an
        // internal service provider per tenant/schema or retain tenant services in a global cache.
        public override bool ShouldUseSameServiceProvider(DbContextOptionsExtensionInfo other) => other is ConfigurationInfo;
    }

    internal string Table { get; }
    internal int Lanes { get; }
    internal string InsertSql { get; }
    internal PostgreSqlDirectDispatcher? Dispatcher { get; }
    internal bool SupportsScheduling { get; }
    internal bool SupportsOrderedScheduling { get; }
    internal string InboxInsertSql { get; }
    internal string InboxExistsSql { get; }

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
        InboxExistsSql = $"SELECT EXISTS (SELECT 1 FROM {inbox} WHERE consumer = @consumer AND message_id = @id)";
        InboxInsertSql = $"INSERT INTO {inbox} (consumer, message_id, processed_at) VALUES (@consumer, @id, clock_timestamp()) ON CONFLICT (consumer, message_id) DO NOTHING";
        InsertSql = PostgreSqlOutboxBatch.CreateInsertSql(Table);
    }
}
