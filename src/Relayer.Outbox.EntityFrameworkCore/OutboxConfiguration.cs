using System.Data.Common;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.Extensions.DependencyInjection;

namespace Relayer.Outbox.EntityFrameworkCore;

// Provider implementations supply native commands; coordination stays independent of a database client.
internal abstract class OutboxConfiguration : IDbContextOptionsExtension
{
    private DbContextOptionsExtensionInfo? _info;
    public DbContextOptionsExtensionInfo Info => _info ??= new ConfigurationInfo(this);
    public void ApplyServices(IServiceCollection services) { }
    public void Validate(IDbContextOptions options)
    {
        ValidatePooling(options);
        ValidateProvider(options);
    }

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
        // Settings affect commands only; never retain tenant services in a per-store EF provider cache.
        public override bool ShouldUseSameServiceProvider(DbContextOptionsExtensionInfo other) => other is ConfigurationInfo;
    }

    internal OutboxTenantBinding? TenantBinding { get; init; }
    internal string Table { get; init; } = null!;
    internal int Lanes { get; init; }
    internal bool SupportsScheduling { get; init; }
    internal bool SupportsOrderedScheduling { get; init; }
    internal abstract void ValidateProvider(string? providerName);
    internal abstract void ValidateProvider(IDbContextOptions options);
    internal abstract OutboxConfiguration? FindConfiguration(IDbContextOptions options);
    internal abstract IOutboxBatch CreateBatch();
    internal string InsertSql { get; init; } = null!;
    internal abstract void Committed(List<BufferedOutboxMessage> rows, int count);
    internal abstract IExecutionStrategy CreateConflictStrategy(DbContext context);
    internal abstract ValueTask<int> InsertInboxAsync(DbConnection connection, DbTransaction transaction, string consumer, Guid id, CancellationToken cancellationToken);
    internal abstract ValueTask<bool> InboxExistsAsync(DbConnection connection, string consumer, Guid id, CancellationToken cancellationToken);

    internal static EntityFrameworkCoreConsumerSettings ValidateConsumer(DbContext context, bool inbox,
        OutboxConfiguration fallback, bool explicitOptions, bool requireOutbox)
    {
        fallback.ValidateProvider(context.Database.ProviderName);
        var nativeOptions = context.GetService<IDbContextOptions>();
        ValidatePooling(nativeOptions);
        var outbox = fallback.FindConfiguration(nativeOptions);
        if (outbox is not null && explicitOptions)
            throw new InvalidOperationException("Configure schema and prefix on AddRelayerOutbox; the consumer inbox uses that same configuration.");
        if (requireOutbox && outbox is null)
            throw new InvalidOperationException("Saga consumers require AddRelayerOutbox on the selected scoped DbContext. State, inbox and outgoing messages must share one transaction.");
        outbox?.TenantBinding?.Validate();
        return new(inbox || outbox is not null, outbox is not null, outbox ?? fallback);
    }
}

internal readonly record struct EntityFrameworkCoreConsumerSettings(bool Inbox, bool Outbox, OutboxConfiguration Configuration);
