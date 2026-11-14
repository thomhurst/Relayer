using System.Diagnostics.CodeAnalysis;
namespace Relayer.Outbox.PostgreSql;

/// <summary>Limits shared by every store in the PostgreSQL tenant catalog.</summary>
[Experimental("RLYEXP004", UrlFormat = "https://relayer.tomlonghurst.dev/docs/reference/diagnostics#rlyexp004")]
public sealed class PostgreSqlTenantDispatchOptions
{
    /// <summary>Maximum simultaneous native connections for direct dispatch, recovery, leases and cleanup combined.</summary>
    public int MaxConnections { get; init; } = 4;
    /// <summary>Delay between finite catalog passes, including refreshing additions and removals.</summary>
    public TimeSpan PollInterval { get; init; } = TimeSpan.FromSeconds(1);
    /// <summary>Maximum duration of one tenant turn or lease release. Failures do not stop other tenants.</summary>
    public TimeSpan TurnTimeout { get; init; } = TimeSpan.FromSeconds(15);
    /// <summary>Maximum messages in each native dispatch batch or retention delete.</summary>
    public int BatchSize { get; init; } = 100;
    /// <summary>Maximum delete batches per enabled table in one cleanup turn (1–100).</summary>
    public int CleanupBatchLimit { get; init; } = 10;
    /// <summary>Maximum buffered commit batches per tenant. Overflow remains durable for recovery.</summary>
    public int QueueCapacity { get; init; } = 16;
    /// <summary>Minimum age of unordered rows before recovery dispatch.</summary>
    public TimeSpan RecoveryDelay { get; init; } = TimeSpan.FromSeconds(30);
    /// <summary>Delay between retention passes per tenant.</summary>
    public TimeSpan CleanupInterval { get; init; } = TimeSpan.FromMinutes(1);
    /// <summary>Dead-row retention. Null retains rows indefinitely.</summary>
    public TimeSpan? DeadRetention { get; init; } = TimeSpan.FromDays(30);
    /// <summary>Inbox retention. Null retains deduplication identities indefinitely.</summary>
    public TimeSpan? InboxRetention { get; init; } = TimeSpan.FromDays(7);

    /// <summary>Sent metadata retention for stores with RetainSentMessages enabled.</summary>
    public TimeSpan SentRetention { get; init; } = TimeSpan.FromDays(7);

    internal void Validate()
    {
        if (MaxConnections is < 1 or > 1024) throw new ArgumentOutOfRangeException(nameof(MaxConnections));
        if (QueueCapacity is <= 0 or int.MaxValue) throw new ArgumentOutOfRangeException(nameof(QueueCapacity));
        ValidateDelay(PollInterval, nameof(PollInterval));
        ValidateDelay(TurnTimeout, nameof(TurnTimeout));
        _ = Maintenance(new()).Validate();
    }

    private static void ValidateDelay(TimeSpan value, string name)
    {
        if (value <= TimeSpan.Zero || value.TotalMilliseconds > uint.MaxValue - 1)
            throw new ArgumentOutOfRangeException(name);
    }

    internal PostgreSqlMaintenanceOptions Maintenance(PostgreSqlOutboxOptions store) => new()
    {
        Schema = store.Schema, TablePrefix = store.TablePrefix, MaxDispatchAttempts = store.MaxDispatchAttempts, RetainSentMessages = store.RetainSentMessages, SentRetention = SentRetention,
        BatchSize = BatchSize, CleanupBatchLimit = CleanupBatchLimit, RecoveryDelay = RecoveryDelay, CleanupInterval = CleanupInterval,
        DeadRetention = DeadRetention, InboxRetention = InboxRetention,
        LeaseDuration = store.LeaseDuration, ClaimDuration = store.LeaseDuration
    };
}
