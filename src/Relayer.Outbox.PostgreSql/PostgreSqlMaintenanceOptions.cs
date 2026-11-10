namespace Relayer.Outbox.PostgreSql;

/// <summary>Immutable settings for leased PostgreSQL recovery and retention workers.</summary>
public sealed class PostgreSqlMaintenanceOptions
{
    /// <summary>Database schema. Null selects public.</summary>
    public string? Schema { get; init; }
    /// <summary>Table prefix, identical to every writer and dispatcher.</summary>
    public string TablePrefix { get; init; } = "relayer_";
    /// <summary>Retain sent IDs, destinations and completion times. Disabled by default; enable on every dispatcher and maintenance worker for this store.</summary>
    public bool RetainSentMessages { get; init; }

    /// <summary>Failed sends before a row is dead-lettered. Must match other dispatchers for this store.</summary>
    public int MaxDispatchAttempts { get; init; } = 10;
    /// <summary>Maximum rows scanned or deleted per pass.</summary>
    public int BatchSize { get; init; } = 100;
    /// <summary>Minimum row age before recovery can dispatch it.</summary>
    public TimeSpan RecoveryDelay { get; init; } = TimeSpan.FromSeconds(30);
    /// <summary>Delay between bounded recovery passes.</summary>
    public TimeSpan SweepInterval { get; init; } = TimeSpan.FromSeconds(5);
    /// <summary>Leadership lease duration. Connected transactions also hold the lease row lock.</summary>
    public TimeSpan LeaseDuration { get; init; } = TimeSpan.FromSeconds(30);
    /// <summary>Row claim duration; choose longer than native send and database latency.</summary>
    public TimeSpan ClaimDuration { get; init; } = TimeSpan.FromSeconds(30);
    /// <summary>Delay between bounded retention passes.</summary>
    public TimeSpan CleanupInterval { get; init; } = TimeSpan.FromMinutes(1);
    /// <summary>Retention from failed_at for dead rows. Null retains them indefinitely.</summary>
    public TimeSpan? DeadRetention { get; init; } = TimeSpan.FromDays(30);
    /// <summary>Retention from processed_at for inbox deduplication. Null retains it indefinitely.</summary>
    public TimeSpan? InboxRetention { get; init; } = TimeSpan.FromDays(7);

    /// <summary>Sent metadata retention from sent_at. Must be positive; cleanup requires RetainSentMessages.</summary>
    public TimeSpan SentRetention { get; init; } = TimeSpan.FromDays(7);

    internal PostgreSqlOutboxOptions Validate()
    {
        if (BatchSize is < 1 or > 10000) throw new ArgumentOutOfRangeException(nameof(BatchSize));
        if (RecoveryDelay < TimeSpan.Zero) throw new ArgumentOutOfRangeException(nameof(RecoveryDelay));
        if (LeaseDuration < TimeSpan.FromSeconds(1) || LeaseDuration > TimeSpan.FromHours(1))
            throw new ArgumentOutOfRangeException(nameof(LeaseDuration));
        ValidateDelay(SweepInterval, nameof(SweepInterval));
        ValidateDuration(ClaimDuration, nameof(ClaimDuration));
        ValidateDelay(CleanupInterval, nameof(CleanupInterval));
        ValidateDuration(SentRetention, nameof(SentRetention));
        if (DeadRetention is { } dead) ValidateDuration(dead, nameof(DeadRetention));
        if (InboxRetention is { } inbox) ValidateDuration(inbox, nameof(InboxRetention));
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(MaxDispatchAttempts);
        ArgumentNullException.ThrowIfNull(TablePrefix);
        var store = new PostgreSqlOutboxOptions { Schema = Schema, TablePrefix = TablePrefix, MaxDispatchAttempts = MaxDispatchAttempts, RetainSentMessages = RetainSentMessages };
        _ = PostgreSqlOutboxSchema.CreateScript(store);
        return store;
    }

    private static void ValidateDelay(TimeSpan value, string name)
    {
        ValidateDuration(value, name);
        if (value.TotalMilliseconds > uint.MaxValue - 1) throw new ArgumentOutOfRangeException(name);
    }

    private static void ValidateDuration(TimeSpan value, string name)
    {
        if (value <= TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(name);
    }
}
