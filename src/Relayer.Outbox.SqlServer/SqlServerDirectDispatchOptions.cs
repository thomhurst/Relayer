namespace Relayer.Outbox.SqlServer;

/// <summary>Limits for commit-triggered, unordered SQL Server outbox dispatch.</summary>
public sealed class SqlServerDirectDispatchOptions
{
    /// <summary>Maximum queued commits. Overflow leaves durable rows pending and logs an error.</summary>
    public int QueueCapacity { get; init; } = 256;

    /// <summary>Maximum messages claimed and sent together, also bounded by the native endpoint limit.</summary>
    public int BatchSize { get; init; } = 100;

    /// <summary>Lifetime of a fenced claim. A send that outlives it can be delivered again.</summary>
    public TimeSpan ClaimDuration { get; init; } = TimeSpan.FromSeconds(30);
}
