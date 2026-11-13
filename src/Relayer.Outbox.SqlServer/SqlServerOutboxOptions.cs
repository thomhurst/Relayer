namespace Relayer.Outbox.SqlServer;

/// <summary>Database identifiers for SQL Server durable outbox primitives.</summary>
public sealed class SqlServerOutboxOptions
{
    /// <summary>Unclassified send or preparation failures before dead-lettering. Explicitly retryable sends retain durable backoff. Keep identical across dispatchers.</summary>
    public int MaxDispatchAttempts { get; init; } = 10;

    /// <summary>Returns true for send failures that should retry indefinitely with durable backoff.
    /// Null preserves bounded poison handling for all send failures. Use native exception classification
    /// and configure the same policy on every dispatcher for this store.</summary>
    public Func<Exception, bool>? RetrySendFailure { get; init; }

    /// <summary>Named commit-triggered direct dispatcher, or null to leave all rows for recovery workers.</summary>
    public string? DispatchStoreName { get; init; }

    /// <summary>Schema containing the outbox. Null selects dbo.</summary>
    public string? Schema { get; init; }

    /// <summary>Stable ordering lane count. Keep identical across writers and ordered dispatch workers.</summary>
    public int Lanes { get; init; } = 32;

    /// <summary>Prefix for table and index names.</summary>
    public string TablePrefix { get; init; } = "relayer_";
}
