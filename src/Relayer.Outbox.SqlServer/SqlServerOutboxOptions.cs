namespace Relayer.Outbox.SqlServer;

/// <summary>Database identifiers for SQL Server durable outbox primitives.</summary>
public sealed class SqlServerOutboxOptions
{
    /// <summary>Failed native sends before dead-lettering. Keep identical across dispatchers.</summary>
    public int MaxDispatchAttempts { get; init; } = 10;

    /// <summary>Named commit-triggered direct dispatcher, or null to leave all rows for recovery workers.</summary>
    public string? DispatchStoreName { get; init; }

    /// <summary>Schema containing the outbox. Null selects dbo.</summary>
    public string? Schema { get; init; }

    /// <summary>Stable ordering lane count. Keep identical across writers and ordered dispatch workers.</summary>
    public int Lanes { get; init; } = 32;

    /// <summary>Prefix for table and index names.</summary>
    public string TablePrefix { get; init; } = "relayer_";
}
