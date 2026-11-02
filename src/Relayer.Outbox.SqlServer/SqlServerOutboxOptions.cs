namespace Relayer.Outbox.SqlServer;

/// <summary>Database identifiers for SQL Server durable outbox primitives.</summary>
public sealed class SqlServerOutboxOptions
{
    /// <summary>Schema containing the outbox. Null selects dbo.</summary>
    public string? Schema { get; init; }

    /// <summary>Stable ordering lane count. Keep identical across writers and future dispatch workers.</summary>
    public int Lanes { get; init; } = 32;

    /// <summary>Prefix for table and index names.</summary>
    public string TablePrefix { get; init; } = "relayer_";
}
