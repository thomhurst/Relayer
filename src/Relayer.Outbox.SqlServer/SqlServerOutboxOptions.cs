namespace Relayer.Outbox.SqlServer;

/// <summary>Database identifiers for SQL Server durable outbox primitives.</summary>
public sealed class SqlServerOutboxOptions
{
    /// <summary>Schema containing the outbox. Null selects dbo.</summary>
    public string? Schema { get; init; }

    /// <summary>Prefix for table and index names.</summary>
    public string TablePrefix { get; init; } = "relayer_";
}
