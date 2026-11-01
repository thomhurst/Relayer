namespace Relayer.Outbox.PostgreSql;

/// <summary>Options for the PostgreSql outbox and inbox (docs/design.md section 6).</summary>
public sealed class PostgreSqlOutboxOptions
{
    /// <summary>The database schema for Relayer tables. <see langword="null"/> uses public.</summary>
    public string? Schema { get; set; }

    /// <summary>Prefix for table and index names, so several endpoints can share a schema.</summary>
    public string TablePrefix { get; set; } = "relayer_";

    /// <summary>Number of lanes for ordered dispatch. Changing it requires draining the outbox first.</summary>
    public int Lanes { get; set; } = 32;

    /// <summary>How long a lane or job lease lasts without a heartbeat.</summary>
    public TimeSpan LeaseDuration { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>Dispatch attempts before a row is moved to the dead table.</summary>
    public int MaxDispatchAttempts { get; set; } = 10;
}
