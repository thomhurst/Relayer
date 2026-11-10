namespace Relayer.Outbox.EntityFrameworkCore;

/// <summary>Options for the EntityFrameworkCore outbox and inbox (docs/design.md section 6).</summary>
public sealed class EntityFrameworkCoreOutboxOptions
{
    /// <summary>Application-authorized tenant binding for this scoped native store. Required with AddRelayerTenancy.</summary>
    /// <remarks>Create in the application-owned native store factory after authorization; never share across scopes.</remarks>
    public OutboxTenantBinding? TenantBinding { get; init; }

    /// <summary>The database schema for Relayer tables. <see langword="null"/> uses the default schema.</summary>
    public string? Schema { get; init; }

    /// <summary>Prefix for table and index names, so several endpoints can share a schema.</summary>
    public string TablePrefix { get; init; } = "relayer_";

    /// <summary>Number of lanes for ordered dispatch. Changing it requires draining the outbox first.</summary>
    public int Lanes { get; init; } = 32;

    /// <summary>Named PostgreSQL direct dispatcher notified after commit. Null persists rows without dispatching.</summary>
    public string? DispatchStoreName { get; init; }

    /// <summary>How long a lane or job lease lasts without a heartbeat.</summary>
    public TimeSpan LeaseDuration { get; init; } = TimeSpan.FromSeconds(30);

    /// <summary>Dispatch attempts before a row is moved to the dead table.</summary>
    public int MaxDispatchAttempts { get; init; } = 10;
}
