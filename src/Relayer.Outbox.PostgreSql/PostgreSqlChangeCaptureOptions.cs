using Npgsql.Replication.PgOutput;

namespace Relayer.Outbox.PostgreSql;

/// <summary>Settings for an explicitly provisioned PostgreSQL logical replication slot.</summary>
public sealed class PostgreSqlChangeCaptureOptions
{
    /// <summary>Existing persistent pgoutput slot. Only one active reader may use a slot.</summary>
    public required PgOutputReplicationSlot Slot { get; init; }
    /// <summary>Native pgoutput settings. Use protocol V1, without streaming or two-phase decoding.</summary>
    public required PgOutputReplicationOptions Replication { get; init; }
    /// <summary>Schema shared with writers. Null selects public.</summary>
    public string? Schema { get; init; }
    /// <summary>Table prefix shared with writers.</summary>
    public string TablePrefix { get; init; } = "relayer_";
    /// <summary>Failed sends before a row is dead-lettered. Must match other dispatchers for this store.</summary>
    public int MaxDispatchAttempts { get; init; } = 10;
    /// <summary>Maximum rows read per database transaction.</summary>
    public int BatchSize { get; init; } = 100;
    /// <summary>Claim duration, longer than native send and database latency.</summary>
    public TimeSpan ClaimDuration { get; init; } = TimeSpan.FromSeconds(30);
    /// <summary>Delay before reconnecting after a transient native connection failure.</summary>
    public TimeSpan ReconnectDelay { get; init; } = TimeSpan.FromSeconds(5);

    internal PostgreSqlMaintenanceOptions Validate()
    {
        ArgumentNullException.ThrowIfNull(Slot);
        ArgumentNullException.ThrowIfNull(Replication);
        if (Replication.ProtocolVersion != PgOutputProtocolVersion.V1 ||
            Replication.StreamingMode is not (null or PgOutputStreamingMode.Off) || Replication.TwoPhase == true)
            throw new ArgumentException("Outbox CDC requires pgoutput protocol V1 without streaming or two-phase decoding.", nameof(Replication));
        if (ReconnectDelay <= TimeSpan.Zero || ReconnectDelay.TotalMilliseconds > uint.MaxValue - 1)
            throw new ArgumentOutOfRangeException(nameof(ReconnectDelay));
        var recovery = new PostgreSqlMaintenanceOptions
        {
            Schema = Schema, TablePrefix = TablePrefix, BatchSize = BatchSize,
            MaxDispatchAttempts = MaxDispatchAttempts, ClaimDuration = ClaimDuration, RecoveryDelay = TimeSpan.Zero
        };
        _ = recovery.Validate();
        return recovery;
    }
}
