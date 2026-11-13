namespace Relayer.Transports;

/// <summary>Relayer metadata carried by durable outbox dispatch.</summary>
public static class OutboxHeaders
{
    /// <summary>The positive decimal insert sequence of an ordered outbox row.</summary>
    public const string Sequence = "relayer.outbox.sequence";
}
