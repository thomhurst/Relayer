using System.Buffers;
using Npgsql;

namespace Relayer.Outbox.PostgreSql;

/// <summary>A borrowed durable local attempt. Use Transaction for business commands; never complete or retain it.</summary>
public readonly record struct PostgreSqlLocalDelivery(OutgoingMessage Message, OutboxClaim Claim,
    NpgsqlTransaction Transaction, int Attempt)
{
    internal PostgreSqlLocalTransport Owner { get; init; } = null!;
    internal PostgreSqlOutboxStore CompletionStore { get; init; } = null!;
}

/// <summary>Reads original outbox payloads and CloudEvents headers without copying.</summary>
public readonly struct PostgreSqlLocalMessageBinding : IMessageBinding<PostgreSqlLocalDelivery>
{
    /// <inheritdoc />
    public static ReadOnlySequence<byte> GetBody(PostgreSqlLocalDelivery message) => new(message.Message.Body);
    /// <inheritdoc />
    public static int? GetDeliveryCount(PostgreSqlLocalDelivery message) => message.Attempt;
    /// <inheritdoc />
    public static bool TryGetHeader(PostgreSqlLocalDelivery message, string name, out WireString value)
    {
        foreach (var header in message.Message.Headers.Span)
            if (header.Key == name) { value = header.Value; return true; }
        value = default;
        return false;
    }
}
