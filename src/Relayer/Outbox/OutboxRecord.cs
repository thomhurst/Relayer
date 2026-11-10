namespace Relayer;

/// <summary>A durable outbox row projected into a serialized send without generating another envelope.</summary>
/// <remarks>
/// Id is the original UUIDv7 message ID. MessageType is the stable wire identity, including its version,
/// never a CLR type name. Message preserves the original body, native header names/values, ordering key and
/// delivery time, including CloudEvents identity and creation context. It must contain no native callbacks.
/// InsertSequence is the database-assigned insertion sequence when supplied by the provider.
/// Providers validate these invariants when writing rows. Lane is null for unordered rows.
/// Payload memory is borrowed until the next ReadPendingAsync on the store instance or its disposal.
/// </remarks>
public readonly record struct OutboxRecord(
    Guid Id,
    string TransportName,
    string MessageType,
    OutgoingMessage Message,
    DateTimeOffset CreatedAt,
    int? Lane = null,
    int Attempts = 0,
    string? LastError = null,
    string? TenantId = null,
    long? InsertSequence = null);
