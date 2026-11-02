namespace Relayer.Outbox.MongoDB;

/// <summary>Immutable MongoDB collection names and ordered lane mapping.</summary>
public sealed class MongoDBOutboxOptions
{
    /// <summary>Collection prefix. Use the same value for writers, stores and future workers.</summary>
    public string CollectionPrefix { get; init; } = "relayer_";

    /// <summary>Ordering-key hash modulus. Drain the outbox before changing it.</summary>
    public int Lanes { get; init; } = 32;

    internal void Validate()
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(CollectionPrefix);
        if (CollectionPrefix.Contains('\0') || CollectionPrefix.Contains('$') || CollectionPrefix.StartsWith("system.", StringComparison.Ordinal))
            throw new ArgumentException("CollectionPrefix must not contain NUL or '$', or start with 'system.'.", nameof(CollectionPrefix));
        if (Lanes <= 0) throw new ArgumentOutOfRangeException(nameof(Lanes), "Lanes must be positive.");
    }
}
