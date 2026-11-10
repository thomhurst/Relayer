namespace Relayer;

/// <summary>Inspects and maintains the configured dead-letter sources of one named transport.</summary>
public interface IDeadLetterManager
{
    /// <summary>Lists configured sources, without querying broker-wide entities or message counts.</summary>
    IReadOnlyList<DeadLetterSource> List();

    /// <summary>Peeks a bounded, non-destructive page. Cursors are opaque and scoped to the source.</summary>
    ValueTask<DeadLetterPage> PeekAsync(string source, int maxMessages = 20, string? cursor = null,
        CancellationToken cancellationToken = default);

    /// <summary>Scans at most maxMessages deliveries, sending matches before removing their originals.
    /// Redrive is at-least-once. Filter exceptions and send failures leave the original unsettled.</summary>
    ValueTask<int> RedriveAsync(string source, Func<DeadLetterMessage, bool>? filter = null,
        int maxMessages = 20, CancellationToken cancellationToken = default);

    /// <summary>Permanently removes up to maxMessages available messages using native settlement.</summary>
    ValueTask<int> PurgeAsync(string source, int maxMessages = 20, CancellationToken cancellationToken = default);
}

/// <summary>A configured source identifier and its native redrive destination.</summary>
public sealed record DeadLetterSource(string Id, string Destination);

/// <summary>A management snapshot. Native objects are available through Features; settlement through these objects is unsupported.</summary>
public sealed class DeadLetterMessage
{
    /// <summary>Native message identifier, if present.</summary>
    public string? MessageId { get; init; }
    /// <summary>Owned body storage, valid after the management call returns.</summary>
    public ReadOnlyMemory<byte> Body { get; init; }
    /// <summary>Transport-native message properties and metadata.</summary>
    public MessageFeatures Features { get; } = new();
}

/// <summary>A bounded peek result. A continuation can return an empty page when the source has changed.</summary>
public sealed record DeadLetterPage(IReadOnlyList<DeadLetterMessage> Messages, string? NextCursor);
