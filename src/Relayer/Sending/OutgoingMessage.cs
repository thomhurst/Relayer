namespace Relayer;

/// <summary>A serialized send shared by direct sends, outbox dispatch, scheduled copies, and dead-letter copies.</summary>
/// <remarks>
/// Body, header entries, header value memory, ordering-key memory, and native options are borrowed. The caller must keep them alive
/// and unchanged until the transport's send completes, including faulted or cancelled completion. A transport
/// must not retain them after completion. An outbox must copy body, headers, and ordering key into durable storage before
/// enlistment completes. Headers include the original CloudEvents attributes and creation context; dispatch
/// must preserve them rather than generating another identity or capturing the dispatcher's trace context.
/// </remarks>
public readonly struct OutgoingMessage
{
    private readonly SendOptions? _options;

    /// <summary>Creates a send without copying its body, headers, or native options.</summary>
    /// <param name="destination">The native destination on the selected named transport.</param>
    /// <param name="body">The serialized body.</param>
    /// <param name="headers">Resolved native header names and values, including envelope attributes.</param>
    /// <param name="orderingKey">The resolved native ordering key, or null for an unordered send.</param>
    /// <param name="deliverAt">The resolved earliest delivery time, or null for immediate delivery.</param>
    /// <param name="options">
    /// Native callbacks only. Portable headers, ordering, and scheduling must already be resolved into the other arguments.
    /// </param>
    public OutgoingMessage(string destination, ReadOnlyMemory<byte> body,
        ReadOnlyMemory<KeyValuePair<string, WireString>> headers = default,
        WireString? orderingKey = null, DateTimeOffset? deliverAt = null, SendOptions? options = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(destination);
        Destination = destination;
        Body = body;
        Headers = headers;
        OrderingKey = orderingKey;
        DeliverAt = deliverAt;
        _options = options;
    }

    /// <summary>The native destination on the selected named transport.</summary>
    public string Destination { get; }

    /// <summary>The serialized body, borrowed until send completion.</summary>
    public ReadOnlyMemory<byte> Body { get; }

    /// <summary>Resolved native header names and values, borrowed until send completion.</summary>
    public ReadOnlyMemory<KeyValuePair<string, WireString>> Headers { get; }

    /// <summary>The native ordering key as borrowed text or UTF-8 memory, or null when none is specified.</summary>
    public WireString? OrderingKey { get; }

    /// <summary>The earliest delivery time, or null for immediate delivery.</summary>
    public DateTimeOffset? DeliverAt { get; }

    /// <summary>Applies callbacks to the native outgoing object immediately before sending.</summary>
    public void ApplyNative<TNative>(TNative nativeMessage) where TNative : class
    {
        ArgumentNullException.ThrowIfNull(nativeMessage);
        _options?.ApplyNative(nativeMessage);
    }

    /// <summary>Rejects non-persistable native callbacks. Call before enlisting this message in an outbox.</summary>
    /// <exception cref="InvalidOperationException">Native callbacks cannot be persisted.</exception>
    public void ValidateForOutbox()
    {
        if (_options is { HasNativeConfigurators: true })
        {
            throw new InvalidOperationException(
                "Native configurators cannot be persisted in the outbox. " +
                "Use portable headers, ordering, or scheduling options, or send outside the active outbox transaction.");
        }
    }
}
