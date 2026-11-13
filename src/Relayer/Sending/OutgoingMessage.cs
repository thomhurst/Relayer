namespace Relayer.Transports;

/// <summary>A serialized send shared by direct sends, outbox dispatch, scheduled copies, and dead-letter copies.</summary>
/// <remarks>
/// Body, header entries, header value memory, ordering-key memory, and native options are borrowed. The caller must keep them alive
/// and unchanged until the transport's send completes, including faulted or cancelled completion. A transport
/// must not retain them after completion. An outbox must take an owned copy of body, headers, and ordering key before
/// enlistment completes and persist it before transaction commit. Headers include the original CloudEvents attributes and creation context; dispatch
/// must preserve them rather than generating another identity or capturing the dispatcher's trace context.
/// </remarks>
public readonly struct OutgoingMessage
{
    private readonly SendOptions? _options;
    internal bool TracingStarted { get; private init; }
    internal bool IsOutboxDispatch { get; private init; }

    /// <summary>Creates a send without copying its body, headers, or native options.</summary>
    /// <param name="destination">The native destination on the selected named transport.</param>
    /// <param name="body">The serialized body.</param>
    /// <param name="headers">Resolved native header names and values, including envelope attributes.</param>
    /// <param name="orderingKey">The resolved native ordering key, or null for an unordered send.</param>
    /// <param name="deliverAt">The resolved earliest delivery time, or null for immediate delivery.</param>
    /// <param name="options">
    /// Native callbacks only. Portable headers, ordering, and scheduling must already be resolved into the other arguments.
    /// </param>
    /// <param name="isTombstone">Whether to send a native null value; supported only by tombstone-capable transports.</param>
    public OutgoingMessage(string destination, ReadOnlyMemory<byte> body,
        ReadOnlyMemory<KeyValuePair<string, WireString>> headers = default,
        WireString? orderingKey = null, DateTimeOffset? deliverAt = null, SendOptions? options = null, bool isTombstone = false)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(destination);
        Destination = destination;
        Body = body;
        Headers = headers;
        OrderingKey = orderingKey;
        DeliverAt = deliverAt;
        _options = options;
        IsTombstone = isTombstone;
    }

    /// <summary>The native destination on the selected named transport.</summary>
    public string Destination { get; }

    /// <summary>Whether the native value is null rather than a non-null, possibly empty body.</summary>
    public bool IsTombstone { get; }

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
        _options?.ApplyNativeCore(nativeMessage);
    }

    /// <summary>Whether this send needs an immutable native record for its transforms.</summary>
    public bool HasNativeTransforms => _options?.HasNativeTransforms ?? false;

    /// <summary>Applies mutable native callbacks and immutable record transforms in registration order.</summary>
    public TRecord ApplyNative<TNative, TRecord>(TNative nativeMessage, TRecord record)
        where TNative : class where TRecord : class
    {
        ArgumentNullException.ThrowIfNull(nativeMessage);
        ArgumentNullException.ThrowIfNull(record);
        return _options is null ? record : _options.ApplyNativeCore(nativeMessage, record);
    }

    internal OutgoingMessage Forward(string destination, ReadOnlyMemory<KeyValuePair<string, WireString>> headers,
        DateTimeOffset? deliverAt) => new(destination, Body, headers, OrderingKey, deliverAt, _options, IsTombstone);

    internal OutgoingMessage WithTracingStarted() => new(Destination, Body, Headers, OrderingKey, DeliverAt, _options, IsTombstone) { TracingStarted = true };

    // A new send span must link to stored creation headers, even when enqueue was already traced.
    internal OutgoingMessage ForOutboxDispatch() => new(Destination, Body, Headers, OrderingKey, DeliverAt, _options, IsTombstone) { IsOutboxDispatch = true };

    internal void CopyNativeOptionsTo(SendOptions target)
    {
        target.Reset();
        _options?.CopyNativeTo(target);
    }

    /// <summary>Rejects native callbacks and null values that cannot be persisted. Call before outbox enlistment.</summary>
    /// <exception cref="InvalidOperationException">Native callbacks or null values cannot be persisted.</exception>
    public void ValidateForOutbox()
    {
        if (IsTombstone)
            throw new InvalidOperationException("Native tombstones cannot yet be persisted in the outbox. Use SendNowAsync outside the outbox path.");
        if (_options is { HasNativeConfigurators: true })
        {
            throw new InvalidOperationException(
                "Native configurators cannot be persisted in the outbox. " +
                "Use portable headers, ordering, or scheduling options, or send outside the active outbox transaction.");
        }
    }
}
