namespace Relayer;

/// <summary>Per-send options.</summary>
public sealed class SendOptions
{
    private List<Delegate>? _nativeConfigurators;
    internal bool HasNativeTransforms { get; private set; }

    /// <summary>Additional headers to write to the native message.</summary>
    public Dictionary<string, string> Headers { get; } = [];

    /// <summary>Deliver no earlier than this time. Uses native scheduling when supported, otherwise the outbox.</summary>
    public DateTimeOffset? DeliverAt { get; set; }

    /// <summary>Overrides the route's ordering key (Kafka key, ASB session ID, SQS message group ID).</summary>
    public string? OrderingKey { get; set; }

    /// <summary>Whether this send contains native callbacks, which cannot be persisted in an outbox.</summary>
    internal bool HasNativeConfigurators => _nativeConfigurators is { Count: > 0 };

    internal void CopyNativeTo(SendOptions target)
    {
        if (_nativeConfigurators is not { Count: > 0 }) return;
        target._nativeConfigurators ??= [];
        target._nativeConfigurators.AddRange(_nativeConfigurators);
        target.HasNativeTransforms |= HasNativeTransforms;
    }

    internal bool CanRetain => Headers.EnsureCapacity(0) <= 128 &&
        (_nativeConfigurators?.Capacity ?? 0) <= 16;

    internal void Reset()
    {
        Headers.Clear();
        DeliverAt = null;
        OrderingKey = null;
        _nativeConfigurators?.Clear();
        HasNativeTransforms = false;
    }

    /// <summary>
    /// Customises the native outgoing message just before it is handed to the client.
    /// The send fails if a callback cannot accept the route's native outgoing type,
    /// rather than silently ignoring the configuration.
    /// </summary>
    public SendOptions ConfigureNative<TNative>(Action<TNative> configure) where TNative : class
    {
        ArgumentNullException.ThrowIfNull(configure);
        (_nativeConfigurators ??= []).Add(configure);
        return this;
    }

    /// <summary>Transforms an immutable native outgoing record before sending. Native callbacks cannot be persisted in an outbox.</summary>
    public SendOptions TransformNative<TNative>(Func<TNative, TNative> configure) where TNative : class
    {
        ArgumentNullException.ThrowIfNull(configure);
        (_nativeConfigurators ??= []).Add(configure);
        HasNativeTransforms = true;
        return this;
    }

    internal TRecord ApplyNativeCore<TNative, TRecord>(TNative nativeMessage, TRecord record)
        where TNative : class where TRecord : class
    {
        if (_nativeConfigurators is null) return record;
        foreach (var callback in _nativeConfigurators)
        {
            if (callback is Action<TNative> configure) configure(nativeMessage);
            else if (callback is Func<TRecord, TRecord> transform)
                record = transform(record) ?? throw new InvalidOperationException("A native record transform returned null.");
            else throw new InvalidOperationException("Native configuration does not match the route's native outgoing type.");
        }
        return record;
    }

    /// <summary>Applies native callbacks in registration order. Called by transports with their native outgoing type.</summary>
    /// <remarks>No wrapping closure, reflection, or dynamic invocation is used.</remarks>
    internal void ApplyNativeCore<TNative>(TNative nativeMessage) where TNative : class
    {
        ArgumentNullException.ThrowIfNull(nativeMessage);

        if (_nativeConfigurators is null)
        {
            return;
        }

        foreach (var callback in _nativeConfigurators)
        {
            if (callback is not Action<TNative> configure)
            {
                throw new InvalidOperationException(
                    "Native configuration does not match the route's native outgoing type. " +
                    "Register ConfigureNative<TNative> with a callback that accepts the transport's native outgoing type.");
            }

            configure(nativeMessage);
        }
    }
}
