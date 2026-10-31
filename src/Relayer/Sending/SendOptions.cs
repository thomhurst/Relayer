namespace Relayer;

/// <summary>Per-send options.</summary>
public sealed class SendOptions
{
    private List<Action<object>>? _nativeConfigurators;

    /// <summary>Additional headers to write to the native message.</summary>
    public Dictionary<string, string> Headers { get; } = [];

    /// <summary>Deliver no earlier than this time. Uses native scheduling when supported, otherwise the outbox.</summary>
    public DateTimeOffset? DeliverAt { get; set; }

    /// <summary>Overrides the route's ordering key (Kafka key, ASB session ID, SQS message group ID).</summary>
    public string? OrderingKey { get; set; }

    /// <summary>
    /// Customises the native outgoing message just before it is handed to the client.
    /// The send fails if the route's native outgoing type is not <typeparamref name="TNative"/>,
    /// rather than silently ignoring the configuration.
    /// </summary>
    public SendOptions ConfigureNative<TNative>(Action<TNative> configure) where TNative : class
    {
        ArgumentNullException.ThrowIfNull(configure);
        (_nativeConfigurators ??= []).Add(native =>
        {
            if (native is not TNative typed)
            {
                throw new InvalidOperationException(
                    $"Native configuration expects {typeof(TNative).Name}, but the route's transport produces {native.GetType().Name}.");
            }

            configure(typed);
        });
        return this;
    }

    /// <summary>Applies native configurators. Called by transports.</summary>
    public void ApplyNative(object nativeMessage)
    {
        ArgumentNullException.ThrowIfNull(nativeMessage);

        if (_nativeConfigurators is null)
        {
            return;
        }

        foreach (var configure in _nativeConfigurators)
        {
            configure(nativeMessage);
        }
    }
}
