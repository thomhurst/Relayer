using Azure.Messaging.ServiceBus;
using Azure.Messaging.ServiceBus.Administration;

namespace Relayer.AzureServiceBus;

/// <summary>Options for the AzureServiceBus transport. Native client configuration is passed through unchanged (docs/decisions.md D1).</summary>
public sealed class ServiceBusTransportOptions
{
    /// <summary>Connection string for a client owned and disposed by Relayer. Do not also set Client.</summary>
    public string? ConnectionString { get; set; }

    /// <summary>A caller-owned client, including clients constructed with TokenCredential. Relayer never disposes it.</summary>
    public ServiceBusClient? Client { get; set; }

    /// <summary>Native client options, passed straight to <see cref="ServiceBusClient"/>.</summary>
    public ServiceBusClientOptions ClientOptions { get; set; } = new();

    /// <summary>Caller-owned administration client, used only in Provision mode. Required for caller-owned messaging clients.</summary>
    public ServiceBusAdministrationClient? AdministrationClient { get; set; }

    /// <summary>Native queue declarations. Names must match send destinations.</summary>
    public IList<CreateQueueOptions> Queues { get; } = [];

    /// <summary>
    /// Entity names whose native RequiresSession=true declarations are trusted in Validate and None modes.
    /// Use a queue name, "topic/Subscriptions/subscription", or a declared topic name to assert that every subscription
    /// on that topic, including ones other applications own, requires sessions. Provision still verifies native settings.
    /// This assertion does not enable sessions at the broker.
    /// </summary>
    public ISet<string> TrustedSessionEntities { get; } = new HashSet<string>(StringComparer.Ordinal);

    /// <summary>Native topic declarations. Names must match send destinations.</summary>
    public IList<CreateTopicOptions> Topics { get; } = [];

    /// <summary>
    /// Sets native <c>Subject</c> (<c>sys.Label</c>) to the CloudEvents type, for example <c>orders.placed;v=1</c>, so
    /// subscription filters can match the message type. Applies to direct and outbox sends; a native callback's Subject wins.
    /// Raw-envelope routes have no type and leave Subject unset. Defaults to true.
    /// </summary>
    public bool SubjectFromMessageType { get; set; } = true;

    /// <summary>
    /// Starts the native processors with the host. Defaults to true. When false, the host composes and validates the
    /// consumers but leaves their processors stopped until the keyed <see cref="Relayer.Transports.IConsumerControl"/>
    /// for this transport starts them, for example once provisioned entities exist. Sending works meanwhile.
    /// </summary>
    public bool AutoStartConsumers { get; set; } = true;

    /// <summary>Native subscriptions to provision independently of receive composition.</summary>
    public IList<CreateSubscriptionOptions> Subscriptions { get; } = [];
}
