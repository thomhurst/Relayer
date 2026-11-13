using Google.Cloud.PubSub.V1;

namespace Relayer.GooglePubSub;

/// <summary>Native clients and explicit topic/subscription declarations. Clients remain caller-owned.</summary>
public sealed class GooglePubSubTransportOptions
{
    /// <summary>The prepared native publisher API client, including credentials, endpoint and retry settings.</summary>
    public PublisherServiceApiClient? Publisher { get; set; }
    /// <summary>The prepared native subscriber API client, including credentials, endpoint and retry settings.</summary>
    public SubscriberServiceApiClient? Subscriber { get; set; }
    /// <summary>Native batching and ordering settings, passed unchanged to each topic publisher.</summary>
    public PublisherClient.Settings PublisherSettings { get; set; } = new();
    /// <summary>Fully qualified topic names managed by this declaration.</summary>
    public IList<TopicName> Topics { get; } = new List<TopicName>();
    /// <summary>Native subscription declarations. Provision creates these unchanged; Validate checks their settings.</summary>
    public IList<Subscription> Subscriptions { get; } = new List<Subscription>();
}

/// <summary>Pub/Sub route configuration. Use the common route ordering key and native send callbacks.</summary>
public sealed class GooglePubSubRouteOptions;

/// <summary>Native streaming pull settings and the common durable failure policy.</summary>
public sealed class GooglePubSubConsumerOptions
{
    /// <summary>Native streaming, flow control and acknowledgement settings. Maximum outstanding elements must equal Concurrency.</summary>
    public SubscriberClient.Settings Settings { get; set; } = new()
    {
        FlowControlSettings = new(1, null),
    };
}
