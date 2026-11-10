using Google.Cloud.PubSub.V1;

namespace Relayer.GooglePubSub;

/// <summary>Native clients for a named transport, available as a keyed service and through the delivery feature.</summary>
public interface IGooglePubSubClients
{
    /// <summary>The unchanged caller-owned administration/publisher API client.</summary>
    PublisherServiceApiClient PublisherApi { get; }
    /// <summary>The unchanged caller-owned administration/subscriber API client.</summary>
    SubscriberServiceApiClient SubscriberApi { get; }
    /// <summary>The adapter-owned native topic publisher, including ResumePublish.</summary>
    /// <remarks>Do not shut down or dispose this publisher; the transport owns its lifetime.</remarks>
    PublisherClient PublisherFor(string topic);
}
