using Google.Api.Gax.Grpc;
using Google.Cloud.PubSub.V1;
using Relayer.Transports;

namespace Relayer.GooglePubSub;

// PublisherClientImpl uses this overload after forming a native batch.
internal sealed class GooglePubSubPublishTelemetry(PublisherServiceApiClient publisher, SendTracing tracing)
    : PublisherServiceApiClient
{
    public override Task<PublishResponse> PublishAsync(TopicName topic, IEnumerable<PubsubMessage> messages, CallSettings? callSettings = null)
    {
        var duration = tracing.StartNativeSend();

        if (!duration.IsEnabled)
        {
            return publisher.PublishAsync(topic, messages, callSettings);
        }

        Task<PublishResponse> publish;

        try
        {
            publish = publisher.PublishAsync(topic, messages, callSettings);
        }
        catch
        {
            duration.Complete(failed: true);
            throw;
        }

        if (publish.IsCompleted)
        {
            duration.Complete(failed: !publish.IsCompletedSuccessfully);
            return publish;
        }

        return RecordOnCompletionAsync(publish, duration);
    }

    private static async Task<PublishResponse> RecordOnCompletionAsync(Task<PublishResponse> publish, SendTracing.NativeSendOperation duration)
    {
        try
        {
            return await publish.ConfigureAwait(false);
        }
        finally
        {
            duration.Complete(failed: !publish.IsCompletedSuccessfully);
        }
    }
}
