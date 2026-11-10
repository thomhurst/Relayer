using Google.Cloud.PubSub.V1;

namespace Relayer.GooglePubSub.Tests;

// Observe actual native Stop/Dispose completion without changing delivery or shutdown behavior.
internal sealed class ObservedSubscriberClient(SubscriberClient native, PubSubShutdownInterceptor observer) : SubscriberClient
{
    public override Task StartAsync(Func<PubsubMessage, CancellationToken, Task<Reply>> handlerAsync) => native.StartAsync(handlerAsync);

    public override async Task StopAsync(ShutdownOptions shutdownOptions, CancellationToken cancellationToken = default)
    {
        await native.StopAsync(shutdownOptions, cancellationToken);
        observer.ReceiverClosed();
    }

    public override async ValueTask DisposeAsync()
    {
        await native.DisposeAsync();
        observer.ReceiverClosed();
    }
}
