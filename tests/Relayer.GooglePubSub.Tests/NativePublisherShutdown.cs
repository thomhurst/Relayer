using System.Reflection;
using Google.Cloud.PubSub.V1;

namespace Relayer.GooglePubSub.Tests;

internal static class NativePublisherShutdown
{
    internal static void Observe(PublisherClient publisher, PubSubShutdownInterceptor observer)
    {
        // SDK 3.39.0 invokes its constructor-supplied shutdown callback after draining batches,
        // immediately before completing ShutdownAsync/DisposeAsync. Preserve the existing callback.
        // Test setup only: production constructs the publisher with a no-op callback.
        var field = typeof(PublisherClientImpl).GetField("_shutdown", BindingFlags.Instance | BindingFlags.NonPublic)!;
        var native = (Func<Task>)field.GetValue(publisher)!;
        field.SetValue(publisher, (Func<Task>)(async () =>
        {
            await native();
            observer.PublisherClosed();
        }));
    }
}
