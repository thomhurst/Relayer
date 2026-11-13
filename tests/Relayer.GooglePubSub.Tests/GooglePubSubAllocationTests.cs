using Relayer.Transports;
using Google.Cloud.PubSub.V1;
using Microsoft.Extensions.Options;

namespace Relayer.GooglePubSub.Tests;

public class GooglePubSubAllocationTests
{
    [Test]
    public async Task Warmed_synchronous_native_callback_adds_zero_allocations()
    {
        using var tracing = new MessageTracing(Options.Create(new TracingOptions()), []);
        var native = new CallbackSubscriber();
        await using var worker = new GooglePubSubConsumer(() => native, tracing, "projects/test/topics/orders", null!)
        {
            Receive = static (_, _) => new(Disposition.Complete),
        };
        worker.Start();
        var message = new PubsubMessage();
        for (var i = 0; i < 1000; i++) await native.Callback(message, default);
        var before = GC.GetAllocatedBytesForCurrentThread();
        for (var i = 0; i < 1000; i++) await native.Callback(message, default);
        var allocated = GC.GetAllocatedBytesForCurrentThread() - before;
        await Assert.That(allocated).IsEqualTo(0L);
    }

    private sealed class CallbackSubscriber : SubscriberClient
    {
        internal Func<PubsubMessage, CancellationToken, Task<Reply>> Callback { get; private set; } = null!;
        private readonly TaskCompletionSource _completion = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public override Task StartAsync(Func<PubsubMessage, CancellationToken, Task<Reply>> handlerAsync)
        {
            Callback = handlerAsync;
            return _completion.Task;
        }
        public override ValueTask DisposeAsync()
        {
            _completion.TrySetResult();
            return default;
        }
    }
}
