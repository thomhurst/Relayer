using Relayer.Transports;
using Azure.Messaging.ServiceBus;
using Relayer.AzureServiceBus;

namespace Relayer.Tests;

public sealed class ServiceBusConsumerLifecycleTests
{
    [Test]
    public async Task Native_stop_does_not_cancel_durable_work_before_the_host_drain_deadline()
    {
        using var nativeStop = new CancellationTokenSource();
        var processor = new DrainProcessor(nativeStop);
        var pending = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var workToken = CancellationToken.None;
        await using var consumer = new ServiceBusConsumer(Declaration(), processor, (_, token) =>
        { workToken = token; return new(pending.Task.WaitAsync(token)); });
        await consumer.StartAsync(default);
        var args = Args(nativeStop.Token);
        processor.Processing = consumer.ProcessAsync(args);
        var drain = consumer.StopAsync(default);
        await Assert.That(nativeStop.IsCancellationRequested).IsTrue();
        await Assert.That(workToken.IsCancellationRequested).IsFalse();
        await Assert.That(drain.IsCompleted).IsFalse();
        pending.SetResult();
        await drain;
    }

    [Test]
    public async Task Expired_host_drain_aborts_work_and_requires_a_new_host()
    {
        using var nativeStop = new CancellationTokenSource();
        using var deadline = new CancellationTokenSource();
        var processor = new DrainProcessor(nativeStop);
        var pending = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var workToken = CancellationToken.None;
        await using var consumer = new ServiceBusConsumer(Declaration(), processor, (_, token) =>
        { workToken = token; return new(pending.Task.WaitAsync(token)); });
        await consumer.StartAsync(default);
        processor.Processing = consumer.ProcessAsync(Args(nativeStop.Token));
        var drain = consumer.StopAsync(deadline.Token);
        deadline.Cancel();
        await Assert.That(async () => await drain).Throws<OperationCanceledException>();
        await Assert.That(workToken.IsCancellationRequested).IsTrue();
        await Assert.That(async () => await processor.Processing).Throws<OperationCanceledException>();
        await Assert.That(async () => await consumer.StartAsync(default)).Throws<InvalidOperationException>();
    }

    private static ConsumerDefinition<ServiceBusConsumerOptions> Declaration() => new("orders", new(), [], 1, 1, TimeSpan.FromMilliseconds(100));
    private static ProcessMessageEventArgs Args(CancellationToken token) => new(
        ServiceBusModelFactory.ServiceBusReceivedMessage(deliveryCount: 1), new ServiceBusReceiveTests.NativeReceiver(), "drain", token);

    private sealed class DrainProcessor(CancellationTokenSource nativeStop) : ServiceBusProcessor
    {
        internal Task Processing { get; set; } = Task.CompletedTask;
        public override Task StartProcessingAsync(CancellationToken cancellationToken = default) => Task.CompletedTask;
        public override async Task StopProcessingAsync(CancellationToken cancellationToken = default)
        {
            nativeStop.Cancel();
            try { await Processing; }
            catch (OperationCanceledException) when (nativeStop.IsCancellationRequested) { }
        }
        public override Task CloseAsync(CancellationToken cancellationToken = default) => Task.CompletedTask;
    }
}
