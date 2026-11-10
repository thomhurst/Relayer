using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Relayer.AzureServiceBus;
using Relayer.HealthChecks;

namespace Relayer.Tests;

public sealed class ServiceBusHealthProbeTests
{
    [Test]
    public async Task Send_only_probe_reopens_native_link_each_poll_and_preserves_failures_without_probe_messages()
    {
        var client = new ServiceBusSendTests.NativeClient();
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddRelayerServiceBusHealthProviders("ExactBus");
        services.AddRelayerServiceBusHealthProviders("ExactBus");
        services.AddRelayer(builder => builder.ServiceBus("ExactBus", bus =>
        {
            bus.TopologyMode = TopologyMode.None;
            bus.Options.Configure(options => { options.Client = client; options.Queues.Add(new("orders")); });
            bus.Send<ServiceBusSendTests.First>("orders");
        }));
        await using var provider = services.BuildServiceProvider();
        var probe = provider.GetRequiredKeyedService<ITransportHealthProbe>("ExactBus");
        await Assert.That(provider.GetKeyedService<ITransportHealthProbe>("exactbus")).IsNull();
        await Assert.That((await probe.CheckAsync(default)).Status).IsEqualTo(HealthStatus.Healthy);
        await Assert.That((await probe.CheckAsync(default)).Status).IsEqualTo(HealthStatus.Healthy);
        var failure = new ServiceBusException("disconnected", ServiceBusFailureReason.ServiceCommunicationProblem);
        client.BatchFailure = failure;
        var disconnected = await probe.CheckAsync(default);
        await Assert.That(disconnected.Status).IsEqualTo(HealthStatus.Unhealthy);
        await Assert.That(disconnected.Exception).IsSameReferenceAs(failure);
        // One cached send endpoint plus one new native link per poll, disposed even when attachment fails.
        await Assert.That(client.Senders.Count).IsEqualTo(4);
        foreach (var sender in client.Senders.Skip(1))
        {
            await Assert.That(sender.BatchCreations).IsEqualTo(1);
            await Assert.That(sender.Disposals).IsEqualTo(1);
            await Assert.That(sender.Sent.Count).IsEqualTo(0);
        }
        using var cancellation = new CancellationTokenSource();
        cancellation.Cancel();
        await Assert.That(async () => await probe.CheckAsync(cancellation.Token)).Throws<OperationCanceledException>();
        await Assert.That(client.Senders.Count).IsEqualTo(4);
        client.BatchFailure = null;
        await Assert.That((await probe.CheckAsync(default)).Status).IsEqualTo(HealthStatus.Healthy);
    }
}
