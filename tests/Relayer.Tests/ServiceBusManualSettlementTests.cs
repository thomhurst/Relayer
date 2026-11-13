using Relayer.Transports;
using Microsoft.Extensions.DependencyInjection;
using Relayer.AzureServiceBus;

namespace Relayer.Tests;

public sealed class ServiceBusManualSettlementTests
{
    [Test]
    public async Task Unset_disposition_is_rejected_without_acknowledging_delivery()
    {
        using var tracing = new MessageTracing(Microsoft.Extensions.Options.Options.Create(new TracingOptions()), []);
        var settlement = new ServiceBusSettlement(tracing, new() { System = "servicebus", Destination = "queue" });
        var receiver = new ServiceBusReceiveTests.NativeReceiver();
        await Assert.That(async () => await settlement.SettleAsync(ServiceBusReceiveTests.Args(receiver), default, default))
            .Throws<InvalidOperationException>();
        await Assert.That(receiver.Operations.Count).IsEqualTo(0);
    }

    [Test]
    public async Task Portable_request_after_native_completion_is_rejected_without_another_broker_operation()
    {
        var client = new ServiceBusReceiveTests.NativeClient();
        await using var provider = ServiceBusReceiveTests.Provider(client);
        provider.GetRequiredService<ServiceBusReceiveTests.Control>().Handle = async (_, context, _) =>
        {
            await context.ServiceBus().CompleteAsync();
            await Assert.That(() => context.Features.GetRequired<ISettlementFeature>().Retry(TimeSpan.Zero)).Throws<InvalidOperationException>();
        };
        await ServiceBusReceiveTests.Start(provider);
        var receiver = new ServiceBusReceiveTests.NativeReceiver();
        await client.Processors.Single().Emit(ServiceBusReceiveTests.Args(receiver));
        await Assert.That(receiver.Operations.ToArray()).IsEquivalentTo(new[] { "complete" });
        await ServiceBusReceiveTests.Stop(provider);
    }
}
