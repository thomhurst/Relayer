using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Relayer.Transports;

namespace Relayer.Kafka.Tests;

public class KafkaConfigurationTests
{
    [Test]
    public async Task Named_options_and_route_keys_remain_isolated()
    {
        var services = new ServiceCollection();
        services.AddRelayer(builder =>
        {
            builder.Kafka("first", kafka =>
            {
                kafka.Options.Configure(options => options.BootstrapServers.Add("first:9092"));
                kafka.Send<Order>("orders", route => route.OrderingKey(static order => order.Customer));
            });
            builder.Kafka("second", kafka =>
            {
                kafka.Options.Configure(options => options.BootstrapServers.Add("second:9092"));
                kafka.Send<OtherOrder>("others", route => route.OrderingKey(static order => order.Number));
            });
        });
        await using var provider = services.BuildServiceProvider();
        var options = provider.GetRequiredService<IOptionsMonitor<KafkaTransportOptions>>();
        await Assert.That(options.Get("first").BootstrapServers.Single()).IsEqualTo("first:9092");
        await Assert.That(options.Get("second").BootstrapServers.Single()).IsEqualTo("second:9092");
        var transports = provider.GetRequiredService<RelayerConfiguration>().Transports;
        var first = (RouteDefinition<Order, KafkaRouteOptions>)transports[0].Routes.Single();
        var second = (RouteDefinition<OtherOrder, KafkaRouteOptions>)transports[1].Routes.Single();
        await Assert.That(first.GetOrderingKeySelector<string>()!(new("customer-1", 1))).IsEqualTo("customer-1");
        await Assert.That(second.GetOrderingKeySelector<int>()!(new(42))).IsEqualTo(42);
    }

    [Test]
    public async Task Unimplemented_consumers_fail_before_building_a_native_client()
    {
        var services = new ServiceCollection();
        services.AddRelayer(builder => builder.Kafka("events", kafka => kafka.Consume("orders", _ => { })));
        await using var provider = services.BuildServiceProvider();
        await Assert.That(() => provider.GetServices<ITransport>().ToArray())
            .Throws<NotSupportedException>().WithMessageContaining("consumer execution is not implemented");
    }
}
