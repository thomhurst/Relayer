using Microsoft.Extensions.DependencyInjection;
using Relayer.AzureServiceBus;
using Relayer.Transports;
using static Relayer.Tests.ServiceBusReceiveTests;

namespace Relayer.Tests;

public sealed class ServiceBusHostedRouteTests
{
    [Test]
    [Arguments("orders", "ORDERS", null, false)]
    [Arguments("orders", "orders", null, true)]
    [Arguments("events", "events", "audit", false)]
    public async Task Custom_prefix_fails_before_hosted_processor_creation(
        string destination, string source, string? subscription, bool sessions)
    {
        var client = new NativeClient();
        await using var provider = Provider(client, destination, source, subscription, sessions, "custom:");

        await Assert.That(async () => await Start(provider)).Throws<InvalidOperationException>()
            .WithMessageContaining($"to destination '{destination}' writes 'custom:type'. Hosted consumers require 'cloudEvents:type'");
        await Assert.That(client.Processors.Count + client.SessionProcessors.Count).IsEqualTo(0);
    }

    [Test]
    [Arguments("orders", null)]
    [Arguments("external", "custom:")]
    public async Task Compatible_prefix_or_external_destination_allows_startup(string destination, string? prefix)
    {
        var client = new NativeClient();
        await using var provider = Provider(client, destination, "orders", null, false, prefix);

        await Start(provider);
        await Assert.That(client.Processors.Single().Source).IsEqualTo("orders");
        await Stop(provider);
    }

    private static ServiceProvider Provider(NativeClient client, string destination, string source,
        string? subscription, bool sessions, string? prefix)
    {
        var services = new ServiceCollection();
        services.AddSingleton<Control>();
        services.AddRelayer(builder =>
        {
            builder.Infrastructure().MessageTypes.Add<Payload>("servicebus.receive-test");
            builder.UseJsonSerializer(ServiceBusReceiveTestJson.Default);
            builder.ServiceBus("bus", bus =>
            {
                bus.TopologyMode = TopologyMode.None;
                bus.Options.Configure(options => options.Client = client);
                bus.Send<Payload>(destination, route =>
                {
                    if (prefix is not null)
                    {
                        route.SendOptions = new()
                        {
                            HeaderNames = new(prefix)
                        };
                    }
                });
                bus.Consume(source, consumer =>
                {
                    consumer.Handler<Handler>();
                    consumer.Options.Subscription = subscription;

                    if (sessions)
                    {
                        consumer.Options.SessionProcessorOptions = new()
                        {
                            AutoCompleteMessages = false,
                            MaxConcurrentSessions = 1
                        };
                    }
                });
            });
        });
        return services.BuildServiceProvider();
    }
}
