using Relayer.Transports;
#pragma warning disable RLY004 // These fixtures intentionally declare empty consumers to test configuration validation.
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;

namespace Relayer.Tests;

public class BuilderTests
{
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task Send_defaults_apply_to_later_routes_and_route_overrides_take_precedence(bool replace)
    {
        var services = new ServiceCollection();
        services.AddRelayer(relayer => relayer.AddTransport<BrokerOptions, RouteOptions, ConsumerOptions>("events", broker =>
        {
            broker.Send<OrderPlaced>("before");
            broker.SendDefaults = new() { Source = "/defaults", HeaderNames = new("cloudEvents:") };
            broker.Send<OrderCancelled>("after", route =>
            {
                if (replace) route.SendOptions = new() { Source = "/override", HeaderNames = new("custom:") };
            });
        }));
        await using var provider = services.BuildServiceProvider();
        var routes = provider.GetRequiredService<RelayerConfiguration>().Transports[0].Routes;
        var before = (RouteDefinition<OrderPlaced, RouteOptions>)routes[0];
        var after = (RouteDefinition<OrderCancelled, RouteOptions>)routes[1];
        await Assert.That(before.SendOptions.HeaderNames.Id).IsEqualTo("ce_id");
        await Assert.That(after.SendOptions.HeaderNames.Id).IsEqualTo(replace ? "custom:id" : "cloudEvents:id");
        await Assert.That(after.SendOptions.Source).IsEqualTo(replace ? "/override" : "/defaults");
    }

    [Test]
    public async Task Configuration_call_site_preserves_native_types_and_typed_keys()
    {
        var services = new ServiceCollection();
        var orderId = Guid.NewGuid();
        services.AddRelayer(relayer =>
        {
            relayer.AddMessageTypesFromThisAssembly();
            relayer.AddTransport<BrokerOptions, RouteOptions, ConsumerOptions>("events", broker =>
            {
                broker.Options.Configure(o => o.Server = "localhost");
                broker.Send<OrderPlaced>("orders", route => route.OrderingKey(m => m.OrderId));
                broker.Consume("orders", consumer =>
                {
                    consumer.Options.Group = "billing";
                    consumer.Handler<OrderHandler>().Concurrency(8).Batch(10, TimeSpan.FromMilliseconds(20));
                });
            });
        });
        await using var provider = services.BuildServiceProvider();
        await Start(provider);
        var configuration = provider.GetRequiredService<RelayerConfiguration>();
        var transport = configuration.Transports[0];
        var route = (RouteDefinition<OrderPlaced, RouteOptions>)transport.Routes[0];
        var consumer = (ConsumerDefinition<ConsumerOptions>)transport.Consumers[0];
        await Assert.That(configuration.TopologyMode).IsEqualTo(TopologyMode.Validate);
        await Assert.That(route.Destination).IsEqualTo("orders");
        await Assert.That(route.GetOrderingKeySelector<Guid>()!(new OrderPlaced(orderId))).IsEqualTo(orderId);
        await Assert.That(consumer.Options.Group).IsEqualTo("billing");
        await Assert.That(consumer.Concurrency).IsEqualTo(8);
        await Assert.That(consumer.BatchSize).IsEqualTo(10);
        await Assert.That(consumer.BatchWait).IsEqualTo(TimeSpan.FromMilliseconds(20));
        await using var first = provider.CreateAsyncScope();
        await using var second = provider.CreateAsyncScope();
        await Assert.That(ReferenceEquals(first.ServiceProvider.GetRequiredService<OrderHandler>(),
            second.ServiceProvider.GetRequiredService<OrderHandler>())).IsFalse();
    }

    [Test]
    public async Task Named_options_bind_reload_and_remain_isolated()
    {
        var configuration = new ConfigurationBuilder().AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["Events:Server"] = "first",
            ["Commands:Server"] = "other"
        }).Build();
        var services = new ServiceCollection();
        services.AddRelayer(relayer =>
        {
            relayer.AddTransport<BrokerOptions, RouteOptions, ConsumerOptions>("events", b =>
                b.Options.Bind(configuration.GetSection("Events")));
            relayer.AddTransport<BrokerOptions, RouteOptions, ConsumerOptions>("commands", b =>
                b.Options.Bind(configuration.GetSection("Commands")));
        });
        await using var provider = services.BuildServiceProvider();
        await Start(provider);
        var monitor = provider.GetRequiredService<IOptionsMonitor<BrokerOptions>>();
        await Assert.That(monitor.Get("events").Server).IsEqualTo("first");
        await Assert.That(monitor.Get("commands").Server).IsEqualTo("other");
        configuration["Events:Server"] = "changed";
        configuration.Reload();
        await Assert.That(monitor.Get("events").Server).IsEqualTo("changed");
        await Assert.That(monitor.Get("commands").Server).IsEqualTo("other");
    }

    [Test]
    public async Task Startup_reports_all_declaration_and_options_errors_together()
    {
        var services = new ServiceCollection();
        services.AddRelayer(relayer =>
        {
            relayer.TopologyMode = (TopologyMode)123;
            relayer.AddTransport<BrokerOptions, RouteOptions, ConsumerOptions>("events", b =>
            {
                b.Options.Validate(_ => false, "server required");
                b.Send<OrderPlaced>("");
                b.Consume("", c => c.Concurrency(0).Batch(0, TimeSpan.Zero));
            });
            relayer.AddTransport<BrokerOptions, RouteOptions, ConsumerOptions>("events", b => b.Send<OrderPlaced>("other"));
            relayer.AddTransport<OtherBrokerOptions, RouteOptions, ConsumerOptions>("commands", b =>
                b.Options.Validate(_ => false, "credentials required"));
        });
        await using var provider = services.BuildServiceProvider();
        OptionsValidationException? failure = null;
        try { await Start(provider); }
        catch (OptionsValidationException error) { failure = error; }
        await Assert.That(failure).IsNotNull();
        var errors = failure!.Message;
        foreach (var expected in new[] { "TopologyMode", "server required", "credentials required", "destination",
                     "stable message identity", "source", "handler", "concurrency", "batch size", "batch wait", "more than once", "only one route" })
            await Assert.That(errors).Contains(expected);
    }

    [Test]
    public async Task Validation_runs_before_transport_hosted_services()
    {
        var services = new ServiceCollection();
        services.AddRelayer(relayer => relayer.AddTransport<BrokerOptions, RouteOptions, ConsumerOptions>("events", b =>
        {
            services.AddSingleton<IHostedService, TransportService>();
            b.Consume("orders", _ => { });
        }));
        await using var provider = services.BuildServiceProvider();
        await Assert.That(provider.GetServices<IHostedService>().First()).IsTypeOf<TopologyStartupService>();
        await Assert.That(async () => await Start(provider)).Throws<OptionsValidationException>();
        await Assert.That(provider.GetServices<IHostedService>().OfType<TransportService>().Single().Started).IsFalse();
    }

    [Test]
    public async Task String_key_and_unconfigured_key_remain_distinct()
    {
        var services = new ServiceCollection();
        services.AddRelayer(relayer =>
        {
            relayer.AddMessageTypesFromThisAssembly();
            relayer.AddTransport<BrokerOptions, RouteOptions, ConsumerOptions>("events", b =>
            {
                b.Send<OrderPlaced>("placed", r => r.OrderingKey(_ => "customer"));
                b.Send<OrderCancelled>("cancelled");
            });
        });
        await using var provider = services.BuildServiceProvider();
        await Start(provider);
        var routes = provider.GetRequiredService<RelayerConfiguration>().Transports[0].Routes;
        var placed = (RouteDefinition<OrderPlaced, RouteOptions>)routes[0];
        var cancelled = (RouteDefinition<OrderCancelled, RouteOptions>)routes[1];
        await Assert.That(placed.GetOrderingKeySelector<string>()!(new OrderPlaced(Guid.Empty))).IsEqualTo("customer");
        await Assert.That(cancelled.HasOrderingKey).IsFalse();
        await Assert.That(cancelled.GetOrderingKeySelector<Guid>()).IsNull();
        await Assert.That(() => placed.GetOrderingKeySelector<Guid>()).Throws<InvalidOperationException>();
    }

    [Test]
    public async Task Captured_builders_cannot_add_declarations_after_registration()
    {
        IRelayerBuilder? root = null;
        TransportBuilder<BrokerOptions, RouteOptions, ConsumerOptions>? transport = null;
        ConsumerBuilder<ConsumerOptions>? consumer = null;
        RouteBuilder<OrderPlaced, RouteOptions>? route = null;
        new ServiceCollection().AddRelayer(relayer =>
        {
            root = relayer;
            relayer.AddTransport<BrokerOptions, RouteOptions, ConsumerOptions>("events", b =>
            {
                transport = b;
                b.Send<OrderPlaced>("orders", r => route = r);
                b.Consume("orders", c => consumer = c);
            });
        });
        await Assert.That(() => root!.AddTransport<BrokerOptions, RouteOptions, ConsumerOptions>("late", _ => { }))
            .Throws<InvalidOperationException>();
        await Assert.That(() => transport!.Send<OrderCancelled>("late")).Throws<InvalidOperationException>();
        await Assert.That(() => transport!.SendDefaults = new()).Throws<InvalidOperationException>();
        await Assert.That(() => consumer!.Concurrency(10)).Throws<InvalidOperationException>();
        var serviceCount = root!.Services.Count;
        await Assert.That(() => consumer!.Handler<OrderHandler>()).Throws<InvalidOperationException>();
        await Assert.That(root.Services.Count).IsEqualTo(serviceCount);
        await Assert.That(() => route!.OrderingKey(_ => "late")).Throws<InvalidOperationException>();
        await Assert.That(() => root!.TopologyMode = TopologyMode.None).Throws<InvalidOperationException>();
    }

    [Test]
    public async Task Invalid_bound_values_are_reported_with_other_transports_errors()
    {
        var configuration = new ConfigurationBuilder().AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["Count"] = "invalid"
        }).Build();
        var services = new ServiceCollection();
        services.AddRelayer(relayer =>
        {
            relayer.AddTransport<NumericOptions, RouteOptions, ConsumerOptions>("numeric", b => b.Options.Bind(configuration));
            relayer.AddTransport<BrokerOptions, RouteOptions, ConsumerOptions>("other", b =>
                b.Options.Validate(_ => false, "other failure"));
        });
        await using var provider = services.BuildServiceProvider();
        OptionsValidationException? failure = null;
        try { await Start(provider); }
        catch (OptionsValidationException error) { failure = error; }
        await Assert.That(failure).IsNotNull();
        await Assert.That(failure!.Message).Contains("numeric");
        await Assert.That(failure.Message).Contains("other failure");
    }

    [Test]
    public async Task Existing_handler_registration_is_preserved_and_defaults_are_safe()
    {
        var handler = new OrderHandler();
        var services = new ServiceCollection();
        services.AddSingleton(handler);
        services.AddRelayer(relayer => relayer.AddTransport<BrokerOptions, RouteOptions, ConsumerOptions>("events", b =>
            b.Consume("orders", c => c.Handler<OrderHandler>().Handler<OrderHandler>())));
        await using var provider = services.BuildServiceProvider();
        await Start(provider);
        var consumer = provider.GetRequiredService<RelayerConfiguration>().Transports[0].Consumers[0];
        await Assert.That(consumer.Handlers.Count).IsEqualTo(1);
        await Assert.That(consumer.Concurrency).IsEqualTo(1);
        await Assert.That(consumer.BatchSize).IsEqualTo(1);
        await Assert.That(ReferenceEquals(handler, provider.GetRequiredService<OrderHandler>())).IsTrue();
    }

    private static async Task Start(IServiceProvider provider)
    {
        foreach (var service in provider.GetServices<IHostedService>().OfType<IHostedLifecycleService>())
            await service.StartingAsync(CancellationToken.None);
        foreach (var service in provider.GetServices<IHostedService>())
            await service.StartAsync(CancellationToken.None);
    }

    public sealed class BrokerOptions { public string Server { get; set; } = ""; }
    public sealed class OtherBrokerOptions { }
    public sealed class NumericOptions { public int Count { get; set; } }
    public sealed class RouteOptions { }
    public sealed class ConsumerOptions { public string Group { get; set; } = ""; }
    public sealed class OrderHandler : IHandler<OrderPlaced>
    {
        public ValueTask HandleAsync(OrderPlaced message, MessageContext context, CancellationToken cancellationToken) => ValueTask.CompletedTask;
    }
    private sealed class TransportService : IHostedService
    {
        public bool Started { get; private set; }
        public Task StartAsync(CancellationToken cancellationToken) { Started = true; return Task.CompletedTask; }
        public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
    }
}
