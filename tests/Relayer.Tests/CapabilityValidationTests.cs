using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using Relayer.Transports;

namespace Relayer.Tests;

public class CapabilityValidationTests
{
    [Test]
    [Arguments(false, false)]
    [Arguments(false, true)]
    [Arguments(true, true)]
    [Arguments(true, false)]
    public async Task Ordering_uses_the_destination_capability(bool ordered, bool scalar)
    {
        var transport = new TestTransport("events", _ => new() { OrderedByKey = ordered });
        await using var provider = Create(transport, route =>
        {
            if (scalar) route.OrderingKey(message => message.OrderId);
            else route.OrderingKey(_ => "customer");
        });

        var failure = await Start(provider);

        if (ordered) await Assert.That(failure).IsNull();
        else
        {
            await Assert.That(failure).IsNotNull();
            foreach (var expected in new[] { "events", "orders", nameof(OrderPlaced), "OrderedByKey", "ordering" })
                await Assert.That(failure!.Message).Contains(expected);
        }
    }

    [Test]
    public async Task Unordered_routes_do_not_require_ordering_support()
    {
        await using var provider = Create(new TestTransport("events", _ => new()));
        await Assert.That(await Start(provider)).IsNull();
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task Each_endpoint_is_resolved_once_for_routes_and_consumers(bool composeSenderFirst)
    {
        var transport = new TestTransport("events", endpoint => new() { OrderedByKey = endpoint == "orders" });
        await using var provider = Create(transport, route => route.OrderingKey(message => message.OrderId), broker =>
        {
            broker.Send<OrderCancelled>("unordered");
            broker.Consume("orders", consumer => consumer.Handler<BuilderTests.OrderHandler>());
            broker.Consume("unordered", consumer => consumer.Handler<BuilderTests.OrderHandler>());
        });

        if (composeSenderFirst)
        {
            using var scope = provider.CreateScope();
            _ = scope.ServiceProvider.GetRequiredService<ISender>();
        }

        await Assert.That(await Start(provider)).IsNull();
        await Assert.That(transport.Lookups["orders"]).IsEqualTo(1);
        await Assert.That(transport.Lookups["unordered"]).IsEqualTo(1);
    }

    [Test]
    public async Task Consumer_only_endpoints_are_validated()
    {
        var services = new ServiceCollection();
        services.AddSingleton<ITransport>(new TestTransport("events", _ => new() { MaxBatchSize = 0 }));
        services.AddRelayer(relayer => relayer.AddTransport<BuilderTests.BrokerOptions,
            BuilderTests.ConsumerOptions>("events", broker =>
            broker.Consume("inbound", consumer => consumer.Handler<BuilderTests.OrderHandler>())));
        await using var provider = services.BuildServiceProvider();

        var failure = await Start(provider);
        await Assert.That(failure).IsNotNull();
        await Assert.That(failure!.Message).Contains("inbound");
        await Assert.That(failure.Message).Contains("MaxBatchSize");
    }

    [Test]
    [Arguments("delivery")]
    [Arguments("redelivery")]
    [Arguments("batch")]
    [Arguments("bytes")]
    [Arguments("negative-bytes")]
    [Arguments("model")]
    public async Task Invalid_capability_values_fail_startup(string field)
    {
        var (capabilities, property) = field switch
        {
            "delivery" => (new TransportCapabilities { MaxDeliveryDelay = TimeSpan.FromTicks(-1) }, "MaxDeliveryDelay"),
            "redelivery" => (new TransportCapabilities { MaxRedeliveryDelay = TimeSpan.FromTicks(-1) }, "MaxRedeliveryDelay"),
            "batch" => (new TransportCapabilities { MaxBatchSize = 0 }, "MaxBatchSize"),
            "bytes" => (new TransportCapabilities { MaxMessageBytes = 0 }, "MaxMessageBytes"),
            "negative-bytes" => (new TransportCapabilities { MaxMessageBytes = -1 }, "MaxMessageBytes"),
            _ => (new TransportCapabilities { ConsumptionModel = (ConsumptionModel)123 }, "ConsumptionModel"),
        };
        await using var provider = Create(new TestTransport("events", _ => capabilities));
        var failure = await Start(provider);
        await Assert.That(failure).IsNotNull();
        foreach (var expected in new[] { "events", "orders", property })
            await Assert.That(failure!.Message).Contains(expected);
    }

    [Test]
    public async Task Zero_delays_and_unknown_message_size_are_valid()
    {
        await using var provider = Create(new TestTransport("events", _ => new()
        {
            MaxDeliveryDelay = TimeSpan.Zero,
            MaxRedeliveryDelay = TimeSpan.Zero,
        }));
        await Assert.That(await Start(provider)).IsNull();
    }

    [Test]
    public async Task Named_adapters_are_case_sensitive_and_missing_adapters_fail()
    {
        await using var provider = Create(new TestTransport("Events", _ => new() { OrderedByKey = true }));
        var failure = await Start(provider);
        await Assert.That(failure).IsNotNull();
        await Assert.That(failure!.Message).Contains("events");
        await Assert.That(failure.Message).Contains("ITransport");
    }

    [Test]
    public async Task Duplicate_adapter_names_fail_without_selecting_an_arbitrary_adapter()
    {
        var first = new TestTransport("events", _ => new() { OrderedByKey = true });
        var second = new TestTransport("events", _ => new());
        await using var provider = Create(first, extra: second);
        var failure = await Start(provider);
        await Assert.That(failure).IsNotNull();
        await Assert.That(failure!.Message).Contains("events");
        await Assert.That(failure.Message).Contains("more than once");
        await Assert.That(first.Lookups.Count).IsEqualTo(0);
        await Assert.That(second.Lookups.Count).IsEqualTo(0);
    }

    [Test]
    public async Task Capability_errors_aggregate_with_configuration_errors_before_receivers_start()
    {
        var receiver = new Receiver();
        var transport = new TestTransport("events", _ => throw new InvalidOperationException("Endpoint not declared."));
        await using var provider = Create(transport, additional: broker =>
        {
            broker.Options.Validate(_ => false, "credentials required");
#pragma warning disable RLY004 // Deliberately omit the handler to exercise aggregate startup validation.
            broker.Consume("orders", consumer => consumer.Concurrency(0));
#pragma warning restore RLY004
        }, receiver: receiver);
        var failure = await Start(provider);
        await Assert.That(failure).IsNotNull();
        foreach (var expected in new[] { "events", "orders", "Endpoint not declared.", "credentials required", "concurrency" })
            await Assert.That(failure!.Message).Contains(expected);
        await Assert.That(receiver.Started).IsFalse();
        await Assert.That(transport.Lookups["orders"]).IsEqualTo(1);
    }

    [Test]
    public async Task Null_capabilities_fail_with_endpoint_context()
    {
        await using var provider = Create(new TestTransport("events", _ => null!));
        var failure = await Start(provider);
        await Assert.That(failure).IsNotNull();
        await Assert.That(failure!.Message).Contains("orders");
        await Assert.That(failure.Message).Contains("TransportCapabilities");
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task Endpoint_resolution_failures_include_context(bool unsupported)
    {
        await using var provider = Create(new TestTransport("events", _ => unsupported
            ? throw new NotSupportedException("Endpoint unavailable.")
            : throw new ArgumentException("Endpoint unavailable.")));
        var failure = await Start(provider);
        await Assert.That(failure).IsNotNull();
        foreach (var expected in new[] { "events", "orders", "Endpoint unavailable." })
            await Assert.That(failure!.Message).Contains(expected);
    }

    [Test]
    public async Task Invalid_options_used_by_adapter_factories_preserve_declaration_errors()
    {
        var services = new ServiceCollection();
        services.AddSingleton<ITransport>(provider =>
        {
            _ = provider.GetRequiredService<IOptionsMonitor<BuilderTests.BrokerOptions>>().Get("events");
            return new TestTransport("events", _ => new());
        });
        services.AddRelayer(relayer => relayer.AddTransport<BuilderTests.BrokerOptions,
            BuilderTests.ConsumerOptions>("events", broker =>
        {
            broker.Options.Validate(_ => false, "credentials required");
#pragma warning disable RLY004 // Deliberately omit the handler to preserve declaration errors when adapter creation fails.
            broker.Consume("orders", consumer => consumer.Concurrency(0));
#pragma warning restore RLY004
        }));
        await using var provider = services.BuildServiceProvider();

        var failure = await Start(provider);
        await Assert.That(failure).IsNotNull();
        foreach (var expected in new[] { "events", "credentials required", "concurrency", "handler" })
            await Assert.That(failure!.Message).Contains(expected);
    }

    [Test]
    public async Task Identical_endpoint_names_on_different_adapters_have_independent_capabilities()
    {
        var ordered = new TestTransport("events", _ => new() { OrderedByKey = true });
        var unordered = new TestTransport("commands", _ => new());
        var services = new ServiceCollection();
        services.AddSingleton<ITransport>(ordered);
        services.AddSingleton<ITransport>(unordered);
        services.AddRelayer(relayer =>
        {
            relayer.AddMessageTypesFromThisAssembly();
            relayer.AddTransport<BuilderTests.BrokerOptions, BuilderTests.ConsumerOptions>("events", broker =>
                broker.Send<OrderPlaced>("orders", route => route.OrderingKey(message => message.OrderId)));
            relayer.AddTransport<BuilderTests.BrokerOptions, BuilderTests.ConsumerOptions>("commands", broker =>
                broker.Send<OrderCancelled>("orders", route => route.OrderingKey(_ => "customer")));
        });
        await using var provider = services.BuildServiceProvider();

        var failure = await Start(provider);
        await Assert.That(failure).IsNotNull();
        await Assert.That(failure!.Failures.Count()).IsEqualTo(1);
        await Assert.That(failure.Message).Contains("commands");
        await Assert.That(ordered.Lookups["orders"]).IsEqualTo(1);
        await Assert.That(unordered.Lookups["orders"]).IsEqualTo(1);
    }

    [Test]
    public async Task All_invalid_values_on_an_endpoint_are_reported_together()
    {
        await using var provider = Create(new TestTransport("events", _ => new()
        {
            MaxDeliveryDelay = TimeSpan.FromTicks(-1),
            MaxRedeliveryDelay = TimeSpan.FromTicks(-1),
            MaxBatchSize = -1,
            MaxMessageBytes = -1,
            ConsumptionModel = (ConsumptionModel)123,
        }));
        var failure = await Start(provider);
        await Assert.That(failure).IsNotNull();
        await Assert.That(failure!.Failures.Count()).IsEqualTo(5);
    }

    [Test]
    public async Task Blank_adapter_names_fail_startup()
    {
        await using var provider = Create(new TestTransport(" ", _ => new()));
        var failure = await Start(provider);
        await Assert.That(failure).IsNotNull();
        await Assert.That(failure!.Message).Contains("adapter name must not be blank");
    }

    [Test]
    public async Task Blank_sources_are_reported_without_calling_the_native_resolver()
    {
        var transport = new TestTransport("events", _ => new());
        await using var provider = Create(transport, additional: broker =>
            broker.Consume(" ", consumer => consumer.Handler<BuilderTests.OrderHandler>()));
        var failure = await Start(provider);
        await Assert.That(failure).IsNotNull();
        await Assert.That(failure!.Message).Contains("source must not be blank");
        await Assert.That(transport.Lookups.ContainsKey(" ")).IsFalse();
    }

    [Test]
    public async Task Declaration_only_hosts_do_not_require_runtime_adapters()
    {
        await using var provider = Create(null, route => route.OrderingKey(message => message.OrderId));
        await Assert.That(await Start(provider)).IsNull();
    }

    [Test]
    [Arguments(-1, 0, false, false, false, false)]
    [Arguments(0, 0, false, false, false, true)]
    [Arguments(10, 9, false, false, false, true)]
    [Arguments(10, 10, false, false, false, true)]
    [Arguments(10, 11, false, false, false, false)]
    [Arguments(-1, 10, true, false, false, false)]
    [Arguments(10, 11, true, true, false, true)]
    [Arguments(-1, 10, true, true, false, true)]
    [Arguments(-1, 10, true, true, true, false)]
    public async Task Scheduling_selects_native_boundaries_or_an_executable_fallback(int nativeTicks, int requiredTicks,
        bool hasOutbox, bool scheduler, bool ordered, bool valid)
    {
        var transport = new TestTransport("events", _ => new()
        {
            MaxDeliveryDelay = nativeTicks < 0 ? null : TimeSpan.FromTicks(nativeTicks), OrderedByKey = ordered
        });
        await using var provider = Create(transport, route =>
        {
            route.SendOptions = new() { RequiredDeliveryDelay = TimeSpan.FromTicks(requiredTicks) };
            if (ordered) route.OrderingKey(_ => "key");
        }, outbox: hasOutbox ? new SchedulingOutbox(scheduler) : null);
        var failure = await Start(provider);
        await Assert.That(failure is null).IsEqualTo(valid);
        if (!valid)
            foreach (var part in new[] { nameof(OrderPlaced), "orders", "events", "RequiredDeliveryDelay", "dispatch worker" })
                await Assert.That(failure!.Message).Contains(part);
    }

    [Test]
    [Arguments(SendTransactionRequirement.None, false, false, true)]
    [Arguments(SendTransactionRequirement.Native, true, false, true)]
    [Arguments(SendTransactionRequirement.Native, false, true, false)]
    [Arguments(SendTransactionRequirement.Outbox, true, false, false)]
    [Arguments(SendTransactionRequirement.Outbox, false, true, true)]
    [Arguments((SendTransactionRequirement)99, true, true, false)]
    public async Task Transaction_requirements_preserve_the_requested_guarantee(SendTransactionRequirement requirement,
        bool native, bool outbox, bool valid)
    {
        await using var provider = Create(new TestTransport("events", _ => new() { Transactions = native }),
            route => route.SendOptions = new() { TransactionRequirement = requirement },
            outbox: outbox ? new SchedulingOutbox(false) : null);
        var failure = await Start(provider);
        await Assert.That(failure is null).IsEqualTo(valid);
        if (!valid)
            foreach (var part in new[] { nameof(OrderPlaced), "events", "Transaction" })
                await Assert.That(failure!.Message).Contains(part);
    }

    [Test]
    public async Task Native_transactions_cannot_use_a_database_scheduler_fallback()
    {
        await using var provider = Create(new TestTransport("events", _ => new() { Transactions = true }),
            route => route.SendOptions = new() { TransactionRequirement = SendTransactionRequirement.Native,
                RequiredDeliveryDelay = TimeSpan.FromSeconds(1) }, outbox: new SchedulingOutbox(true));
        await Assert.That(await Start(provider)).IsNotNull();
    }

    [Test]
    public async Task Negative_required_delay_fails_before_receivers_start()
    {
        var receiver = new Receiver();
        await using var provider = Create(new TestTransport("events", _ => new()),
            route => route.SendOptions = new() { RequiredDeliveryDelay = TimeSpan.FromTicks(-1) }, receiver: receiver);
        await Assert.That(await Start(provider)).IsNotNull();
        await Assert.That(receiver.Started).IsFalse();
    }

    [Test]
    public async Task Ordered_scheduling_fallback_requires_the_ordered_executor()
    {
        await using var provider = Create(new TestTransport("events", _ => new() { OrderedByKey = true }), route =>
        {
            route.OrderingKey(_ => "key");
            route.SendOptions = new() { RequiredDeliveryDelay = TimeSpan.FromDays(1) };
        }, outbox: new SchedulingOutbox(true, ordered: true));
        await Assert.That(await Start(provider)).IsNull();
    }

    [Test]
    public async Task Required_outbox_scheduling_cannot_use_native_delay_instead_of_a_due_worker()
    {
        await using var provider = Create(new TestTransport("events", _ => new() { MaxDeliveryDelay = TimeSpan.MaxValue }),
            route => route.SendOptions = new() { RequiredDeliveryDelay = TimeSpan.FromSeconds(1),
                TransactionRequirement = SendTransactionRequirement.Outbox }, outbox: new SchedulingOutbox(false));
        await Assert.That(await Start(provider)).IsNotNull();
    }

    private sealed class SchedulingOutbox(bool scheduled, bool ordered = false) : IOutboxEnlistment
    {
        public bool IsActive => false;
        public bool SupportsScheduling => scheduled;
        public bool SupportsOrderedScheduling => ordered;
        public ValueTask EnlistAsync(OutboxRecord record, CancellationToken cancellationToken) => throw new NotSupportedException();
    }

    private static ServiceProvider Create(TestTransport? transport,
        Action<RouteBuilder<OrderPlaced>>? route = null,
        Action<TransportBuilder<BuilderTests.BrokerOptions, BuilderTests.ConsumerOptions>>? additional = null,
        TestTransport? extra = null, Receiver? receiver = null, IOutboxEnlistment? outbox = null)
    {
        var services = new ServiceCollection();
        if (outbox is not null) services.AddScoped<IOutboxEnlistment>(_ => outbox);
        if (transport is not null) services.AddSingleton<ITransport>(transport);
        if (extra is not null) services.AddSingleton<ITransport>(extra);
        services.AddRelayer(relayer =>
        {
            relayer.AddMessageTypesFromThisAssembly();
            relayer.AddTransport<BuilderTests.BrokerOptions, BuilderTests.ConsumerOptions>("events", broker =>
            {
                broker.Send("orders", route);
                additional?.Invoke(broker);
                if (receiver is not null) services.AddSingleton<IHostedService>(receiver);
            });
        });
        return services.BuildServiceProvider();
    }

    private static async Task<OptionsValidationException?> Start(IServiceProvider provider)
    {
        try
        {
            foreach (var service in provider.GetServices<IHostedService>().OfType<IHostedLifecycleService>())
                await service.StartingAsync(CancellationToken.None);
            foreach (var service in provider.GetServices<IHostedService>())
                await service.StartAsync(CancellationToken.None);
            return null;
        }
        catch (OptionsValidationException error) { return error; }
    }

    private sealed class Receiver : IHostedService
    {
        public bool Started { get; private set; }
        public Task StartAsync(CancellationToken cancellationToken) { Started = true; return Task.CompletedTask; }
        public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
    }

    private sealed class TestTransport(string name, Func<string, TransportCapabilities> resolve) : ITransport
    {
        public string Name => name;
        public Dictionary<string, int> Lookups { get; } = new(StringComparer.Ordinal);
        public TransportCapabilities GetCapabilities(string endpoint)
        {
            Lookups.TryGetValue(endpoint, out var count);
            Lookups[endpoint] = count + 1;
            return resolve(endpoint);
        }
        public ValueTask<TopologyReport> ApplyTopologyAsync(TopologyMode mode, CancellationToken cancellationToken)
            => ValueTask.FromResult(new TopologyReport([]));
        public ValueTask SendAsync(OutgoingMessage message, CancellationToken cancellationToken) => throw new NotSupportedException();
        public ValueTask SendBatchAsync(ReadOnlyMemory<OutgoingMessage> messages, CancellationToken cancellationToken) => throw new NotSupportedException();
        public Task StartAsync(CancellationToken cancellationToken) => Task.CompletedTask;
        public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
        public ValueTask DisposeAsync() => default;
    }
}
