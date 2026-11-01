using System.Text;
using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Relayer.Transports;

namespace Relayer.Tests;

public class TopologyTests
{
    [Test]
    [Arguments(TopologyMode.Provision)]
    [Arguments(TopologyMode.Validate)]
    public async Task Concurrent_host_startup_waits_for_topology_before_starting_receivers(TopologyMode mode)
    {
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var adapter = new InMemoryTransport("events") { TopologyEntered = entered, TopologyRelease = release.Task };
        var receiver = new Receiver();
        using var host = CreateConcurrentHost(adapter, receiver, mode);
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        var startup = host.StartAsync(timeout.Token);
        try
        {
            await entered.Task.WaitAsync(timeout.Token);
            await Assert.That(receiver.Started).IsFalse();
            await Assert.That(startup.IsCompleted).IsFalse();
        }
        finally
        {
            release.TrySetResult();
            // Observe startup and stop the host even when a regression assertion fails.
            if (mode == TopologyMode.Validate)
                await Assert.That(async () => await startup).Throws<OptionsValidationException>();
            else
                await startup;
            await host.StopAsync(timeout.Token);
        }
        await Assert.That(receiver.Started).IsEqualTo(mode == TopologyMode.Provision);
        await Assert.That(adapter.Entities.Contains("orders")).IsEqualTo(mode == TopologyMode.Provision);
    }

    [Test]
    public async Task Concurrent_host_startup_rejects_invalid_configuration_before_topology_and_receivers()
    {
        var adapter = new InMemoryTransport("events");
        var receiver = new Receiver();
        using var host = CreateConcurrentHost(adapter, receiver, (TopologyMode)99);
        await Assert.That(async () => await host.StartAsync()).Throws<OptionsValidationException>();
        await Assert.That(adapter.Calls.Count).IsEqualTo(0);
        await Assert.That(receiver.Started).IsFalse();
    }

    [Test]
    public async Task Default_validate_reports_missing_entities_without_provisioning()
    {
        var adapter = new InMemoryTransport("events");
        await using var provider = Create(adapter);
        var failure = await Start(provider);
        await Assert.That(failure).IsNotNull();
        await Assert.That(failure!.Message).Contains("events");
        await Assert.That(failure.Message).Contains("Missing entity 'orders'");
        await Assert.That(adapter.Entities.Count).IsEqualTo(0);
        await Assert.That(adapter.Calls.Single()).IsEqualTo(TopologyMode.Validate);
    }

    [Test]
    public async Task Validate_existing_entities_is_read_only()
    {
        var adapter = new InMemoryTransport("events");
        adapter.Entities.Add("orders");
        await using var provider = Create(adapter);
        await Assert.That(await Start(provider)).IsNull();
        await Assert.That(adapter.Entities.Count).IsEqualTo(1);
        await Assert.That(adapter.Writes).IsEqualTo(0);
    }

    [Test]
    public async Task Provision_creates_missing_entities_and_is_idempotent()
    {
        var adapter = new InMemoryTransport("events");
        adapter.Entities.Add("unrelated");
        await using var provider = Create(adapter, TopologyMode.Provision);
        await Assert.That(await Start(provider)).IsNull();
        await Assert.That(await Start(provider)).IsNull();
        await Assert.That(adapter.Entities.Count).IsEqualTo(2);
        await Assert.That(adapter.Writes).IsEqualTo(1);
    }

    [Test]
    public async Task None_skips_topology_calls()
    {
        var adapter = new InMemoryTransport("events") { Error = new UnauthorizedAccessException() };
        await using var provider = Create(adapter, TopologyMode.None);
        await Assert.That(await Start(provider)).IsNull();
        await Assert.That(adapter.Calls.Count).IsEqualTo(0);
    }

    [Test]
    public async Task Transport_modes_override_global_default()
    {
        var first = new InMemoryTransport("events");
        var second = new InMemoryTransport("commands");
        await using var provider = Create(first, TopologyMode.None, builder =>
        {
            builder.AddTransport<BuilderTests.BrokerOptions, BuilderTests.RouteOptions, BuilderTests.ConsumerOptions>("commands", transport =>
            {
                transport.TopologyMode = TopologyMode.Provision;
                transport.Send<OrderCancelled>("orders");
            });
        }, second);
        await Assert.That(await Start(provider)).IsNull();
        await Assert.That(first.Calls.Count).IsEqualTo(0);
        await Assert.That(second.Entities.Contains("orders")).IsTrue();
    }

    [Test]
    public async Task Native_errors_include_transport_and_prevent_receiver_start()
    {
        var adapter = new InMemoryTransport("events") { Error = new UnauthorizedAccessException("read denied") };
        var receiver = new Receiver();
        await using var provider = Create(adapter, extra: builder => builder.Services.AddSingleton<IHostedService>(receiver));
        var failure = await Start(provider);
        await Assert.That(failure).IsNotNull();
        await Assert.That(failure!.Message).Contains("events");
        await Assert.That(failure.Message).Contains("read denied");
        await Assert.That(receiver.Started).IsFalse();
    }

    [Test]
    public async Task All_transport_differences_are_reported()
    {
        var first = new InMemoryTransport("events");
        var second = new InMemoryTransport("commands");
        await using var provider = Create(first, extra: builder =>
            builder.AddTransport<BuilderTests.BrokerOptions, BuilderTests.RouteOptions, BuilderTests.ConsumerOptions>("commands", transport =>
                transport.Send<OrderCancelled>("commands")), second: second);
        var failure = await Start(provider);
        await Assert.That(failure!.Failures.Count()).IsEqualTo(2);
        await Assert.That(second.Calls.Count).IsEqualTo(1);
    }

    [Test]
    public async Task Invalid_mode_fails_before_any_topology_operation()
    {
        var adapter = new InMemoryTransport("events");
        await using var provider = Create(adapter, extra: builder =>
            builder.AddTransport<BuilderTests.BrokerOptions, BuilderTests.RouteOptions, BuilderTests.ConsumerOptions>("invalid", transport =>
                transport.TopologyMode = (TopologyMode)99));
        await Assert.That((await Start(provider))!.Message).Contains("TopologyMode");
        await Assert.That(adapter.Calls.Count).IsEqualTo(0);
    }

    [Test]
    public async Task Cancellation_is_propagated_to_adapter()
    {
        using var cancellation = new CancellationTokenSource();
        var adapter = new InMemoryTransport("events") { Cancel = cancellation };
        await using var provider = Create(adapter);
        var cancelled = false;
        try { await Start(provider, cancellation.Token); }
        catch (OperationCanceledException error) { cancelled = error.CancellationToken == cancellation.Token; }
        await Assert.That(cancelled).IsTrue();
    }

    [Test]
    public async Task Model_and_export_work_without_adapters_and_preserve_declarations()
    {
        await using var provider = Create(null);
        var topology = provider.GetRequiredService<ITopology>();
        var declaration = topology.Transports.Single();
        await Assert.That(declaration.Routes.Single().Destination).IsEqualTo("orders");
        await Assert.That(declaration.Consumers.Single().Source).IsEqualTo("orders");
        using var stream = new MemoryStream();
        topology.ExportJson(stream);
        using var json = JsonDocument.Parse(stream.ToArray());
        var transport = json.RootElement.GetProperty("transports")[0];
        await Assert.That(transport.GetProperty("mode").GetString()).IsEqualTo("Validate");
        var route = transport.GetProperty("routes")[0];
        await Assert.That(route.GetProperty("message").GetProperty("name").GetString()).IsEqualTo("tests.order-placed");
        await Assert.That(route.GetProperty("message").GetProperty("version").GetInt32()).IsEqualTo(2);
        var consumerMessage = transport.GetProperty("consumers")[0].GetProperty("messages")[0];
        await Assert.That(consumerMessage.GetProperty("name").GetString()).IsEqualTo("tests.order-placed");
        await Assert.That(Encoding.UTF8.GetString(stream.ToArray()).Contains(nameof(OrderPlaced), StringComparison.Ordinal)).IsFalse();
        await Assert.That(stream.CanWrite).IsTrue();
    }

    [Test]
    public async Task Report_snapshots_native_diff()
    {
        var differences = new List<string> { "orders: expected 3 partitions, actual 1" };
        var report = new TopologyReport(differences);
        differences.Clear();
        await Assert.That(report.Differences.Count).IsEqualTo(1);
    }

    [Test]
    public async Task Consumer_only_entities_are_provisioned_before_receivers_start()
    {
        var adapter = new InMemoryTransport("events");
        var receiver = new Receiver();
        await using var provider = Create(adapter, TopologyMode.Provision, builder =>
        {
            builder.AddTransport<BuilderTests.BrokerOptions, BuilderTests.RouteOptions, BuilderTests.ConsumerOptions>("inbound", transport =>
                transport.Consume("inbound-only", consumer => consumer.Handler<BuilderTests.OrderHandler>()));
            builder.Services.AddSingleton<IHostedService>(receiver);
        }, new InMemoryTransport("inbound"));
        await Assert.That(await Start(provider)).IsNull();
        await Assert.That(receiver.Started).IsTrue();
        var inbound = provider.GetServices<ITransport>().Cast<InMemoryTransport>().Single(item => item.Name == "inbound");
        await Assert.That(inbound.Entities.Contains("inbound-only")).IsTrue();
    }

    private static IHost CreateConcurrentHost(InMemoryTransport adapter, Receiver receiver, TopologyMode mode)
    {
        var builder = Host.CreateApplicationBuilder();
        builder.Logging.ClearProviders();
        builder.Services.Configure<HostOptions>(options => options.ServicesStartConcurrently = true);
        builder.Services.AddSingleton<IHostedService>(receiver);
        builder.Services.AddSingleton<ITransport>(adapter);
        builder.Services.AddRelayer(relayer =>
        {
            relayer.AddMessageTypesFromThisAssembly();
            relayer.TopologyMode = mode;
            relayer.AddTransport<BuilderTests.BrokerOptions, BuilderTests.RouteOptions, BuilderTests.ConsumerOptions>("events",
                transport => transport.Send<OrderPlaced>("orders"));
        });
        var host = builder.Build();
        adapter.Declaration = host.Services.GetRequiredService<ITopology>().Transports.Single();
        return host;
    }

    private static ServiceProvider Create(InMemoryTransport? adapter, TopologyMode? mode = null,
        Action<IRelayerBuilder>? extra = null, InMemoryTransport? second = null)
    {
        var services = new ServiceCollection();
        if (adapter is not null) services.AddSingleton<ITransport>(adapter);
        if (second is not null) services.AddSingleton<ITransport>(second);
        services.AddRelayer(builder =>
        {
            builder.AddMessageTypesFromThisAssembly();
            if (mode.HasValue) builder.TopologyMode = mode.Value;
            builder.AddTransport<BuilderTests.BrokerOptions, BuilderTests.RouteOptions, BuilderTests.ConsumerOptions>("events", transport =>
            {
                transport.Send<OrderPlaced>("orders");
                transport.Consume("orders", consumer => consumer.Handler<BuilderTests.OrderHandler>());
            });
            extra?.Invoke(builder);
        });
        var provider = services.BuildServiceProvider();
        foreach (var transport in provider.GetServices<ITransport>().Cast<InMemoryTransport>())
            transport.Declaration = provider.GetRequiredService<ITopology>().Transports.SingleOrDefault(item => item.Name == transport.Name);
        return provider;
    }

    private static async Task<OptionsValidationException?> Start(IServiceProvider provider, CancellationToken cancellationToken = default)
    {
        try
        {
            foreach (var service in provider.GetServices<IHostedService>().OfType<IHostedLifecycleService>())
                await service.StartingAsync(cancellationToken);
            foreach (var service in provider.GetServices<IHostedService>()) await service.StartAsync(cancellationToken);
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

    private sealed class InMemoryTransport(string name) : ITransport
    {
        public string Name => name;
        public TransportDefinition? Declaration { get; set; }
        public HashSet<string> Entities { get; } = new(StringComparer.Ordinal);
        public List<TopologyMode> Calls { get; } = [];
        public int Writes { get; private set; }
        public Exception? Error { get; init; }
        public CancellationTokenSource? Cancel { get; init; }
        public TaskCompletionSource? TopologyEntered { get; init; }
        public Task? TopologyRelease { get; init; }
        public TransportCapabilities GetCapabilities(string endpoint) => new();
        public async ValueTask<TopologyReport> ApplyTopologyAsync(TopologyMode mode, CancellationToken cancellationToken)
        {
            Calls.Add(mode);
            TopologyEntered?.TrySetResult();
            if (TopologyRelease is not null) await TopologyRelease.WaitAsync(cancellationToken);
            await Task.Yield(); // Exercise genuinely asynchronous startup.
            Cancel?.Cancel();
            cancellationToken.ThrowIfCancellationRequested();
            if (Error is not null) throw Error;
            var endpoints = Declaration!.Routes.Select(route => route.Destination)
                .Concat(Declaration.Consumers.Select(consumer => consumer.Source)).Distinct(StringComparer.Ordinal);
            var differences = new List<string>();
            foreach (var endpoint in endpoints)
            {
                if (Entities.Contains(endpoint)) continue;
                if (mode == TopologyMode.Provision) { Entities.Add(endpoint); Writes++; }
                else differences.Add($"Missing entity '{endpoint}'");
            }
            return new TopologyReport(differences);
        }
        public ValueTask SendAsync(OutgoingMessage message, CancellationToken cancellationToken) => throw new NotSupportedException();
        public ValueTask SendBatchAsync(ReadOnlyMemory<OutgoingMessage> messages, CancellationToken cancellationToken) => throw new NotSupportedException();
        public Task StartAsync(CancellationToken cancellationToken) => Task.CompletedTask;
        public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
        public ValueTask DisposeAsync() => default;
    }
}
