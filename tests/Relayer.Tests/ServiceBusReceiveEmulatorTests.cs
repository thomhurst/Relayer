using Relayer.Transports;
using Azure.Messaging.ServiceBus.Administration;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Text;
using System.Text.Json.Serialization;
using System.Text.Json.Nodes;
using System.Threading.Channels;
using Azure.Messaging.ServiceBus;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using DotNet.Testcontainers.Networks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging;
using Relayer.AzureServiceBus;
using Relayer.HealthChecks;
using Testcontainers.MsSql;
using Testcontainers.ServiceBus;

namespace Relayer.Tests;

public sealed class ServiceBusReceiveEmulatorTests
{
    [Test]
    public async Task Native_processors_receive_queues_subscriptions_sessions_drain_and_recover()
    {
        // Provisioning owns its deadline; broker assertions get their full operation budget.
        await using var broker = await ServiceBusReceiveBroker.StartAsync(["receive-orders"], ["receive-sessions"], includeTopic: true, dedicated: true);
        using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(6));
        var token = timeout.Token;
        await using var client = new ServiceBusClient(broker.ConnectionString, new ServiceBusClientOptions
        {
            RetryOptions = new() { MaxRetries = 0 },
        });

        var services = new ServiceCollection();
        var control = new EmulatorControl();
        services.AddSingleton(control);
        services.AddLogging(logging => logging.AddSimpleConsole());
        services.AddRelayerServiceBusHealthProviders("bus");
        services.AddRelayer(builder =>
        {
            builder.Infrastructure().MessageTypes.Add<EmulatorPayload>("servicebus.emulator-receive");
            builder.UseJsonSerializer(ServiceBusReceiveEmulatorJson.Default);
            builder.ServiceBus("bus", bus =>
            {
                bus.TopologyMode = TopologyMode.None;
                bus.Options.Configure(options =>
                {
                    options.Client = client;
                    options.Queues.Add(new("receive-orders"));
                    options.Queues.Add(new("receive-sessions") { RequiresSession = true });
                    options.Topics.Add(new("receive-events"));
                    options.Subscriptions.Add(new("receive-events", "audit"));
                });
                bus.Consume("receive-orders", consumer => consumer.Handler<EmulatorHandler>());
                bus.Consume("receive-events", consumer =>
                {
                    consumer.Options.Subscription = "audit";
                    consumer.Handler<EmulatorHandler>();
                });
                bus.Consume("receive-sessions", consumer =>
                {
                    consumer.Options.SessionProcessorOptions = new() { AutoCompleteMessages = false, MaxConcurrentSessions = 1 };
                    consumer.Handler<EmulatorHandler>();
                });
            });
        });
        await using var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true });
        var spans = new ConcurrentQueue<Activity>();
        var tracing = provider.GetRequiredService<MessageTracing>();
        using var listener = new ActivityListener
        {
            ShouldListenTo = source => ReferenceEquals(source, tracing.Source) || source.Name.StartsWith("Azure.Messaging.ServiceBus", StringComparison.Ordinal),
            Sample = static (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
            ActivityStopped = spans.Enqueue,
        };
        ActivitySource.AddActivityListener(listener);
        await ServiceBusReceiveTests.Start(provider, token);
        var probe = provider.GetRequiredKeyedService<ITransportHealthProbe>("bus");
        var liveness = provider.GetRequiredKeyedService<IConsumerHealthProvider>("bus");
        var declarations = provider.GetRequiredService<RelayerConfiguration>().Transports.Single().Consumers;
        foreach (var declaration in declarations)
            await Assert.That((await liveness.GetStateAsync(declaration, token))!.Value.IsIdle).IsTrue();

        await using var queue = client.CreateSender("receive-orders");
        await using var topic = client.CreateSender("receive-events");
        await using var sessions = client.CreateSender("receive-sessions");
        await queue.SendMessageAsync(Message(1), token);
        await topic.SendMessageAsync(Message(10), token);
        await sessions.SendMessagesAsync([Message(20, "customer"), Message(21, "customer")], token);
        var delivered = new List<int>();
        for (var i = 0; i < 4; i++) delivered.Add(await control.Received.Reader.ReadAsync(token));
        await Assert.That(delivered.Order().ToArray()).IsEquivalentTo(new[] { 1, 10, 20, 21 });
        await Assert.That(control.SessionValues.ToArray()).IsEquivalentTo(new[] { 20, 21 });
        // Real deliveries establish AMQP readiness before the native connectivity assertion.
        await Assert.That((await probe.CheckAsync(token)).Status).IsEqualTo(HealthStatus.Healthy);

        // An active worker holds graceful shutdown until its work and settlement complete.
        control.Release = new(TaskCreationOptions.RunContinuationsAsynchronously);
        await queue.SendMessageAsync(Message(30), token);
        await control.Blocked.Task.WaitAsync(token);
        var active = await liveness.GetStateAsync(declarations.Single(declaration => declaration.Source == "receive-orders"), token);
        await Assert.That(active!.Value.IsIdle).IsFalse();
        await Assert.That(active.Value.LastProgressAt <= DateTimeOffset.UtcNow).IsTrue();
        var drain = ServiceBusReceiveTests.Stop(provider, token);
        await Assert.That(drain.IsCompleted).IsFalse();
        control.Release.SetResult();
        await drain;
        await using (var receiver = client.CreateReceiver("receive-orders"))
        {
            var remaining = await receiver.PeekMessageAsync(cancellationToken: token);
            if (remaining is not null) Console.WriteLine($"Remaining after drain: {remaining.Body}, state={remaining.State}, delivery={remaining.DeliveryCount}");
            await Assert.That(remaining).IsNull();
        }

        await ServiceBusReceiveTests.Start(provider, token);
        // Restart only this fixture's emulator, preserving its advertised client address.
        var connectionString = broker.ConnectionString;
        var administrationConnectionString = broker.AdministrationConnectionString;
        await broker.DisconnectAsync(token);
        await Assert.That((await probe.CheckAsync(token)).Status).IsEqualTo(HealthStatus.Unhealthy);
        await broker.ReconnectAsync(token);
        await Assert.That(broker.ConnectionString).IsEqualTo(connectionString);
        await Assert.That(broker.AdministrationConnectionString).IsEqualTo(administrationConnectionString);
        var administration = new Azure.Messaging.ServiceBus.Administration.ServiceBusAdministrationClient(administrationConnectionString);
        await Assert.That((await administration.QueueExistsAsync("receive-orders", token)).Value).IsTrue();
        await queue.SendMessageAsync(Message(40), token);
        while (await control.Received.Reader.ReadAsync(token) != 40) { }
        await Assert.That((await probe.CheckAsync(token)).Status).IsEqualTo(HealthStatus.Healthy);
        await ServiceBusReceiveTests.Stop(provider, token);
        var captured = spans.ToArray();
        Console.WriteLine("Captured native scopes: " + string.Join("; ", captured.Select(span => $"{span.Source.Name}:{span.DisplayName} id={span.SpanId} parent={span.ParentSpanId}")));
        var settlements = captured.Where(span => ReferenceEquals(span.Source, tracing.Source) &&
            Equals(span.GetTagItem("messaging.operation.type"), "settle")).ToArray();
        await Assert.That(captured.Any(span => span.Source.Name.StartsWith("Azure.Messaging.ServiceBus", StringComparison.Ordinal) &&
            settlements.Any(settlement => settlement.SpanId == span.ParentSpanId && settlement.TraceId == span.TraceId))).IsTrue();
        await Assert.That(captured.Any(span => ReferenceEquals(span.Source, tracing.Source) &&
            Equals(span.GetTagItem("messaging.destination.subscription.name"), "audit"))).IsTrue();
    }

    private static ServiceBusMessage Message(int value, string? session = null)
    {
        var message = new ServiceBusMessage(new BinaryData($"{{\"Value\":{value}}}")) { SessionId = session, ContentType = "application/json" };
        message.ApplicationProperties["cloudEvents:id"] = Guid.NewGuid().ToString("N");
        message.ApplicationProperties["cloudEvents:type"] = "servicebus.emulator-receive;v=1";
        message.ApplicationProperties["traceparent"] = "00-0123456789abcdef0123456789abcdef-0123456789abcdef-01";
        return message;
    }

    [Message("servicebus.emulator-receive")]
    public sealed record EmulatorPayload(int Value);

    public sealed class EmulatorControl
    {
        internal Channel<int> Received { get; } = Channel.CreateUnbounded<int>();
        internal ConcurrentQueue<int> SessionValues { get; } = new();
        internal TaskCompletionSource Blocked { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        internal TaskCompletionSource? Release;
    }

    public sealed class EmulatorHandler(EmulatorControl control) : IHandler<EmulatorPayload>
    {
        public async ValueTask HandleAsync(EmulatorPayload message, MessageContext context, CancellationToken cancellationToken)
        {
            var native = context.ServiceBus();
            if (message.Value is 20 or 21)
            {
                if (native.Message.SessionId != "customer") throw new InvalidOperationException("Session ID changed.");
                control.SessionValues.Enqueue(message.Value);
                await native.CompleteAsync();
            }
            if (message.Value == 30)
            {
                control.Blocked.SetResult();
                await control.Release!.Task.WaitAsync(cancellationToken);
            }
            if (!Equals(native.Message.ApplicationProperties["traceparent"], "00-0123456789abcdef0123456789abcdef-0123456789abcdef-01"))
                throw new InvalidOperationException("Native instrumentation changed creation context.");
            control.Received.Writer.TryWrite(message.Value);
        }
    }
}

// Entities for one test on the session's shared emulator. A test that restarts the emulator
// asks for a dedicated one, reached through a proxy that keeps its public ports stable.
internal sealed class ServiceBusReceiveBroker : IAsyncDisposable
{
    private readonly INetwork? _network;
    private readonly MsSqlContainer? _database;
    private readonly ServiceBusContainer _emulator;
    private readonly IContainer? _proxy;
    private readonly string? _configurationPath;
    private readonly List<string> _queues = [];
    private bool _topic;
    internal string ConnectionString => ThroughProxy(_emulator.GetConnectionString(), ServiceBusBuilder.ServiceBusPort);
    internal string AdministrationConnectionString => ThroughProxy(_emulator.GetHttpConnectionString(), ServiceBusBuilder.ServiceBusHttpPort);

    private ServiceBusReceiveBroker(ServiceBusContainer emulator) => _emulator = emulator;

    private ServiceBusReceiveBroker(INetwork network, MsSqlContainer database, ServiceBusContainer emulator, IContainer proxy, string configurationPath)
    { _network = network; _database = database; _emulator = emulator; _proxy = proxy; _configurationPath = configurationPath; }

    private string ThroughProxy(string connectionString, int port) => _proxy is null ? connectionString : connectionString.Replace(
        new UriBuilder("sb", _emulator.Hostname, _emulator.GetMappedPublicPort(port)).ToString(),
        new UriBuilder("sb", _proxy.Hostname, _proxy.GetMappedPublicPort(port)).ToString(), StringComparison.Ordinal);

    internal static async Task<ServiceBusReceiveBroker> StartAsync(string[]? queues = null,
        string[]? sessions = null, bool includeTopic = false, bool dedicated = false)
    {
        if (!dedicated)
        {
            using var creation = new CancellationTokenSource(TimeSpan.FromMinutes(16));
            var stack = await SharedBrokers.ServiceBus.Value.WaitAsync(creation.Token);
            var shared = new ServiceBusReceiveBroker(stack.Emulator);
            try
            {
                foreach (var name in queues ?? []) { await stack.CreateQueueAsync(name, false, creation.Token); shared._queues.Add(name); }
                foreach (var name in sessions ?? []) { await stack.CreateQueueAsync(name, true, creation.Token); shared._queues.Add(name); }
                if (includeTopic)
                {
                    var admin = new ServiceBusAdministrationClient(shared.AdministrationConnectionString);
                    await admin.CreateTopicAsync(new CreateTopicOptions("receive-events")
                    {
                        DefaultMessageTimeToLive = TimeSpan.FromHours(1), MaxSizeInMegabytes = 100,
                        DuplicateDetectionHistoryTimeWindow = TimeSpan.FromMinutes(10)
                    }, creation.Token);
                    shared._topic = true;
                    await admin.CreateSubscriptionAsync(new CreateSubscriptionOptions("receive-events", "audit")
                    {
                        DefaultMessageTimeToLive = TimeSpan.FromHours(1), LockDuration = TimeSpan.FromMinutes(1), MaxDeliveryCount = 10
                    }, creation.Token);
                }
                return shared;
            }
            catch { await shared.DisposeAsync(); throw; }
        }
        using var provisioning = new CancellationTokenSource(TimeSpan.FromMinutes(15));
        var token = provisioning.Token;
        var network = new NetworkBuilder().Build();
        var databaseBuilder = new MsSqlBuilder("mcr.microsoft.com/mssql/server:2022-CU14-ubuntu-22.04")
            .WithNetwork(network).WithNetworkAliases(ServiceBusBuilder.DatabaseNetworkAlias);
        var configurationPath = Path.Combine(AppContext.BaseDirectory, "servicebus-receive-" + Guid.NewGuid().ToString("N") + ".json");
        var configuration = JsonNode.Parse(await File.ReadAllTextAsync(Path.Combine(AppContext.BaseDirectory, "servicebus-config.json"), token))!;
        var nativeNamespace = configuration["UserConfig"]!["Namespaces"]![0]!;
        var nativeQueues = nativeNamespace["Queues"]!.AsArray();
        var template = nativeQueues[0]!.DeepClone();
        foreach (var name in queues ?? []) AddQueue(name, false);
        foreach (var name in sessions ?? []) AddQueue(name, true);
        if (includeTopic)
        {
            nativeNamespace["Topics"]!.AsArray().Add(JsonNode.Parse("""
                { "Name": "receive-events", "Properties": {
                    "DefaultMessageTimeToLive": "PT1H", "DuplicateDetectionHistoryTimeWindow": "PT20S",
                    "RequiresDuplicateDetection": false },
                  "Subscriptions": [{ "Name": "audit", "Properties": {
                    "DefaultMessageTimeToLive": "PT1H", "DeadLetteringOnMessageExpiration": false,
                    "LockDuration": "PT1M", "MaxDeliveryCount": 10, "RequiresSession": false } }] }
                """));
        }
        await File.WriteAllTextAsync(configurationPath, configuration.ToJsonString(), token);
        var emulatorBuilder = new ServiceBusBuilder("mcr.microsoft.com/azure-messaging/servicebus-emulator:2.0.0")
            .WithAcceptLicenseAgreement(true).WithConfig(configurationPath).WithNetworkAliases("servicebus");
        // Keep Docker's public bindings alive while the upstream emulator stops and starts.
        // Use the same pinned Toxiproxy image as the Pub/Sub reconnect fixture.
        var proxyBuilder = new ContainerBuilder("ghcr.io/shopify/toxiproxy@sha256:5d3dae09bc185dcc5ff6cf43abf6062a4a720aff0c4306a471e773acdcd107aa")
            .WithNetwork(network).WithPortBinding(5672, true).WithPortBinding(5300, true)
            .WithResourceMapping(Encoding.UTF8.GetBytes("""
                [
                  { "name": "amqp", "listen": "0.0.0.0:5672", "upstream": "servicebus:5672", "enabled": true },
                  { "name": "management", "listen": "0.0.0.0:5300", "upstream": "servicebus:5300", "enabled": true }
                ]
                """), "/proxies.json")
            .WithCommand("-config", "/proxies.json")
            .WithWaitStrategy(Wait.ForUnixContainer().UntilMessageIsLogged("Populated proxies from file"));
        if (OperatingSystem.IsLinux())
        {
            databaseBuilder = databaseBuilder.WithPortBinding("20000-29999", "1433");
            emulatorBuilder = emulatorBuilder.WithPortBinding("20000-29999", "5672").WithPortBinding("20000-29999", "5300");
            proxyBuilder = proxyBuilder.WithPortBinding("20000-29999", "5672").WithPortBinding("20000-29999", "5300");
        }
        var database = databaseBuilder.Build();
        var emulator = emulatorBuilder.WithMsSqlContainer(network, database, ServiceBusBuilder.DatabaseNetworkAlias).Build();
        var proxy = proxyBuilder.Build();
        var broker = new ServiceBusReceiveBroker(network, database, emulator, proxy, configurationPath);
        try
        {
            await ServiceBusStartupDiagnostics.StartAsync(async ct =>
            {
                await emulator.StartAsync(ct);
                await proxy.StartAsync(ct);
            }, database, emulator, token);
            return broker;
        }
        catch (OperationCanceledException error) when (provisioning.IsCancellationRequested)
        {
            await CleanupFailedStartupAsync();
            throw new TimeoutException("Service Bus emulator provisioning exceeded 15 minutes (image pulls and container readiness).", error);
        }
        catch { await CleanupFailedStartupAsync(); throw; }

        async Task CleanupFailedStartupAsync()
        {
            try { await broker.DisposeAsync(); }
            catch (Exception cleanupError)
            {
                try { Console.Error.WriteLine($"Service Bus startup cleanup failed: {cleanupError}"); }
                catch (Exception) { /* Preserve the startup failure even if output is unavailable. */ }
            }
        }

        void AddQueue(string name, bool session)
        {
            var queue = template.DeepClone();
            queue["Name"] = name;
            queue["Properties"]!["RequiresSession"] = session;
            nativeQueues.Add(queue);
        }
    }

    internal Task DisconnectAsync(CancellationToken token) => Dedicated().StopAsync(token);
    internal Task ReconnectAsync(CancellationToken token) => Dedicated().StartAsync(token);
    private ServiceBusContainer Dedicated() => _proxy is not null ? _emulator
        : throw new InvalidOperationException("Stopping the emulator needs StartAsync(dedicated: true); other tests share this one.");

    public async ValueTask DisposeAsync()
    {
        if (_proxy is null)
        {
            // Throwing here would replace an exception unwinding through await using; the session removes the emulator.
            try
            {
                var admin = new ServiceBusAdministrationClient(AdministrationConnectionString);
                foreach (var queue in _queues) await admin.DeleteQueueAsync(queue);
                if (_topic) await admin.DeleteTopicAsync("receive-events");
            }
            catch (Exception exception) { Console.Error.WriteLine($"Could not delete Service Bus test entities: {exception}"); }
            return;
        }
        try { await _proxy.DisposeAsync(); }
        finally { await DisposeBrokerAsync(); }
    }

    private async ValueTask DisposeBrokerAsync()
    {
        try { await _emulator.DisposeAsync(); }
        finally
        {
            try { await _database!.DisposeAsync(); }
            finally { try { await _network!.DisposeAsync(); } finally { File.Delete(_configurationPath!); } }
        }
    }
}

[JsonSerializable(typeof(ServiceBusReceiveEmulatorTests.EmulatorPayload))]
internal partial class ServiceBusReceiveEmulatorJson : JsonSerializerContext;
