using System.Collections.Concurrent;
using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Text.Json.Serialization;
using System.Text.Json.Nodes;
using System.Threading.Channels;
using Azure.Messaging.ServiceBus;
using DotNet.Testcontainers.Builders;
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
        await using var broker = await ServiceBusReceiveBroker.StartAsync(["receive-orders"], ["receive-sessions"], includeTopic: true);
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
            builder.MessageTypes.Add<EmulatorPayload>("servicebus.emulator-receive");
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
        await broker.DisconnectAsync(token);
        await Assert.That((await probe.CheckAsync(token)).Status).IsEqualTo(HealthStatus.Unhealthy);
        await broker.ReconnectAsync(token);
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

internal sealed class ServiceBusReceiveBroker : IAsyncDisposable
{
    private readonly INetwork _network;
    private readonly MsSqlContainer _database;
    private readonly ServiceBusContainer _emulator;
    private readonly string _configurationPath;
    internal string ConnectionString => _emulator.GetConnectionString();
    internal string AdministrationConnectionString => _emulator.GetHttpConnectionString();

    private ServiceBusReceiveBroker(INetwork network, MsSqlContainer database, ServiceBusContainer emulator, string configurationPath)
    { _network = network; _database = database; _emulator = emulator; _configurationPath = configurationPath; }

    internal static async Task<ServiceBusReceiveBroker> StartAsync(string[]? queues = null,
        string[]? sessions = null, bool includeTopic = false)
    {
        using var provisioning = new CancellationTokenSource(TimeSpan.FromMinutes(15));
        var token = provisioning.Token;
        var network = new NetworkBuilder().Build();
        var databaseBuilder = new MsSqlBuilder("mcr.microsoft.com/mssql/server:2022-CU14-ubuntu-22.04")
            .WithNetwork(network).WithNetworkAliases(ServiceBusBuilder.DatabaseNetworkAlias);
        var amqp = ReservePort();
        var management = ReservePort();
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
            .WithAcceptLicenseAgreement(true).WithConfig(configurationPath)
            .WithPortBinding(amqp, 5672).WithPortBinding(management, 5300);
        if (OperatingSystem.IsLinux()) databaseBuilder = databaseBuilder.WithPortBinding("20000-29999", "1433");
        var database = databaseBuilder.Build();
        var emulator = emulatorBuilder.WithMsSqlContainer(network, database, ServiceBusBuilder.DatabaseNetworkAlias).Build();
        var broker = new ServiceBusReceiveBroker(network, database, emulator, configurationPath);
        try { await emulator.StartAsync(token); return broker; }
        catch (OperationCanceledException error) when (provisioning.IsCancellationRequested)
        {
            await broker.DisposeAsync();
            throw new TimeoutException("Service Bus emulator provisioning exceeded 15 minutes (image pulls and container readiness).", error);
        }
        catch { await broker.DisposeAsync(); throw; }

        void AddQueue(string name, bool session)
        {
            var queue = template.DeepClone();
            queue["Name"] = name;
            queue["Properties"]!["RequiresSession"] = session;
            nativeQueues.Add(queue);
        }
    }

    private static int ReservePort()
    {
        var start = OperatingSystem.IsLinux() ? Random.Shared.Next(20000, 30000) : 0;
        for (var attempt = 0; attempt < 10000; attempt++)
        {
            var listener = new TcpListener(IPAddress.Loopback, start == 0 ? 0 : 20000 + (start - 20000 + attempt) % 10000);
            try { listener.Start(); return ((IPEndPoint)listener.LocalEndpoint).Port; }
            catch (SocketException error) when (OperatingSystem.IsLinux() && error.SocketErrorCode == SocketError.AddressAlreadyInUse) { }
            finally { listener.Stop(); }
        }
        throw new InvalidOperationException("No available Service Bus emulator test port in 20000-29999.");
    }

    internal Task DisconnectAsync(CancellationToken token) => _emulator.StopAsync(token);
    internal Task ReconnectAsync(CancellationToken token) => _emulator.StartAsync(token);
    public async ValueTask DisposeAsync()
    {
        try { await _emulator.DisposeAsync(); }
        finally
        {
            try { await _database.DisposeAsync(); }
            finally { try { await _network.DisposeAsync(); } finally { File.Delete(_configurationPath); } }
        }
    }
}

[JsonSerializable(typeof(ServiceBusReceiveEmulatorTests.EmulatorPayload))]
internal partial class ServiceBusReceiveEmulatorJson : JsonSerializerContext;
