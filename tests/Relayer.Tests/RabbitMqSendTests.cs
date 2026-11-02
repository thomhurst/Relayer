using System.Collections.Concurrent;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQ.Client.Exceptions;
using Relayer.RabbitMQ;
using Relayer.Transports;

namespace Relayer.Tests;

public class RabbitMqSendTests
{
    [Test]
    public async Task Confirmed_concurrent_sends_preserve_native_headers_configuration_and_channel_isolation()
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(3));
        var token = timeout.Token;
        await using var broker = await RabbitMqTopologyTests.Broker.StartAsync(token);
        await using var native = await broker.Factory().CreateConnectionAsync(token);
        await using var channel = await native.CreateChannelAsync(cancellationToken: token);
        await channel.ExchangeDeclareAsync("events", ExchangeType.Direct, true, false, cancellationToken: token);
        await channel.QueueDeclareAsync("orders", true, false, false, cancellationToken: token);
        await channel.QueueBindAsync("orders", "events", "placed", cancellationToken: token);
        await using var provider = Provider(broker.Factory(), ["events:placed", "orders"]);
        var transport = provider.GetRequiredService<ITransport>();
        await transport.StartAsync(token);
        var calls = 0;
        var options = new SendOptions().RabbitMQ(properties =>
        {
            Interlocked.Increment(ref calls);
            properties.Priority = 3;
            properties.Headers!["traceparent"] = "wrong";
            properties.Headers["native"] = "value";
        });
        const string creation = "00-0123456789abcdef0123456789abcdef-0123456789abcdef-01";
        var bytes = Encoding.UTF8.GetBytes("__id-123__");
        KeyValuePair<string, WireString>[] headers = [new("cloudEvents:id", new(bytes.AsMemory(2, 6))),
            new("cloudEvents:type", new("orders.placed;v=1")), new("content-type", new("application/json")),
            new("traceparent", new(creation))];
        await Task.WhenAll(Enumerable.Range(0, 100).Select(index => transport.SendAsync(
            new("events:placed", BitConverter.GetBytes(index), headers, options: options), token).AsTask()));
        await Assert.That(calls).IsEqualTo(100);
        var received = new HashSet<int>();
        for (var i = 0; i < 100; i++)
        {
            var message = (await channel.BasicGetAsync("orders", true, token))!;
            received.Add(BitConverter.ToInt32(message.Body.Span));
            await Assert.That(message.BasicProperties.Persistent).IsTrue();
            await Assert.That(message.BasicProperties.Priority).IsEqualTo((byte)3);
            await Assert.That(message.BasicProperties.ContentType).IsEqualTo("application/json");
            await Assert.That(Header(message, "cloudEvents:id")).IsEqualTo("id-123");
            await Assert.That(Header(message, "traceparent")).IsEqualTo(creation);
            await Assert.That(Header(message, "native")).IsEqualTo("value");
        }
        await Assert.That(received.Count).IsEqualTo(100);
        await transport.SendBatchAsync(new[] { new OutgoingMessage("orders", new byte[] { 1 }),
            new OutgoingMessage("events:placed", new byte[] { 2 }) }, token);
        for (var i = 0; i < 2; i++)
        {
            var message = (await channel.BasicGetAsync("orders", true, token))!;
            await Assert.That(message.BasicProperties.Priority).IsEqualTo((byte)0);
            await Assert.That(message.BasicProperties.ContentType).IsNull();
            await Assert.That(message.BasicProperties.Headers?.ContainsKey("native") == true).IsFalse();
        }
        await transport.StopAsync(token);
        await Assert.That(async () => await transport.SendAsync(new("orders", default), token)).Throws<InvalidOperationException>();
    }

    [Test]
    public async Task Unroutable_missing_exchange_cancellation_and_closed_channels_do_not_report_success()
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(3));
        var token = timeout.Token;
        await using var broker = await RabbitMqTopologyTests.Broker.StartAsync(token);
        await using var native = await broker.Factory().CreateConnectionAsync(token);
        await using var channel = await native.CreateChannelAsync(cancellationToken: token);
        await channel.QueueDeclareAsync("orders", true, false, false, cancellationToken: token);
        await channel.QueueDeclareAsync("full", true, false, false, new Dictionary<string, object?>
        {
            ["x-max-length"] = 1, ["x-overflow"] = "reject-publish",
        }, cancellationToken: token);
        await using var nackProvider = Provider(broker.Factory(), ["full"]);
        var nackTransport = nackProvider.GetRequiredService<ITransport>();
        await nackTransport.StartAsync(token);
        await nackTransport.SendAsync(new("full", new byte[] { 1 }), token);
        var nack = await Assert.That(async () => await nackTransport.SendAsync(new("full", new byte[] { 2 }), token)).Throws<PublishException>();
        await Assert.That(nack!.IsReturn).IsFalse();
        await using var provider = Provider(broker.Factory(), ["orders", "missing", "absent:key"], options => options.PublisherCount = 1);
        var transport = provider.GetRequiredService<ITransport>();
        await transport.StartAsync(token);
        var returned = await Assert.That(async () => await transport.SendAsync(new("missing", new byte[] { 1 }), token)).Throws<PublishException>();
        await Assert.That(returned!.IsReturn).IsTrue();
        await Assert.That(async () => await transport.SendAsync(new("absent:key", default), token)).Throws<Exception>();
        // A protocol exception closes its channel. A later operation gets a fresh native confirm channel.
        await transport.SendAsync(new("orders", new byte[] { 2 }), token);
        await Assert.That((await channel.BasicGetAsync("orders", true, token))!.Body.Span[0]).IsEqualTo((byte)2);
        using var cancelled = new CancellationTokenSource();
        cancelled.Cancel();
        await Assert.That(async () => await transport.SendAsync(new("orders", default), cancelled.Token)).Throws<OperationCanceledException>();
        await Assert.That((await channel.QueueDeclarePassiveAsync("orders", token)).MessageCount).IsEqualTo(0u);
        await Assert.That(async () => await transport.SendAsync(new("orders", default, deliverAt: DateTimeOffset.UtcNow.AddMinutes(1)), token)).Throws<NotSupportedException>();
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task Startup_verifies_native_delayed_exchange_or_keeps_outbox_fallback(bool plugin)
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(3));
        var token = timeout.Token;
        await using var broker = await RabbitMqTopologyTests.Broker.StartAsync(token, plugin);
        await using var native = await broker.Factory().CreateConnectionAsync(token);
        await using var channel = await native.CreateChannelAsync(cancellationToken: token);
        await channel.QueueDeclareAsync("orders", true, false, false, cancellationToken: token);
        var outbox = new ScheduledOutbox();
        var services = new ServiceCollection();
        Register(services, broker.Factory(), ["orders"], options =>
        {
            options.DelayedExchanges.Add("", new("orders.delayed")
            {
                Type = "x-delayed-message", Arguments = new Dictionary<string, object?> { ["x-delayed-type"] = "direct" },
            });
        }, outbox, TopologyMode.Provision);
        using var host = new HostBuilder().ConfigureServices(collection =>
        {
            foreach (var service in services) collection.Add(service);
        }).Build();
        await using var hostLifetime = (IAsyncDisposable)host;
        await host.StartAsync(token);
        var provider = host.Services;
        var transport = provider.GetRequiredService<ITransport>();
        await Assert.That(transport.GetCapabilities("orders").MaxDeliveryDelay is not null).IsEqualTo(plugin);
        await transport.StartAsync(token);
        if (plugin)
        {
            await channel.QueueBindAsync("orders", "orders.delayed", "orders", cancellationToken: token);
            var delivery = new TaskCompletionSource<BasicDeliverEventArgs>(TaskCreationOptions.RunContinuationsAsynchronously);
            var consumer = new AsyncEventingBasicConsumer(channel);
            long deliveredAt = 0;
            consumer.ReceivedAsync += (_, args) =>
            {
                deliveredAt = Stopwatch.GetTimestamp();
                delivery.TrySetResult(args);
                return Task.CompletedTask;
            };
            await channel.BasicConsumeAsync("orders", true, consumer, token);
            var started = Stopwatch.GetTimestamp();
            await transport.SendAsync(new("orders", new byte[] { 42 }, deliverAt: DateTimeOffset.UtcNow.AddMilliseconds(500)), token);
            await delivery.Task.WaitAsync(token);
            await Assert.That(Stopwatch.GetElapsedTime(started, deliveredAt).TotalMilliseconds >= 450).IsTrue();
        }
        else
        {
            using var scope = provider.CreateScope();
            var sender = scope.ServiceProvider.GetRequiredService<ISender>();
            var due = DateTimeOffset.UtcNow.AddMinutes(1);
            await sender.SendAsync(new Sample(42), options => options.DeliverAt = due, token);
            await Assert.That(outbox.DeliverAt).IsEqualTo(due);
            await Assert.That(outbox.Count).IsEqualTo(1);
            await Assert.That((await channel.QueueDeclarePassiveAsync("orders", token)).MessageCount).IsEqualTo(0u);
            await sender.SendNowAsync(new Sample(43), cancellationToken: token);
            var sent = (await channel.BasicGetAsync("orders", true, token))!;
            await Assert.That(Header(sent, "cloudEvents:type")).IsEqualTo("rabbitmq.send.sample;v=1");
            await Assert.That(sent.BasicProperties.ContentType).IsEqualTo("application/json");
        }
    }

    [Test]
    [Arguments(TopologyMode.None)]
    [Arguments(TopologyMode.Validate)]
    public async Task Read_only_startup_does_not_probe_or_advertise_unverified_plugin(TopologyMode mode)
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(3));
        await using var broker = await RabbitMqTopologyTests.Broker.StartAsync(timeout.Token);
        await using var provider = Provider(broker.Factory(), ["orders"], options => options.DelayedExchanges.Add("", new("absent") { Type = "x-delayed-message" }));
        var transport = provider.GetRequiredService<ITransport>();
        await ((ITransportCapabilityInitializer)transport).InitializeCapabilitiesAsync(mode, timeout.Token);
        await Assert.That(transport.GetCapabilities("orders").MaxDeliveryDelay).IsNull();
        await using var connection = await broker.Factory().CreateConnectionAsync(timeout.Token);
        await using var channel = await connection.CreateChannelAsync(cancellationToken: timeout.Token);
        var error = await Assert.That(async () => await channel.ExchangeDeclarePassiveAsync("absent", timeout.Token)).Throws<OperationInterruptedException>();
        await Assert.That(error!.ShutdownReason!.ReplyCode).IsEqualTo((ushort)404);
    }

    [Test]
    public async Task Telemetry_uses_rabbitmq_destination_routing_tags_and_preserves_creation_context()
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(3));
        var token = timeout.Token;
        await using var broker = await RabbitMqTopologyTests.Broker.StartAsync(token);
        await using var native = await broker.Factory().CreateConnectionAsync(token);
        await using var channel = await native.CreateChannelAsync(cancellationToken: token);
        await channel.QueueDeclareAsync("orders", true, false, false, cancellationToken: token);
        await using var provider = Provider(broker.Factory(), ["orders", "missing"]);
        var tracing = provider.GetRequiredService<MessageTracing>();
        var spans = new ConcurrentQueue<Activity>();
        var instrumentation = provider.GetRequiredService<RelayerInstrumentation>();
        var durations = new ConcurrentQueue<(double Seconds, bool Failed)>();
        using var meter = new MeterListener
        {
            InstrumentPublished = (instrument, listener) =>
            {
                if (ReferenceEquals(instrument.Meter.Scope, instrumentation) && instrument.Name == "messaging.client.operation.duration")
                    listener.EnableMeasurementEvents(instrument);
            },
        };
        meter.SetMeasurementEventCallback<double>((_, value, tags, _) =>
        {
            var failed = false;
            foreach (var tag in tags) if (tag.Key == "error.type" && Equals(tag.Value, "_OTHER")) failed = true;
            durations.Enqueue((value, failed));
        });
        meter.Start();
        string? sampledRoutingKey = null;
        using var listener = new ActivityListener
        {
            ShouldListenTo = source => ReferenceEquals(source, tracing.Source) || source.Name == RabbitMQActivitySource.PublisherSourceName,
            Sample = (ref ActivityCreationOptions<ActivityContext> options) =>
            {
                if (options.Source == tracing.Source)
                    sampledRoutingKey = options.Tags?.FirstOrDefault(tag => tag.Key == "messaging.rabbitmq.destination.routing_key").Value as string;
                return ActivitySamplingResult.AllDataAndRecorded;
            },
            ActivityStopped = spans.Enqueue,
        };
        ActivitySource.AddActivityListener(listener);
        var transport = provider.GetRequiredService<ITransport>();
        await transport.StartAsync(token);
        const string original = "00-0123456789abcdef0123456789abcdef-0123456789abcdef-01";
        await transport.SendAsync(new("orders", new byte[] { 1 }, new KeyValuePair<string, WireString>[] { new("traceparent", new(original)) }), token);
        var sent = (await channel.BasicGetAsync("orders", true, token))!;
        await Assert.That(Header(sent, "traceparent")).IsEqualTo(original);
        var span = spans.Single(item => ReferenceEquals(item.Source, tracing.Source));
        await Assert.That(span.DisplayName).IsEqualTo("send :orders");
        await Assert.That(span.Kind).IsEqualTo(ActivityKind.Client);
        await Assert.That(span.GetTagItem("messaging.system")).IsEqualTo("rabbitmq");
        await Assert.That(sampledRoutingKey).IsEqualTo("orders");
        await Assert.That(span.Links.Single().Context.TraceId.ToString()).IsEqualTo("0123456789abcdef0123456789abcdef");
        await transport.SendAsync(new("orders", new byte[] { 2 }), token);
        await Assert.That(spans.Count(item => ReferenceEquals(item.Source, tracing.Source))).IsEqualTo(2);
        await transport.SendBatchAsync(new[] { new OutgoingMessage("orders", new byte[] { 4 }), new OutgoingMessage("orders", new byte[] { 5 }) }, token);
        await Assert.That(spans.Count(item => ReferenceEquals(item.Source, tracing.Source))).IsEqualTo(4);
        await Assert.That(async () => await transport.SendAsync(new("missing", new byte[] { 3 }), token)).Throws<PublishException>();
        await Assert.That(durations.Count).IsEqualTo(5);
        await Assert.That(durations.Count(duration => duration.Failed)).IsEqualTo(1);
        await Assert.That(durations.All(duration => duration.Seconds > 0)).IsTrue();
        var failedSpan = spans.Last(item => ReferenceEquals(item.Source, tracing.Source));
        await Assert.That(failedSpan.Status).IsEqualTo(ActivityStatusCode.Error);
        await Assert.That(failedSpan.GetTagItem("error.type")).IsEqualTo("_OTHER");
    }

    [Test]
    public async Task Shutdown_stops_admission_and_drains_a_borrowed_publisher_before_closing()
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(3));
        var token = timeout.Token;
        await using var broker = await RabbitMqTopologyTests.Broker.StartAsync(token);
        await using var connection = await broker.Factory().CreateConnectionAsync(token);
        await using var channel = await connection.CreateChannelAsync(cancellationToken: token);
        await channel.QueueDeclareAsync("orders", true, false, false, cancellationToken: token);
        await using var provider = Provider(broker.Factory(), ["orders"], options => options.PublisherCount = 1);
        var transport = provider.GetRequiredService<ITransport>();
        await transport.StartAsync(token);
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var release = new ManualResetEventSlim();
        var send = Task.Run(async () => await transport.SendAsync(new("orders", new byte[] { 1 }, options:
            new SendOptions().RabbitMQ(_ => { entered.SetResult(); release.Wait(token); })), token), token);
        await entered.Task.WaitAsync(token);
        using (var waitingToken = new CancellationTokenSource())
        {
            var waiting = transport.SendAsync(new("orders", new byte[] { 2 }), waitingToken.Token);
            await Assert.That(waiting.IsCompleted).IsFalse();
            waitingToken.Cancel();
            await Assert.That(async () => await waiting).Throws<OperationCanceledException>();
        }
        var stop = transport.StopAsync(token);
        try
        {
            await Assert.That(stop.IsCompleted).IsFalse();
            await Assert.That(async () => await transport.SendAsync(new("orders", default), token)).Throws<InvalidOperationException>();
        }
        finally { release.Set(); }
        await send;
        await stop;
        await Assert.That((await channel.QueueDeclarePassiveAsync("orders", token)).MessageCount).IsEqualTo(1u);
    }

    private static string Header(BasicGetResult message, string name) => Encoding.UTF8.GetString((byte[])message.BasicProperties.Headers![name]!);

    private static ServiceProvider Provider(ConnectionFactory factory, string[] destinations,
        Action<RabbitMqTransportOptions>? configure = null, ScheduledOutbox? outbox = null)
    {
        var services = new ServiceCollection();
        Register(services, factory, destinations, configure, outbox);
        return services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true });
    }

    private static void Register(IServiceCollection services, ConnectionFactory factory, string[] destinations,
        Action<RabbitMqTransportOptions>? configure, ScheduledOutbox? outbox, TopologyMode mode = TopologyMode.None)
    {
        if (outbox is not null) services.AddScoped<IOutboxEnlistment>(_ => outbox);
        services.AddRelayer(builder =>
        {
            builder.AddMessageTypesFromThisAssembly();
            builder.RabbitMQ("rabbit", rabbit =>
            {
                rabbit.TopologyMode = mode;
                rabbit.Options.Configure(options => { options.ConnectionFactory = factory; configure?.Invoke(options); });
                // Distinct message types keep the ordinary send API's one-route-per-type contract.
                rabbit.Send<Sample>(destinations[0]);
                if (destinations.Length > 1) rabbit.Send<Other>(destinations[1]);
                if (destinations.Length > 2) rabbit.Send<Third>(destinations[2]);
            });
        });
    }

    [Message("rabbitmq.send.sample")]
    public sealed record Sample(int Value);
    [Message("rabbitmq.send.other")]
    public sealed record Other;
    [Message("rabbitmq.send.third")]
    public sealed record Third;

    private sealed class ScheduledOutbox : IOutboxEnlistment
    {
        public bool IsActive => true;
        public bool SupportsScheduling => true;
        public DateTimeOffset? DeliverAt;
        public int Count;
        public ValueTask EnlistAsync(OutboxRecord record, CancellationToken cancellationToken)
        {
            DeliverAt = record.Message.DeliverAt;
            Count++;
            return ValueTask.CompletedTask;
        }
    }
}
