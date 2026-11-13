using System.Diagnostics;
using System.Text.Json.Serialization;
using System.Threading.Channels;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Hosting;
using RabbitMQ.Client;
using RabbitMQ.Client.Exceptions;
using Relayer.HealthChecks;
using Relayer.RabbitMQ;
using Relayer.Transports;

namespace Relayer.Tests;

public class RabbitMqReceiveTests
{
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task Default_failure_policy_retries_before_native_ack_or_dead_letter_copy(bool recover)
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(2));
        var token = timeout.Token;
        await using var broker = await RabbitMqTopologyTests.Broker.StartAsync(token);
        var attempts = 0;
        var control = new RabbitMqReceiveControl
        {
            Handle = _ => Interlocked.Increment(ref attempts) == 4 && recover
                ? default : ValueTask.FromException(new TimeoutException("dependency unavailable"))
        };
        using var host = Host(broker.Factory(), control);
        await host.StartAsync(token);
        await host.Services.GetRequiredService<ITransport>().SendAsync(
            new("orders", "{\"Value\":42}"u8.ToArray(), Headers()), token);
        for (var i = 0; i < 4; i++)
            await Assert.That(await control.Values.Reader.ReadAsync(token)).IsEqualTo(42);
        await host.StopAsync(token);
        await Assert.That(attempts).IsEqualTo(4);
        await using var connection = await broker.Factory().CreateConnectionAsync(token);
        await using var channel = await connection.CreateChannelAsync(cancellationToken: token);
        await Assert.That((await channel.QueueDeclarePassiveAsync("orders", token)).MessageCount).IsEqualTo(0u);
        var dead = await channel.BasicGetAsync("orders.dead-letter", true, token);
        if (recover) await Assert.That(dead).IsNull();
        else
        {
            await Assert.That(dead).IsNotNull();
            var count = (byte[])dead!.BasicProperties.Headers![FailureHeaders.Attempts]!;
            await Assert.That(System.Text.Encoding.UTF8.GetString(count)).IsEqualTo("4");
        }
    }

    [Test]
    public async Task Poison_copies_are_confirmed_before_ack_and_healthy_messages_continue()
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(2));
        var token = timeout.Token;
        await using var broker = await RabbitMqTopologyTests.Broker.StartAsync(token);
        var control = new RabbitMqReceiveControl();
        using var host = Host(broker.Factory(), control);
        await host.StartAsync(token);
        var transport = host.Services.GetRequiredService<ITransport>();
        var options = new SendOptions().RabbitMQ(properties =>
        {
            properties.CorrelationId = "request-id";
            properties.ReplyTo = "native-replies";
        });
        await transport.SendAsync(new("orders", "bad-json"u8.ToArray(), Headers(), options: options), token);
        await transport.SendAsync(new("orders", "{\"Value\":42}"u8.ToArray(), Headers()), token);
        await Assert.That(await control.Values.Reader.ReadAsync(token)).IsEqualTo(42);
        await host.StopAsync(token);
        await using var native = await broker.Factory().CreateConnectionAsync(token);
        await using var channel = await native.CreateChannelAsync(cancellationToken: token);
        var poison = (await channel.BasicGetAsync("orders.dead-letter", true, token))!;
        await Assert.That(System.Text.Encoding.UTF8.GetString(poison.Body.Span)).IsEqualTo("bad-json");
        await Assert.That(poison.BasicProperties.Headers!.ContainsKey(FailureHeaders.History)).IsTrue();
        await Assert.That(poison.BasicProperties.Headers.ContainsKey("traceparent")).IsTrue();
        await Assert.That(poison.BasicProperties.CorrelationId).IsEqualTo("request-id");
        await Assert.That(poison.BasicProperties.ReplyTo).IsEqualTo("native-replies");
        await Assert.That((await channel.QueueDeclarePassiveAsync("orders", token)).MessageCount).IsEqualTo(0u);
    }

    [Test]
    public async Task Failed_poison_copy_leaves_original_available_for_native_redelivery()
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(2));
        var token = timeout.Token;
        await using var broker = await RabbitMqTopologyTests.Broker.StartAsync(token);
        var control = new RabbitMqReceiveControl();
        using var host = Host(broker.Factory(), control, declareDeadLetter: false);
        await host.StartAsync(token);
        var transport = (RabbitMqNativeTransport)host.Services.GetRequiredService<ITransport>();
        await transport.SendAsync(new("orders", "bad-json"u8.ToArray(), Headers()), token);
        var consumer = transport.Consumers.Single();
        var health = host.Services.GetRequiredService<HealthCheckService>();
        await WaitAsync(async () => (await health.CheckHealthAsync(registration => registration.Name == "relayer.consumer", token)).Status == HealthStatus.Unhealthy, token);
        await Assert.That(consumer.Completion.IsCompleted).IsFalse();
        await Assert.That(host.Services.GetRequiredService<IHostApplicationLifetime>().ApplicationStopping.IsCancellationRequested).IsFalse();
        await using var native = await broker.Factory().CreateConnectionAsync(token);
        await using var channel = await native.CreateChannelAsync(cancellationToken: token);
        // Repair the destination while the original remains owned by the live consumer.
        await channel.QueueDeclareAsync("orders.dead-letter", true, false, false, cancellationToken: token);
        BasicGetResult? poison;
        while ((poison = await channel.BasicGetAsync("orders.dead-letter", true, token)) is null) await Task.Delay(10, token);
        await Assert.That(System.Text.Encoding.UTF8.GetString(poison.Body.Span)).IsEqualTo("bad-json");
        await transport.SendAsync(new("orders", "{\"Value\":42}"u8.ToArray(), Headers()), token);
        await Assert.That(await control.Values.Reader.ReadAsync(token)).IsEqualTo(42);
        await WaitAsync(async () => (await health.CheckHealthAsync(registration => registration.Name == "relayer.consumer", token)).Status == HealthStatus.Healthy, token);
        await Assert.That(consumer.Completion.IsCompleted).IsFalse();
        await host.StopAsync(token);
        await Assert.That((await channel.QueueDeclarePassiveAsync("orders", token)).MessageCount).IsEqualTo(0u);
    }

    [Test]
    public async Task Failure_after_manual_ack_does_not_settle_twice_or_close_the_channel()
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(2));
        var token = timeout.Token;
        await using var broker = await RabbitMqTopologyTests.Broker.StartAsync(token);
        MessageContext? current = null;
        var control = new RabbitMqReceiveControl
        {
            NativeCallback = context => current = context,
            Handle = async value =>
            {
                if (value != 1) return;
                var native = current!.Features.GetRequired<NativeMessage<RabbitMqDelivery>>();
                await native.Message.Channel.BasicAckAsync(native.Message.Message.DeliveryTag, false, token);
                native.Disposition = Disposition.SettledByHandler;
                throw new InvalidOperationException("Failure after successful native ack.");
            },
        };
        using var host = Host(broker.Factory(), control);
        await host.StartAsync(token);
        var transport = (RabbitMqNativeTransport)host.Services.GetRequiredService<ITransport>();
        var consumer = transport.Consumers.Single();
        await transport.SendAsync(new("orders", "{\"Value\":1}"u8.ToArray(), Headers()), token);
        await control.Values.Reader.ReadAsync(token);
        var health = host.Services.GetRequiredService<HealthCheckService>();
        await WaitAsync(async () => (await health.CheckHealthAsync(registration => registration.Name == "relayer.consumer", token)).Status == HealthStatus.Unhealthy, token);
        await transport.SendAsync(new("orders", "{\"Value\":2}"u8.ToArray(), Headers()), token);
        await Assert.That(await control.Values.Reader.ReadAsync(token)).IsEqualTo(2);
        await WaitAsync(async () => (await health.CheckHealthAsync(registration => registration.Name == "relayer.consumer", token)).Status == HealthStatus.Healthy, token);
        await Assert.That(consumer.Completion.IsCompleted).IsFalse();
        await host.StopAsync(token);
    }

    [Test]
    public async Task Concurrent_success_does_not_hide_a_delivery_in_failure_backoff()
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(2));
        var token = timeout.Token;
        await using var broker = await RabbitMqTopologyTests.Broker.StartAsync(token);
        await using var connection = await broker.Factory().CreateConnectionAsync(token);
        await using var channel = await connection.CreateChannelAsync(cancellationToken: token);
        await channel.QueueDeclareAsync("backoff", false, false, false, cancellationToken: token);
        using var tracing = new MessageTracing(Microsoft.Extensions.Options.Options.Create(new TracingOptions()), []);
        await using var transport = new RabbitMqNativeTransport("backoff", new() { ConnectionFactory = broker.Factory() }, ["backoff"], tracing) { HealthTracking = true };
        var clock = new BackoffClock();
        var options = new RabbitMqConsumerOptions { ChannelOptions = new(false, false, consumerDispatchConcurrency: 2), PrefetchCount = 2 };
        var declaration = new ConsumerDefinition<RabbitMqConsumerOptions>("backoff", options, [], 2, 1, TimeSpan.Zero);
        var calls = 0;
        var succeeded = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var consumer = new RabbitMqConsumer(declaration, options, (_, _) =>
        {
            if (Interlocked.Increment(ref calls) == 1) throw new InvalidOperationException("copy failed");
            succeeded.TrySetResult();
            return new(Disposition.Complete);
        }, tracing, clock);
        transport.Consumers.Add(consumer);
        await transport.StartAsync(token);
        await transport.SendAsync(new("backoff", "first"u8.ToArray(), Headers()), token);
        var timer = await clock.Timers.Reader.ReadAsync(token);
        try
        {
            await transport.SendAsync(new("backoff", "second"u8.ToArray(), Headers()), token);
            await succeeded.Task.WaitAsync(token);
            // Drain the successful callback, while the failed callback's timer remains held.
            await WaitAsync(() => Task.FromResult(consumer.ActiveCallbackCount == 1), token);
            await Assert.That(() => consumer.GetHealthState()).Throws<InvalidOperationException>();
        }
        finally { timer.Fire(); }
        await WaitAsync(() =>
        {
            try { return Task.FromResult(consumer.GetHealthState()?.IsIdle == true); }
            catch (InvalidOperationException) { return Task.FromResult(false); }
        }, token);
        await Assert.That(consumer.Completion.IsCompleted).IsFalse();
        await transport.StopAsync(token);
    }

    private sealed class BackoffClock : TimeProvider
    {
        internal Channel<BackoffTimer> Timers { get; } = Channel.CreateUnbounded<BackoffTimer>();
        public override ITimer CreateTimer(TimerCallback callback, object? state, TimeSpan dueTime, TimeSpan period)
        {
            var timer = new BackoffTimer(callback, state);
            Timers.Writer.TryWrite(timer);
            return timer;
        }
    }
    private sealed class BackoffTimer(TimerCallback callback, object? state) : ITimer
    {
        internal void Fire() => callback(state);
        public bool Change(TimeSpan dueTime, TimeSpan period) => true;
        public void Dispose() { }
        public ValueTask DisposeAsync() => default;
    }

    [Test]
    public async Task Optional_health_tracks_oldest_active_callback_and_stop_drains_before_closing_channel()
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(2));
        var token = timeout.Token;
        await using var broker = await RabbitMqTopologyTests.Broker.StartAsync(token);
        var clock = new RabbitMqClock();
        var first = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var second = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var control = new RabbitMqReceiveControl { Handle = value => new(value == 1 ? first.Task : second.Task) };
        using var host = Host(broker.Factory(), control, clock: clock, concurrency: 2);
        var probe = host.Services.GetRequiredKeyedService<ITransportHealthProbe>("rabbit");
        await Assert.That((await probe.CheckAsync(token)).Status).IsEqualTo(HealthStatus.Unhealthy);
        await Assert.That(host.Services.GetKeyedService<ITransportHealthProbe>("other")).IsNull();
        await host.StartAsync(token);
        var definition = host.Services.GetRequiredService<RelayerConfiguration>().Transports.Single().Consumers.Single();
        var health = host.Services.GetRequiredKeyedService<IConsumerHealthProvider>("rabbit");
        await Assert.That((await probe.CheckAsync(token)).Status).IsEqualTo(HealthStatus.Healthy);
        await WaitAsync(async () => (await health.GetStateAsync(definition, token))?.IsIdle == true, token);
        var transport = host.Services.GetRequiredService<ITransport>();
        await transport.SendAsync(new("orders", "{\"Value\":1}"u8.ToArray(), Headers()), token);
        await control.Values.Reader.ReadAsync(token);
        var oldest = clock.GetUtcNow();
        clock.Advance(TimeSpan.FromMinutes(1));
        await transport.SendAsync(new("orders", "{\"Value\":2}"u8.ToArray(), Headers()), token);
        await control.Values.Reader.ReadAsync(token);
        var state = (await health.GetStateAsync(definition, token))!.Value;
        await Assert.That(state.IsIdle).IsFalse();
        await Assert.That(state.LastProgressAt).IsEqualTo(oldest);
        var liveness = await host.Services.GetRequiredService<HealthCheckService>().CheckHealthAsync(registration => registration.Name == "relayer.consumer", token);
        await Assert.That(liveness.Status).IsEqualTo(HealthStatus.Unhealthy);
        second.TrySetResult();
        await WaitAsync(async () => (await health.GetStateAsync(definition, token))?.LastProgressAt == oldest, token);
        var stop = host.StopAsync(token);
        await Assert.That(stop.IsCompleted).IsFalse();
        first.TrySetResult();
        await stop;
        await Assert.That((await probe.CheckAsync(token)).Status).IsEqualTo(HealthStatus.Unhealthy);
        await Assert.That(await health.GetStateAsync(definition, token)).IsNull();
        await using var native = await broker.Factory().CreateConnectionAsync(token);
        await using var channel = await native.CreateChannelAsync(cancellationToken: token);
        await Assert.That((await channel.QueueDeclarePassiveAsync("orders", token)).MessageCount).IsEqualTo(0u);
    }

    [Test]
    public async Task Process_samplers_see_actual_native_destination_and_ack_spans_have_delivery_attributes()
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(2));
        var token = timeout.Token;
        await using var broker = await RabbitMqTopologyTests.Broker.StartAsync(token);
        var control = new RabbitMqReceiveControl();
        using var host = Host(broker.Factory(), control);
        var tracing = host.Services.GetRequiredService<MessageTracing>();
        var sampler = new TaskCompletionSource<string?>(TaskCreationOptions.RunContinuationsAsynchronously);
        var ack = new TaskCompletionSource<Activity>(TaskCreationOptions.RunContinuationsAsynchronously);
        using var listener = new ActivityListener
        {
            ShouldListenTo = source => ReferenceEquals(source, tracing.Source),
            Sample = (ref ActivityCreationOptions<ActivityContext> options) =>
            {
                if (options.Name.StartsWith("process "))
                    sampler.TrySetResult(options.Tags?.First(tag => tag.Key == "messaging.destination.name").Value as string);
                return ActivitySamplingResult.AllDataAndRecorded;
            },
            ActivityStopped = activity => { if (activity.OperationName.StartsWith("ack ")) ack.TrySetResult(activity); },
        };
        ActivitySource.AddActivityListener(listener);
        await host.StartAsync(token);
        await host.Services.GetRequiredService<ITransport>().SendAsync(new("orders", "{\"Value\":42}"u8.ToArray(), Headers()), token);
        await Assert.That(await sampler.Task.WaitAsync(token)).IsEqualTo(":orders:orders");
        var settled = await ack.Task.WaitAsync(token);
        await Assert.That(settled.OperationName).IsEqualTo("ack :orders:orders");
        await Assert.That(settled.GetTagItem("messaging.rabbitmq.destination.routing_key")).IsEqualTo("orders");
        await Assert.That(settled.GetTagItem("messaging.rabbitmq.message.delivery_tag")).IsNotNull();
        await host.StopAsync(token);
    }

    [Test]
    public async Task Native_nack_requeues_once_then_routes_to_configured_DLX_without_rewriting_the_body()
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(2));
        var token = timeout.Token;
        await using var broker = await RabbitMqTopologyTests.Broker.StartAsync(token);
        await using var connection = await broker.Factory().CreateConnectionAsync(token);
        await using var channel = await connection.CreateChannelAsync(cancellationToken: token);
        await channel.QueueDeclareAsync("native.dead", true, false, false, cancellationToken: token);
        await channel.QueueDeclareAsync("native.source", true, false, false,
            new Dictionary<string, object?> { ["x-dead-letter-exchange"] = "", ["x-dead-letter-routing-key"] = "native.dead" }, cancellationToken: token);
        using var tracing = new MessageTracing(Microsoft.Extensions.Options.Options.Create(new TracingOptions()), []);
        var nacks = Channel.CreateUnbounded<Activity>();
        using var listener = new ActivityListener
        {
            ShouldListenTo = source => ReferenceEquals(source, tracing.Source),
            Sample = static (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
            ActivityStopped = activity => { if (activity.OperationName.StartsWith("nack ")) nacks.Writer.TryWrite(activity); },
        };
        ActivitySource.AddActivityListener(listener);
        await using var transport = new RabbitMqNativeTransport("native", new() { ConnectionFactory = broker.Factory() }, ["native.source"], tracing);
        var options = new RabbitMqConsumerOptions();
        var declaration = new ConsumerDefinition<RabbitMqConsumerOptions>("native.source", options, [], 1, 1, TimeSpan.Zero);
        var attempts = 0;
        var requeued = false;
        transport.Consumers.Add(new(declaration, options, (delivery, _) =>
        {
            requeued |= delivery.Message.Redelivered;
            return new(++attempts == 1 ? Disposition.Redeliver(TimeSpan.Zero) : Disposition.DeadLettered(new("native poison")));
        }, tracing));
        await transport.StartAsync(token);
        await transport.SendAsync(new("native.source", "unchanged"u8.ToArray(), Headers()), token);
        await nacks.Reader.ReadAsync(token);
        var dead = await nacks.Reader.ReadAsync(token);
        await Assert.That(dead.OperationName).IsEqualTo("nack :native.source:native.source");
        await Assert.That(dead.GetTagItem("messaging.rabbitmq.message.delivery_tag")).IsNotNull();
        await transport.StopAsync(token);
        BasicGetResult? result;
        while ((result = await channel.BasicGetAsync("native.dead", true, token)) is null) await Task.Delay(10, token);
        await Assert.That(System.Text.Encoding.UTF8.GetString(result.Body.Span)).IsEqualTo("unchanged");
        await Assert.That(requeued).IsTrue();
        await Assert.That(attempts).IsEqualTo(2);
    }

    [Test]
    [Arguments(true)]
    [Arguments(false)]
    public async Task In_flight_ack_failure_respects_native_automatic_recovery(bool automaticRecovery)
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(2));
        var token = timeout.Token;
        await using var broker = await RabbitMqTopologyTests.Broker.StartAsync(token, dedicated: true);
        var factory = broker.Factory();
        factory.AutomaticRecoveryEnabled = automaticRecovery;
        factory.NetworkRecoveryInterval = TimeSpan.FromMilliseconds(100);
        var finish = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var failedAck = new TaskCompletionSource<Exception>(TaskCreationOptions.RunContinuationsAsynchronously);
        var attempts = 0;
        var control = new RabbitMqReceiveControl { Handle = _ => Interlocked.Increment(ref attempts) == 1 ? new(finish.Task) : default };
        using var host = Host(factory, control);
        await host.StartAsync(token);
        var transport = (RabbitMqNativeTransport)host.Services.GetRequiredService<ITransport>();
        var consumer = transport.Consumers.Single();
        var probe = host.Services.GetRequiredKeyedService<ITransportHealthProbe>("rabbit");
        // Observe the real native exception after Relayer's callback exception handler runs.
        control.NativeCallback = context => context.RabbitMQ().Channel.CallbackExceptionAsync += (_, args) =>
        {
            failedAck.TrySetResult(args.Exception);
            return Task.CompletedTask;
        };
        await transport.SendAsync(new("orders", "{\"Value\":1}"u8.ToArray(), Headers()), token);
        await control.Values.Reader.ReadAsync(token);
        await broker.CommandAsync(["rabbitmqctl", "stop_app"], token);
        await WaitAsync(async () => (await probe.CheckAsync(token)).Status == HealthStatus.Unhealthy, token);
        finish.TrySetResult();
        await Assert.That(await failedAck.Task.WaitAsync(token)).IsTypeOf<AlreadyClosedException>();
        if (!automaticRecovery)
        {
            await Assert.That(consumer.Completion.IsFaulted).IsTrue();
#if NET11_0_OR_GREATER
            // Hosting 11 propagates the observed background-service fault from StopAsync.
            await Assert.That(async () => await host.StopAsync(token)).Throws<AlreadyClosedException>();
#else
            await host.StopAsync(token);
#endif
            return;
        }
        await Assert.That(consumer.Completion.IsCompleted).IsFalse();
        await Assert.That(host.Services.GetRequiredService<IHostApplicationLifetime>().ApplicationStopping.IsCancellationRequested).IsFalse();
        await broker.CommandAsync(["rabbitmqctl", "start_app"], token);
        await Assert.That(await control.Values.Reader.ReadAsync(token)).IsEqualTo(1);
        await WaitAsync(() => Task.FromResult(consumer.GetHealthState()?.IsIdle == true), token);
        await Assert.That(consumer.Completion.IsCompleted).IsFalse();
        await host.StopAsync(token);
    }

    [Test]
    public async Task Normal_stop_with_native_recovery_disabled_completes_successfully()
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(2));
        var token = timeout.Token;
        await using var broker = await RabbitMqTopologyTests.Broker.StartAsync(token);
        var factory = broker.Factory();
        factory.AutomaticRecoveryEnabled = false;
        using var host = Host(factory, new());
        await host.StartAsync(token);
        var consumer = ((RabbitMqNativeTransport)host.Services.GetRequiredService<ITransport>()).Consumers.Single();
        await host.StopAsync(token);
        await Assert.That(consumer.Completion.IsCompletedSuccessfully).IsTrue();
    }

    [Test]
    [Arguments(true)]
    [Arguments(false)]
    public async Task Idle_connection_shutdown_respects_native_automatic_recovery(bool automaticRecovery)
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(2));
        var token = timeout.Token;
        await using var broker = await RabbitMqTopologyTests.Broker.StartAsync(token, dedicated: true);
        var factory = broker.Factory();
        factory.AutomaticRecoveryEnabled = automaticRecovery;
        factory.NetworkRecoveryInterval = TimeSpan.FromMilliseconds(100);
        var control = new RabbitMqReceiveControl();
        using var host = Host(factory, control);
        await host.StartAsync(token);
        var transport = (RabbitMqNativeTransport)host.Services.GetRequiredService<ITransport>();
        var consumer = transport.Consumers.Single();
        await WaitAsync(() => Task.FromResult(consumer.GetHealthState()?.IsIdle == true), token);
        await broker.CommandAsync(["rabbitmqctl", "stop_app"], token);
        if (!automaticRecovery)
        {
            await Assert.That(async () => await consumer.Completion.WaitAsync(token)).Throws<AlreadyClosedException>();
            await WaitAsync(() => Task.FromResult(host.Services.GetRequiredService<IHostApplicationLifetime>().ApplicationStopping.IsCancellationRequested), token);
        }
        else
        {
            var probe = host.Services.GetRequiredKeyedService<ITransportHealthProbe>("rabbit");
            await WaitAsync(async () => (await probe.CheckAsync(token)).Status == HealthStatus.Unhealthy, token);
            await Assert.That(consumer.Completion.IsCompleted).IsFalse();
            await broker.CommandAsync(["rabbitmqctl", "start_app"], token);
            await WaitAsync(() => Task.FromResult(consumer.GetHealthState()?.IsIdle == true), token);
            await transport.SendAsync(new("orders", "{\"Value\":42}"u8.ToArray(), Headers()), token);
            await Assert.That(await control.Values.Reader.ReadAsync(token)).IsEqualTo(42);
            await Assert.That(consumer.Completion.IsCompleted).IsFalse();
        }
#if NET11_0_OR_GREATER
        if (!automaticRecovery)
            await Assert.That(async () => await host.StopAsync(token)).Throws<AlreadyClosedException>();
        else
#endif
            await host.StopAsync(token);
        if (automaticRecovery) await Assert.That(consumer.Completion.IsCompletedSuccessfully).IsTrue();
    }

    [Test]
    public async Task Hosted_consumer_and_health_recover_after_a_broker_restart()
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(2));
        var token = timeout.Token;
        await using var broker = await RabbitMqTopologyTests.Broker.StartAsync(token, dedicated: true);
        var factory = broker.Factory();
        factory.NetworkRecoveryInterval = TimeSpan.FromMilliseconds(100);
        var control = new RabbitMqReceiveControl();
        using var host = Host(factory, control);
        await host.StartAsync(token);
        var transport = host.Services.GetRequiredService<ITransport>();
        var probe = host.Services.GetRequiredKeyedService<ITransportHealthProbe>("rabbit");
        await transport.SendAsync(new("orders", "{\"Value\":1}"u8.ToArray(), Headers()), token);
        await control.Values.Reader.ReadAsync(token);
        // First drain completion before injecting the outage: duplicates while settling are legal.
        var consumer = ((RabbitMqNativeTransport)transport).Consumers.Single();
        await WaitAsync(() => Task.FromResult(consumer.GetHealthState()?.IsIdle == true), token);
        await broker.CommandAsync(["rabbitmqctl", "stop_app"], token);
        await WaitAsync(async () => (await probe.CheckAsync(token)).Status == HealthStatus.Unhealthy, token);
        await broker.CommandAsync(["rabbitmqctl", "start_app"], token);
        await WaitAsync(async () => (await probe.CheckAsync(token)).Status == HealthStatus.Healthy, token);
        await WaitAsync(() => Task.FromResult(consumer.GetHealthState()?.IsIdle == true), token);
        await transport.SendAsync(new("orders", "{\"Value\":2}"u8.ToArray(), Headers()), token);
        await Assert.That(await control.Values.Reader.ReadAsync(token)).IsEqualTo(2);
        await host.StopAsync(token);
    }

    internal static IHost Host(ConnectionFactory factory, RabbitMqReceiveControl control, bool declareDeadLetter = true, RabbitMqClock? clock = null, int concurrency = 1) =>
        new HostBuilder().ConfigureServices(services =>
        {
            services.AddSingleton(control);
            if (clock is not null) services.AddSingleton<TimeProvider>(clock);
            services.AddRelayerRabbitMqHealthProviders("rabbit");
            services.AddRelayerHealthChecks(new RelayerHealthCheckOptions { ConsumerStallTimeout = TimeSpan.FromSeconds(30) });
            services.AddRelayer(builder =>
            {
                builder.MessageTypes.Add<RabbitMqReceivePayload>("rabbit.receive");
                builder.UseJsonSerializer(RabbitMqReceiveJson.Default);
                builder.RabbitMQ("rabbit", rabbit =>
                {
                    rabbit.TopologyMode = TopologyMode.Provision;
                    rabbit.Options.Configure(options =>
                    {
                        options.ConnectionFactory = factory;
                        options.Queues.Add(new("orders"));
                        if (declareDeadLetter) options.Queues.Add(new("orders.dead-letter"));
                    });
                    rabbit.Consume("orders", consumer =>
                    {
                        consumer.Concurrency(concurrency);
                        consumer.Options.ChannelOptions = new(false, false, consumerDispatchConcurrency: (ushort)concurrency);
                        consumer.Options.PrefetchCount = (ushort)concurrency;
                        consumer.Handler<RabbitMqReceiveHandler>();
                    });
                });
            });
        }).Build();

    internal static KeyValuePair<string, WireString>[] Headers() => [new("cloudEvents:id", new("receive-id")),
        new("cloudEvents:type", new("rabbit.receive;v=1")), new("content-type", new("application/json")),
        new("traceparent", new("00-0123456789abcdef0123456789abcdef-0123456789abcdef-01"))];
    private static async Task WaitAsync(Func<Task<bool>> condition, CancellationToken token)
    { while (!await condition()) await Task.Delay(10, token); }
}

public sealed class RabbitMqReceiveControl
{
    public Channel<int> Values { get; } = Channel.CreateUnbounded<int>();
    public Func<int, ValueTask>? Handle { get; init; }
    public Action<MessageContext>? NativeCallback { get; set; }
}
[Message("rabbit.receive")]
public sealed record RabbitMqReceivePayload(int Value);
public sealed class RabbitMqReceiveHandler(RabbitMqReceiveControl control) : IHandler<RabbitMqReceivePayload>
{
    public ValueTask HandleAsync(RabbitMqReceivePayload message, MessageContext context, CancellationToken cancellationToken)
    {
        control.NativeCallback?.Invoke(context);
        control.Values.Writer.TryWrite(message.Value);
        return control.Handle?.Invoke(message.Value) ?? default;
    }
}
internal sealed class RabbitMqClock : TimeProvider
{
    private long _ticks = new DateTimeOffset(2030, 1, 1, 0, 0, 0, TimeSpan.Zero).UtcTicks;
    public override DateTimeOffset GetUtcNow() => new(Volatile.Read(ref _ticks), TimeSpan.Zero);
    internal void Advance(TimeSpan by) => Interlocked.Add(ref _ticks, by.Ticks);
}
[JsonSerializable(typeof(RabbitMqReceivePayload))]
internal partial class RabbitMqReceiveJson : JsonSerializerContext;
