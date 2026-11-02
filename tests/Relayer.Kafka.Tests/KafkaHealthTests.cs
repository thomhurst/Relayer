using System.Diagnostics;
using Dekaf;
using Dekaf.Admin;
using Dekaf.Consumer;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Relayer.HealthChecks;

namespace Relayer.Kafka.Tests;

public class KafkaHealthTests
{
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task Synchronous_batch_stays_active_through_native_commit_and_returns_idle_afterward(bool alreadyActive)
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(2));
        var token = timeout.Token;
        using var tracing = new MessageTracing(Options.Create(new TracingOptions()), []);
        await using var fixture = (KafkaFixture)await KafkaFixture.CreateAsync(tracing, token);
        await using var producer = await new ProducerBuilder<byte[], byte[]>().WithBootstrapServers(fixture.Bootstrap).BuildAsync(token);
        await producer.ProduceAsync(new() { Topic = fixture.Destination, Key = [1], Value = [1] }, token);
        var native = new ConsumerBuilder<byte[], byte[]>().WithBootstrapServers(fixture.Bootstrap).WithGroupId(fixture.Destination)
            .WithOffsetCommitMode(OffsetCommitMode.Manual).WithAutoOffsetStore(false).WithAutoOffsetReset(AutoOffsetReset.Earliest).Build();
        native.Assign(new TopicPartition(fixture.Destination, 0));
        await native.InitializeAsync(token);
        var clock = new KafkaHealthClock();
        var worker = new KafkaWorkerHealth(clock);
        worker.Idle();
        var startedAt = clock.GetUtcNow();
        if (alreadyActive)
        {
            worker.Progress();
            clock.Advance(TimeSpan.FromSeconds(10));
        }
        long duringHandler = 0, beforeCommit = 0, afterCommit = 0;
        using var listener = new ActivityListener
        {
            ShouldListenTo = source => ReferenceEquals(source, tracing.Source),
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllData,
            ActivityStarted = activity =>
            {
                if (activity.OperationName != "commit " + fixture.Destination) return;
                beforeCommit = worker.Read();
                // A commit that takes longer than the stall timeout must remain active,
                // including when every handler completed synchronously before the await.
                clock.Advance(TimeSpan.FromMinutes(2));
            },
            ActivityStopped = activity =>
            {
                if (activity.OperationName == "commit " + fixture.Destination) afterCommit = worker.Read();
            },
        };
        ActivitySource.AddActivityListener(listener);
        await using var adapter = new KafkaConsumer(native, (_, _) =>
        {
            duringHandler = worker.Read();
            return new(Disposition.Complete);
        }, tracing, new() { System = "kafka", Destination = fixture.Destination }, new(), worker);
        await using var batches = native.ConsumeBatchAsync(token).GetAsyncEnumerator();
        await Assert.That(await batches.MoveNextAsync()).IsTrue();
        await adapter.ProcessBatchAsync(batches.Current, token);
        await Assert.That(duringHandler).IsEqualTo(startedAt.UtcTicks);
        await Assert.That(beforeCommit).IsEqualTo(startedAt.UtcTicks);
        await Assert.That(afterCommit).IsEqualTo(startedAt.UtcTicks);
        await Assert.That(clock.GetUtcNow() - new DateTimeOffset(afterCommit, TimeSpan.Zero))
            .IsEqualTo(TimeSpan.FromMinutes(2) + (alreadyActive ? TimeSpan.FromSeconds(10) : TimeSpan.Zero));
        await Assert.That(worker.Read()).IsEqualTo(-clock.GetUtcNow().UtcTicks);
        await Assert.That(await native.GetCommittedOffsetAsync(new(fixture.Destination, 0), token)).IsEqualTo(1L);
    }

    [Test]
    public async Task Oldest_active_worker_wins_over_idle_and_progressing_workers_of_exact_declaration()
    {
        var services = new ServiceCollection();
        services.AddRelayer(builder => builder.Kafka("events", kafka =>
        {
            kafka.Consume("orders", consumer => { consumer.Options.Group = "one"; consumer.Handler<KafkaHealthHandler>(); });
            kafka.Consume("orders", consumer => { consumer.Options.Group = "two"; consumer.Handler<KafkaHealthHandler>(); });
        }));
        await using var provider = services.BuildServiceProvider();
        var consumers = provider.GetRequiredService<RelayerConfiguration>().Transports.Single().Consumers;
        var clock = new KafkaHealthClock();
        var health = new KafkaConsumerHealth(clock);
        await Assert.That(health.GetState(consumers[0])).IsNull();
        var stalled = health.Register(consumers[0]);
        var busy = health.Register(consumers[0]);
        var idle = health.Register(consumers[0]);
        var sibling = health.Register(consumers[1]);
        await Assert.That(health.GetState(consumers[0])).IsNull();
        stalled.Progress();
        var activation = clock.GetUtcNow();
        clock.Advance(TimeSpan.FromMinutes(10));
        busy.Progress();
        idle.Idle();
        sibling.Idle();
        await Assert.That(health.GetState(consumers[0])!.Value.IsIdle).IsFalse();
        await Assert.That(health.GetState(consumers[0])!.Value.LastProgressAt).IsEqualTo(activation);
        await Assert.That(health.GetState(consumers[1])!.Value.IsIdle).IsTrue();
        stalled.Idle();
        await Assert.That(health.GetState(consumers[0])!.Value.LastProgressAt).IsEqualTo(clock.GetUtcNow());
        busy.Idle();
        await Assert.That(health.GetState(consumers[0])!.Value.IsIdle).IsTrue();
        busy.Unavailable();
        await Assert.That(health.GetState(consumers[0])).IsNull();
    }

    [Test]
    public async Task Named_registration_is_idempotent_case_sensitive_and_preserves_custom_adapters()
    {
        var services = new ServiceCollection();
        var custom = new CustomKafkaHealth();
        services.AddKeyedSingleton<ITransportHealthProbe>("events", custom);
        services.AddKeyedSingleton<IConsumerHealthProvider>("events", custom);
        services.AddRelayerKafkaHealthProviders("events");
        services.AddRelayerKafkaHealthProviders("events");
        services.AddRelayerKafkaHealthProviders("Events");
        await using var provider = services.BuildServiceProvider();
        await Assert.That(provider.GetRequiredKeyedService<ITransportHealthProbe>("events")).IsSameReferenceAs(custom);
        await Assert.That(provider.GetRequiredKeyedService<IConsumerHealthProvider>("events")).IsSameReferenceAs(custom);
        await Assert.That(provider.GetKeyedServices<IConsumerHealthProvider>("events").Count()).IsEqualTo(1);
        await Assert.That(provider.GetRequiredKeyedService<IConsumerHealthProvider>("Events")).IsNotSameReferenceAs(custom);
        await Assert.That(provider.GetKeyedService<IConsumerHealthProvider>("EVENTS")).IsNull();
    }

    [Test]
    public async Task Connectivity_adapter_rejects_a_mismatched_transport_name_before_creating_a_client()
    {
        var services = new ServiceCollection();
        services.AddRelayer(builder => builder.Kafka("events", _ => { }));
        services.AddRelayerKafkaHealthProviders("Events");
        await using var provider = services.BuildServiceProvider();
        await Assert.That(() => provider.GetRequiredKeyedService<ITransportHealthProbe>("Events"))
            .Throws<InvalidOperationException>().WithMessageContaining("exact case-sensitive transport name");
    }

    [Test]
    public async Task Healthy_idle_stalled_stopped_and_disconnected_states_use_native_Kafka_and_receiver_state()
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(2));
        var token = timeout.Token;
        using var tracing = new MessageTracing(Options.Create(new TracingOptions()), []);
        await using var fixture = (KafkaFixture)await KafkaFixture.CreateAsync(tracing, token);
        var clock = new KafkaHealthClock();
        var control = new KafkaHealthControl();
        var builder = Host.CreateApplicationBuilder();
        builder.Logging.ClearProviders();
        builder.Logging.SetMinimumLevel(LogLevel.Debug).AddSimpleConsole(options =>
        {
            options.SingleLine = true;
            options.TimestampFormat = "HH:mm:ss.fff ";
        });
        builder.Services.AddSingleton<TimeProvider>(clock);
        builder.Services.AddSingleton(control);
        var adminConfigured = 0;
        builder.Services.AddRelayerKafkaHealthProviders("events", native =>
        {
            Interlocked.Increment(ref adminConfigured);
            native.WithClientId("relayer-health-test").WithConnectionTimeout(TimeSpan.FromSeconds(1));
        });
        builder.Services.AddRelayerHealthChecks(new RelayerHealthCheckOptions { ConsumerStallTimeout = TimeSpan.FromMinutes(1) });
        // Bound each native probe independently of the overall test deadline. ConnectionTimeout
        // bounds one connection attempt, not the admin operation's retries while disconnected.
        builder.Services.Configure<HealthCheckServiceOptions>(options =>
            options.Registrations.Single(check => check.Name == "relayer.transport").Timeout = TimeSpan.FromSeconds(5));
        builder.Services.AddRelayer(relayer =>
        {
            relayer.AddMessageTypesFromThisAssembly();
            relayer.UseJsonSerializer(KafkaReceiveJson.Default);
            relayer.Kafka("events", kafka =>
            {
                kafka.TopologyMode = TopologyMode.None;
                kafka.Options.Configure(options =>
                {
                    options.BootstrapServers.Add(fixture.Bootstrap);
                    options.ConfigureConsumer = native => native.WithAutoOffsetReset(AutoOffsetReset.Earliest);
                });
                kafka.Consume(fixture.Destination, consumer =>
                {
                    consumer.Options.Group = fixture.Destination;
                    consumer.Handler<KafkaHealthHandler>();
                });
                // An idle declaration on the same source must not mask the active one.
                kafka.Consume(fixture.Destination, consumer =>
                {
                    consumer.Options.Group = fixture.Destination + ".idle";
                    consumer.Handler<KafkaHealthIdleHandler>();
                });
            });
        });
        using var host = builder.Build();
        var checks = host.Services.GetRequiredService<HealthCheckService>();
        var beforeStart = await checks.CheckHealthAsync(check => check.Name == "relayer.consumer", token);
        await Assert.That(beforeStart.Status).IsEqualTo(HealthStatus.Unhealthy);
        await host.StartAsync(token);
        try
        {
            await Assert.That((await checks.CheckHealthAsync(check => check.Name == "relayer.transport", token)).Status).IsEqualTo(HealthStatus.Healthy);
            await Assert.That(adminConfigured).IsEqualTo(1);
            clock.Advance(TimeSpan.FromHours(1));
            await Assert.That((await checks.CheckHealthAsync(check => check.Name == "relayer.consumer", token)).Status).IsEqualTo(HealthStatus.Healthy);
            await using (var admin = new AdminClientBuilder().WithBootstrapServers(fixture.Bootstrap).Build())
            {
                var offsets = await admin.ListOffsetsAsync([new() { TopicPartition = new(fixture.Destination, 0), Spec = OffsetSpec.Latest }], cancellationToken: token);
                await Assert.That(offsets[new(fixture.Destination, 0)].Offset).IsEqualTo(0L);
            }
            await fixture.Transport.StartAsync(token);
            await fixture.Transport.SendAsync(new(fixture.Destination, "{\"Number\":1}"u8.ToArray(),
                new KeyValuePair<string, WireString>[] { new("ce_id", new("health")), new("ce_type", new("tests.kafka.receive;v=1")) }), token);
            await control.Entered.Task.WaitAsync(token);
            await control.IdleHandled.Task.WaitAsync(token);
            var consumers = host.Services.GetRequiredService<RelayerConfiguration>().Transports.Single().Consumers;
            var adapter = host.Services.GetRequiredKeyedService<IConsumerHealthProvider>("events");
            await WaitForIdleAsync(adapter, consumers[1], token);
            await Assert.That((await checks.CheckHealthAsync(check => check.Name == "relayer.consumer", token)).Status).IsEqualTo(HealthStatus.Healthy);
            clock.Advance(TimeSpan.FromMinutes(1));
            var stalled = await checks.CheckHealthAsync(check => check.Name == "relayer.consumer", token);
            await Assert.That(stalled.Status).IsEqualTo(HealthStatus.Unhealthy);
            var details = stalled.Entries["relayer.consumer"].Data;
            await Assert.That(((HealthCheckResult)details[$"events/0:{fixture.Destination}"]).Status).IsEqualTo(HealthStatus.Unhealthy);
            await Assert.That(((HealthCheckResult)details[$"events/1:{fixture.Destination}"]).Status).IsEqualTo(HealthStatus.Healthy);
            control.Release.TrySetResult();
            await WaitForIdleAsync(adapter, consumers[0], token);
            await Assert.That((await checks.CheckHealthAsync(check => check.Name == "relayer.consumer", token)).Status).IsEqualTo(HealthStatus.Healthy);
            await Assert.That(async () => await adapter.GetStateAsync(consumers[0], new CancellationToken(true))).Throws<OperationCanceledException>();
        }
        finally
        {
            control.Release.TrySetResult();
            await host.StopAsync(token);
        }
        await Assert.That((await checks.CheckHealthAsync(check => check.Name == "relayer.consumer", token)).Status).IsEqualTo(HealthStatus.Unhealthy);
        var probe = host.Services.GetRequiredKeyedService<ITransportHealthProbe>("events");
        await Assert.That(async () => await probe.CheckAsync(new CancellationToken(true))).Throws<OperationCanceledException>();
        await fixture.DisconnectAsync(token);
        var disconnected = await checks.CheckHealthAsync(check => check.Name == "relayer.transport", token);
        await Assert.That(disconnected.Status).IsEqualTo(HealthStatus.Unhealthy);
        await Assert.That(disconnected.Entries["relayer.transport"].Exception).IsNotNull();
        await fixture.ReconnectAsync(token);
        var recovered = await checks.CheckHealthAsync(check => check.Name == "relayer.transport", token);
        await Assert.That(recovered.Status).IsEqualTo(HealthStatus.Healthy);
        await Assert.That(adminConfigured).IsEqualTo(1);
    }

    private static async Task WaitForIdleAsync(IConsumerHealthProvider adapter, IConsumerDefinition consumer, CancellationToken token)
    {
        while ((await adapter.GetStateAsync(consumer, token)) is not { IsIdle: true })
            await Task.Delay(10, token);
    }

    private sealed class CustomKafkaHealth : ITransportHealthProbe, IConsumerHealthProvider
    {
        public ValueTask<HealthCheckResult> CheckAsync(CancellationToken cancellationToken) => new(HealthCheckResult.Healthy());
        public ValueTask<ConsumerHealthState?> GetStateAsync(IConsumerDefinition consumer, CancellationToken cancellationToken) => new((ConsumerHealthState?)null);
    }
}

internal sealed class KafkaHealthClock : TimeProvider
{
    private long _ticks = new DateTimeOffset(2026, 11, 1, 0, 0, 0, TimeSpan.Zero).Ticks;
    public override DateTimeOffset GetUtcNow() => new(Volatile.Read(ref _ticks), TimeSpan.Zero);
    internal void Advance(TimeSpan duration) => Interlocked.Add(ref _ticks, duration.Ticks);
}

public sealed class KafkaHealthControl
{
    internal TaskCompletionSource Entered { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
    internal TaskCompletionSource Release { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
    internal TaskCompletionSource IdleHandled { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
}

public sealed class KafkaHealthHandler(KafkaHealthControl control) : IHandler<KafkaReceiveMessage>
{
    public async ValueTask HandleAsync(KafkaReceiveMessage message, MessageContext context, CancellationToken cancellationToken)
    {
        control.Entered.TrySetResult();
        await control.Release.Task.WaitAsync(cancellationToken);
    }
}

public sealed class KafkaHealthIdleHandler(KafkaHealthControl control) : IHandler<KafkaReceiveMessage>
{
    public ValueTask HandleAsync(KafkaReceiveMessage message, MessageContext context, CancellationToken cancellationToken)
    {
        control.IdleHandled.TrySetResult();
        return default;
    }
}
