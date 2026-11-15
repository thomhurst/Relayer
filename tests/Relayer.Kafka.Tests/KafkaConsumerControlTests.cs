using System.Text;
using Dekaf;
using Dekaf.Admin;
using Dekaf.Consumer;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Relayer.Transports;

namespace Relayer.Kafka.Tests;

[ClassDataSource<KafkaHeaderSnapshotBroker>(Shared = SharedType.PerTestSession)]
public class KafkaConsumerControlTests(KafkaHeaderSnapshotBroker broker)
{
    [Test]
    public async Task Control_fails_before_the_host_starts()
    {
        await broker.Started;
        using var host = BuildHost("unused", new(), autoStart: false);
        var control = host.Services.GetRequiredKeyedService<IConsumerControl>("events");
        await Assert.That(async () => await control.StartAsync(default)).Throws<InvalidOperationException>()
            .WithMessageContaining("start the host");
    }

    [Test]
    public async Task Deferred_consumer_receives_only_after_the_control_starts_it_and_resumes_after_a_stop()
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(2));
        var token = timeout.Token;
        var topic = await CreateTopicAsync("deferred", token);
        var state = new KafkaReplayState();
        using var host = BuildHost(topic, state, autoStart: false);
        var control = host.Services.GetRequiredKeyedService<IConsumerControl>("events");
        var consumers = host.Services.GetRequiredKeyedService<KafkaProducerTransport>("events").Consumers;
        await host.StartAsync(token);
        try
        {
            // Sending works while the composed consumer stays stopped.
            await SendAsync(host, topic, 1, token);
            await Assert.That(consumers.Count).IsEqualTo(1);
            await Assert.That(consumers[0].Completion.IsCompleted).IsTrue();
            await Assert.That(state.Handled.Reader.TryRead(out _)).IsFalse();
            await Assert.That(await CommittedAsync(topic, token)).IsNull();

            await control.StartAsync(token);
            await control.StartAsync(token);
            await Assert.That(await state.Handled.Reader.ReadAsync(token)).IsEqualTo(0L);

            await control.StopAsync(token);
            await control.StopAsync(token);
            await Assert.That(consumers[0].Completion.IsCompletedSuccessfully).IsTrue();
            await Assert.That(await CommittedAsync(topic, token)).IsEqualTo(1L);
            await SendAsync(host, topic, 2, token);

            // A restart runs a new group member from the committed offset.
            await control.StartAsync(token);
            await Assert.That(await state.Handled.Reader.ReadAsync(token)).IsEqualTo(1L);
        }
        finally { await host.StopAsync(token); }
        await Assert.That(consumers[0].Completion.IsCompletedSuccessfully).IsTrue();
        await Assert.That(await CommittedAsync(topic, token)).IsEqualTo(2L);
        await Assert.That(async () => await control.StartAsync(token)).Throws<InvalidOperationException>()
            .WithMessageContaining("start the host");
    }

    [Test]
    public async Task Host_shutdown_with_an_expired_token_stops_consumers_the_control_started()
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(2));
        var token = timeout.Token;
        var topic = await CreateTopicAsync("expired", token);
        var state = new KafkaReplayState();
        using var host = BuildHost(topic, state, autoStart: false);
        var consumers = host.Services.GetRequiredKeyedService<KafkaProducerTransport>("events").Consumers;
        await host.StartAsync(token);
        await host.Services.GetRequiredKeyedService<IConsumerControl>("events").StartAsync(token);
        await SendAsync(host, topic, 1, token);
        await Assert.That(await state.Handled.Reader.ReadAsync(token)).IsEqualTo(0L);

        using var expired = new CancellationTokenSource();
        await expired.CancelAsync();
        try { await host.StopAsync(expired.Token); }
        catch (Exception error) when (error is OperationCanceledException or AggregateException) { /* The wait expired; the drain continues. */ }
        await consumers[0].Completion.WaitAsync(token);
        await Assert.That(consumers[0].Completion.IsCompletedSuccessfully).IsTrue();
    }

    private async Task<string> CreateTopicAsync(string name, CancellationToken token)
    {
        await broker.Started.WaitAsync(token);
        var topic = TestContext.Current!.Isolation.GetIsolatedName(name).ToLowerInvariant();
        await using var admin = new AdminClientBuilder().WithBootstrapServers(broker.Bootstrap).Build();
        await admin.CreateTopicsAsync([new() { Name = topic, NumPartitions = 1, ReplicationFactor = 1 },
            new() { Name = topic + ".dead-letter", NumPartitions = 1, ReplicationFactor = 1 }], cancellationToken: token);
        return topic;
    }

    private async Task<long?> CommittedAsync(string topic, CancellationToken token)
    {
        await using var reader = new ConsumerBuilder<byte[], byte[]>().WithBootstrapServers(broker.Bootstrap).WithGroupId(topic)
            .WithOffsetCommitMode(OffsetCommitMode.Manual).WithAutoOffsetStore(false).Build();
        await reader.InitializeAsync(token);
        var offset = await reader.GetCommittedOffsetAsync(new(topic, 0), token);
        return offset is null or < 0 ? null : offset;
    }

    private static ValueTask SendAsync(IHost host, string topic, int number, CancellationToken token) =>
        host.Services.GetServices<ITransport>().Single(transport => transport.Name == "events").SendAsync(new(topic,
            Encoding.UTF8.GetBytes($"{{\"Number\":{number}}}"),
            new KeyValuePair<string, WireString>[] { new("ce_id", new("record-" + number)), new("ce_type", new("tests.kafka.receive;v=1")) }), token);

    private IHost BuildHost(string topic, KafkaReplayState state, bool autoStart)
    {
        var builder = Host.CreateApplicationBuilder();
        builder.Logging.ClearProviders();
        builder.Services.AddSingleton(state);
        builder.Services.AddRelayer(relayer =>
        {
            relayer.AddMessageTypesFromThisAssembly();
            relayer.UseJsonSerializer(KafkaReceiveJson.Default);
            relayer.Kafka("events", kafka =>
            {
                kafka.TopologyMode = TopologyMode.None;
                kafka.Options.Configure(options =>
                {
                    options.BootstrapServers.Add(broker.Bootstrap);
                    options.AutoStartConsumers = autoStart;
                    options.ConfigureConsumer = native => native.WithAutoOffsetReset(AutoOffsetReset.Earliest);
                });
                kafka.Consume(topic, consumer =>
                {
                    consumer.Options.Group = topic;
                    consumer.Handler<KafkaReplayHandler>();
                });
            });
        });
        return builder.Build();
    }
}
