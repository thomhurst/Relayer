using Relayer.Transports;
using Relayer.Pipeline;
using System.Text.Json;
using System.Text.Json.Serialization;
using BenchmarkDotNet.Attributes;
using Dekaf;
using Dekaf.Admin;
using Dekaf.Consumer;
using Dekaf.Serialization;
using Microsoft.Extensions.DependencyInjection;
using Relayer.Kafka;
using Relayer.Serialization;

namespace Relayer.Benchmarks;

/// <summary>Broker-backed native fetch batches, default JSON and completed-offset commits, versus raw Dekaf.</summary>
/// <remarks>Each invocation replays one fixed 4096-record workload. Iteration setup seeks outside the measurement.</remarks>
[MemoryDiagnoser]
public class KafkaReceiveBenchmarks
{
    private const int Records = 4096;
    private IAdminClient _admin = null!;
    private IKafkaConsumer<byte[], byte[]> _raw = null!;
    private IKafkaConsumer<byte[], byte[]> _native = null!;
    private KafkaConsumer _adapter = null!;
    private KafkaWorkerHealth? _health;
    private ServiceProvider _provider = null!;
    private readonly TopicPartitionOffset[] _offset = new TopicPartitionOffset[1];
    private MessageTracing _tracing = null!;
    private string _topic = null!;

    [Params(false, true)]
    public bool HealthEnabled { get; set; }

    [GlobalSetup]
    public async Task Setup()
    {
        var bootstrap = Environment.GetEnvironmentVariable("RELAYER_KAFKA_BOOTSTRAP_SERVERS")
            ?? throw new InvalidOperationException("Set RELAYER_KAFKA_BOOTSTRAP_SERVERS to a test Kafka broker.");
        _topic = "relayer-receive-benchmark-" + Guid.NewGuid().ToString("N");
        _admin = new AdminClientBuilder().WithBootstrapServers(bootstrap).Build();
        await _admin.CreateTopicsAsync([new() { Name = _topic, NumPartitions = 1, ReplicationFactor = 1 }]);
        await using (var producer = await new ProducerBuilder<byte[], byte[]>().WithBootstrapServers(bootstrap).BuildAsync())
        {
            var headers = new Headers().Add("ce_id", "benchmark").Add("ce_type", "benchmarks.kafka-receive;v=1");
            var body = "{\"Value\":42}"u8.ToArray();
            for (var i = 0; i < Records; i++) await producer.ProduceAsync(new() { Topic = _topic, Key = null, Value = body, Headers = headers });
        }
        _raw = await BuildConsumer(bootstrap, "raw");
        _native = await BuildConsumer(bootstrap, "relayer");
        var services = new ServiceCollection();
        services.AddScoped<KafkaBenchmarkHandler>();
        services.AddRelayer(relayer =>
        {
            relayer.Infrastructure().MessageTypes.Add<KafkaBenchmarkPayload>("benchmarks.kafka-receive");
            relayer.UseJsonSerializer(KafkaReceiveBenchmarkJson.Default);

            relayer.Kafka("benchmark", kafka =>
            {
                kafka.Options.Configure(options => options.BootstrapServers.Add(bootstrap));
                kafka.Consume(_topic, consumer =>
                {
                    consumer.Options.Group = _topic + "relayer";
                    consumer.Handler<KafkaBenchmarkHandler>();
                });
            });
        });
        _provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true });
        _tracing = _provider.GetRequiredService<MessageTracing>();
        var endpoint = new MessagingEndpoint { System = "kafka", Destination = _topic, ConsumerGroup = _topic + "relayer" };
        var telemetry = _provider.GetRequiredService<RelayerInstrumentation>().CreateEndpoint(new()
        {
            System = endpoint.System, Destination = endpoint.Destination, ConsumerGroup = endpoint.ConsumerGroup,
        });
        var failures = new FailurePipeline(_provider, "benchmark", _topic, endpoint.ConsumerGroup,
            new() { DeadLetterEndpoint = _topic + ".dead-letter" }, KafkaHostedService.Copy);
        var pipeline = new ConsumePipelineBuilder(_provider.GetRequiredService<IServiceScopeFactory>(),
            _provider.GetRequiredService<MessageTypeRegistry>(), _provider.GetRequiredService<IMessageSerializer>(),
            failures, _provider.GetService<IFailureClassifier>())
            .Tracing(_tracing, endpoint).Telemetry(telemetry)
            .Handler(_provider.GetRequiredService<HandlerRegistry>().GetHandler<KafkaBenchmarkPayload, KafkaBenchmarkHandler>()).Build(1);
        var receive = new ReceivePipeline<KafkaDelivery, KafkaMessageBinding>(pipeline, new(), 1);
        _health = HealthEnabled ? new KafkaWorkerHealth(TimeProvider.System) : null;
        _adapter = new(_native, receive.ProcessAsync, _tracing, endpoint, new(), _health);
    }

    private async Task<IKafkaConsumer<byte[], byte[]>> BuildConsumer(string bootstrap, string suffix)
    {
        var consumer = new ConsumerBuilder<byte[], byte[]>().WithBootstrapServers(bootstrap).WithGroupId(_topic + suffix)
            .WithOffsetCommitMode(OffsetCommitMode.Manual).WithAutoOffsetStore(false).WithAutoOffsetReset(AutoOffsetReset.Earliest).Build();
        consumer.Assign(new TopicPartition(_topic, 0));
        await consumer.InitializeAsync();
        return consumer;
    }

    [IterationSetup]
    public void Rewind()
    {
        _raw.Seek(new(_topic, 0, 0));
        _native.Seek(new(_topic, 0, 0));
    }

    [Benchmark(Baseline = true, OperationsPerInvoke = Records)]
    public async Task<int> RawDekaf()
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        var count = 0;
        await foreach (var batch in _raw.ConsumeBatchAsync(timeout.Token))
        {
            foreach (var record in batch)
            {
                await using var scope = _provider.CreateAsyncScope();
                var payload = JsonSerializer.Deserialize(record.Value, KafkaReceiveBenchmarkJson.Default.KafkaBenchmarkPayload);
                await scope.ServiceProvider.GetRequiredService<KafkaBenchmarkHandler>().HandleAsync(payload, null!, timeout.Token);
                count++;
            }
            if (batch.TryGetNextOffset(out var checkpoint))
            {
                _offset[0] = checkpoint;
                await _raw.CommitAsync(_offset, timeout.Token);
            }
            if (count >= Records) return count;
        }
        throw new InvalidOperationException("Native stream ended before all benchmark records.");
    }

    [Benchmark(OperationsPerInvoke = Records)]
    public async Task<int> Relayer()
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        var count = 0;
        await foreach (var batch in _native.ConsumeBatchAsync(timeout.Token))
        {
            _health?.Progress();
            count += await _adapter.ProcessBatchAsync(batch, timeout.Token);
            _health?.Idle();
            if (count >= Records) return count;
        }
        throw new InvalidOperationException("Native stream ended before all benchmark records.");
    }

    [GlobalCleanup]
    public async Task Cleanup()
    {
        if (_adapter is not null) await _adapter.DisposeAsync();
        if (_raw is not null) await _raw.DisposeAsync();
        if (_provider is not null) await _provider.DisposeAsync();
        if (_admin is not null)
        {
            try { if (_topic is not null) await _admin.DeleteTopicsAsync([_topic]); }
            finally { await _admin.DisposeAsync(); }
        }
    }
}

[Message("benchmarks.kafka-receive")]
public readonly record struct KafkaBenchmarkPayload(int Value);
public sealed class KafkaBenchmarkHandler : IHandler<KafkaBenchmarkPayload>
{
    public ValueTask HandleAsync(KafkaBenchmarkPayload message, MessageContext context, CancellationToken cancellationToken)
        => message.Value == 42 ? default : ValueTask.FromException(new InvalidOperationException("Unexpected benchmark payload."));
}
[JsonSerializable(typeof(KafkaBenchmarkPayload))]
internal partial class KafkaReceiveBenchmarkJson : JsonSerializerContext;
