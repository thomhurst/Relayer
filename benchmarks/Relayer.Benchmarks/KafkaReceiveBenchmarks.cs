using System.Text.Json;
using System.Text.Json.Serialization;
using BenchmarkDotNet.Attributes;
using Dekaf;
using Dekaf.Admin;
using Dekaf.Consumer;
using Dekaf.Serialization;
using Microsoft.Extensions.DependencyInjection;
using Relayer.Kafka;
using Relayer.Pipeline;
using Relayer.Serialization;
using Relayer.Transports;

namespace Relayer.Benchmarks;

/// <summary>Broker-backed Dekaf partitioned runtime, default JSON and runtime-managed commits, versus the same runtime with a raw handler.</summary>
/// <remarks>Each invocation replays one fixed 4096-record workload and stops the run once every record completes,
/// including the stop commit. Iteration setup seeks outside the measurement.</remarks>
[MemoryDiagnoser]
public class KafkaReceiveBenchmarks
{
    private const int Records = 4096;
    private IAdminClient _admin = null!;
    private IKafkaConsumer<byte[], byte[]> _raw = null!;
    private IKafkaConsumer<byte[], byte[]> _native = null!;
    private KafkaConsumer _adapter = null!;
    private ServiceProvider _provider = null!;
    private ReceivePipeline<KafkaDelivery, KafkaMessageBinding> _receive = null!;
    private PartitionedProcessingOptions _rawOptions = null!;
    private PartitionRecordProcessor<byte[], byte[]> _rawProcessor = null!;
    private CancellationTokenSource _run = null!;
    private int _count;
    private string _topic = null!;

    [Params(false, true)]
    public bool HealthEnabled
    {
        get; set;
    }

    [Params(32, 500)]
    public int MaxPollRecords
    {
        get; set;
    }

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

            for (var i = 0; i < Records; i++)
            {
                await producer.ProduceAsync(new()
                {
                    Topic = _topic,
                    Key = null,
                    Value = body,
                    Headers = headers
                });
            }
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
        var tracing = _provider.GetRequiredService<MessageTracing>();
        var endpoint = new MessagingEndpoint { System = "kafka", Destination = _topic, ConsumerGroup = _topic + "relayer" };
        var telemetry = _provider.GetRequiredService<RelayerInstrumentation>().CreateEndpoint(new()
        {
            System = endpoint.System,
            Destination = endpoint.Destination,
            ConsumerGroup = endpoint.ConsumerGroup,
        });
        var failures = new FailurePipeline(_provider, "benchmark", _topic, endpoint.ConsumerGroup,
            new()
            {
                DeadLetterEndpoint = _topic + ".dead-letter"
            }, KafkaHostedService.Copy);
        var pipeline = new ConsumePipelineBuilder(_provider.GetRequiredService<IServiceScopeFactory>(),
            _provider.GetRequiredService<MessageTypeRegistry>(), _provider.GetRequiredService<IMessageSerializer>(),
            failures, _provider.GetService<IFailureClassifier>())
            .Tracing(tracing, endpoint).Telemetry(telemetry)
            .Handler(_provider.GetRequiredService<HandlerRegistry>().GetHandler<KafkaBenchmarkPayload, KafkaBenchmarkHandler>()).Build(1);
        _receive = new ReceivePipeline<KafkaDelivery, KafkaMessageBinding>(pipeline, new(), 1);
        var health = HealthEnabled ? new KafkaWorkerHealth(TimeProvider.System) : null;
        _adapter = new(() => _native, ReceiveAsync, tracing, endpoint, KafkaProcessing.Defaults(1), 1, health);
        // The raw run uses the same native ordering, limits and commit policy without Relayer's observer.
        _rawOptions = new()
        {
            Ordering = PartitionedProcessingOrder.Key,
            MaxConcurrentHandlers = 1,
            CommitPolicy = PartitionCommitPolicy.CommitCompletedPeriodically,
            Monitor = HealthEnabled ? new PartitionedProcessingMonitor() : null,
        };
        _rawProcessor = RawAsync;
    }

    private async Task<IKafkaConsumer<byte[], byte[]>> BuildConsumer(string bootstrap, string suffix)
    {
        var consumer = new ConsumerBuilder<byte[], byte[]>().WithBootstrapServers(bootstrap).WithGroupId(_topic + suffix)
            .WithMaxPollRecords(MaxPollRecords).WithOffsetCommitMode(OffsetCommitMode.Manual).WithAutoOffsetStore(false).WithAutoOffsetReset(AutoOffsetReset.Earliest).Build();
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
    public Task<int> RawDekaf()
    {
        return RunAsync(_raw, _rawProcessor, _rawOptions);
    }

    [Benchmark(OperationsPerInvoke = Records)]
    public Task<int> Relayer()
    {
        return RunAsync(_native, _adapter.Processor, _adapter.Options);
    }

    private async Task<int> RunAsync(IKafkaConsumer<byte[], byte[]> consumer, PartitionRecordProcessor<byte[], byte[]> processor,
        PartitionedProcessingOptions options)
    {
        _count = 0;
        using var run = _run = new CancellationTokenSource(TimeSpan.FromSeconds(30));

        try
        {
            await consumer.RunPartitionedAsync(processor, options, run.Token);
        }
        catch (OperationCanceledException) when (_count >= Records)
        {
        }

        if (_count < Records)
        {
            throw new InvalidOperationException("Native run ended before all benchmark records.");
        }

        return _count;
    }

    private void Completed()
    {
        if (++_count == Records)
        {
            _run.Cancel();
        }
    }

    private async ValueTask RawAsync(PartitionRecordProcessorContext<byte[], byte[]> context, ConsumeResult<byte[], byte[]> record,
        CancellationToken cancellationToken)
    {
        await using var scope = _provider.CreateAsyncScope();
        var payload = JsonSerializer.Deserialize(record.Value, KafkaReceiveBenchmarkJson.Default.KafkaBenchmarkPayload);
        await scope.ServiceProvider.GetRequiredService<KafkaBenchmarkHandler>().HandleAsync(payload, null!, cancellationToken);
        Completed();
    }

    private async ValueTask<Disposition> ReceiveAsync(KafkaDelivery delivery, CancellationToken cancellationToken)
    {
        var disposition = await _receive.ProcessAsync(delivery, cancellationToken);
        Completed();
        return disposition;
    }

    [GlobalCleanup]
    public async Task Cleanup()
    {
        if (_adapter is not null)
        {
            await _adapter.DisposeAsync();
        }

        if (_raw is not null)
        {
            await _raw.DisposeAsync();
        }

        if (_provider is not null)
        {
            await _provider.DisposeAsync();
        }

        if (_admin is not null)
        {
            try
            {
                if (_topic is not null)
                {
                    await _admin.DeleteTopicsAsync([_topic]);
                }
            }
            finally
            {
                await _admin.DisposeAsync();
            }
        }
    }
}

[Message("benchmarks.kafka-receive")]
public readonly record struct KafkaBenchmarkPayload(int Value);

public sealed class KafkaBenchmarkHandler : IHandler<KafkaBenchmarkPayload>
{
    public ValueTask HandleAsync(KafkaBenchmarkPayload message, MessageContext context, CancellationToken cancellationToken)
    {
        return message.Value == 42 ? default : ValueTask.FromException(new InvalidOperationException("Unexpected benchmark payload."));
    }
}

[JsonSerializable(typeof(KafkaBenchmarkPayload))]
internal partial class KafkaReceiveBenchmarkJson : JsonSerializerContext;
