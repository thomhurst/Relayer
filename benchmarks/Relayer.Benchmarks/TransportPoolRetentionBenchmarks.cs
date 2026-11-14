using BenchmarkDotNet.Attributes;
using Relayer.AmazonSqs;
using Relayer.AzureServiceBus;
using Relayer.Kafka;
using Relayer.RabbitMQ;

namespace Relayer.Benchmarks;

/// <summary>Warmed real transport pools, including repeated overflow bursts.</summary>
[MemoryDiagnoser]
public class TransportPoolRetentionBenchmarks
{
    [Params(1, 256, 512)]
    public int Concurrency { get; set; }

    private readonly KafkaSendPool _kafka = new();
    private KafkaSendOperation[] _kafkaItems = null!;
    private readonly ServiceBusPool<ServiceBusSendState> _serviceBus = new(static () => new());
    private ServiceBusSendState[] _serviceBusItems = null!;
    private readonly RabbitMqSendPool _rabbitMq = new();
    private RabbitMqSendOperation[] _rabbitMqItems = null!;
    private readonly SqsOperationPool _sqs = new();
    private SqsOperationState[] _sqsItems = null!;
    private readonly SnsOperationPool _sns = new();
    private SnsOperationState[] _snsItems = null!;

    [GlobalSetup]
    public void Setup()
    {
        _kafkaItems = new KafkaSendOperation[Concurrency];
        Kafka();
        _serviceBusItems = new ServiceBusSendState[Concurrency];
        ServiceBus();
        _rabbitMqItems = new RabbitMqSendOperation[Concurrency];
        RabbitMq();
        _sqsItems = new SqsOperationState[Concurrency];
        Sqs();
        _snsItems = new SnsOperationState[Concurrency];
        Sns();
    }

    [Benchmark]
    public void Kafka()
    {
        for (var i = 0; i < Concurrency; i++) _kafkaItems[i] = _kafka.Rent(1);
        for (var i = 0; i < Concurrency; i++)
        {
            _kafka.Return(_kafkaItems[i]);
            _kafkaItems[i] = null!;
        }
    }

    [Benchmark]
    public void ServiceBus()
    {
        for (var i = 0; i < Concurrency; i++) _serviceBusItems[i] = _serviceBus.Rent();
        for (var i = 0; i < Concurrency; i++)
        {
            _serviceBus.Return(_serviceBusItems[i]);
            _serviceBusItems[i] = null!;
        }
    }

    [Benchmark]
    public void RabbitMq()
    {
        for (var i = 0; i < Concurrency; i++) _rabbitMqItems[i] = _rabbitMq.Rent();
        for (var i = 0; i < Concurrency; i++)
        {
            _rabbitMq.Return(_rabbitMqItems[i]);
            _rabbitMqItems[i] = null!;
        }
    }

    [Benchmark]
    public void Sqs()
    {
        for (var i = 0; i < Concurrency; i++) _sqsItems[i] = _sqs.Rent();
        for (var i = 0; i < Concurrency; i++)
        {
            _sqs.Return(_sqsItems[i]);
            _sqsItems[i] = null!;
        }
    }

    [Benchmark]
    public void Sns()
    {
        for (var i = 0; i < Concurrency; i++) _snsItems[i] = _sns.Rent();
        for (var i = 0; i < Concurrency; i++)
        {
            _sns.Return(_snsItems[i]);
            _snsItems[i] = null!;
        }
    }
}
