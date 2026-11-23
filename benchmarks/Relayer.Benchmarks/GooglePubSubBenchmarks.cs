using System.Buffers;
using System.Diagnostics.Metrics;
using System.Text.Json.Serialization;
using BenchmarkDotNet.Attributes;
using Google.Cloud.PubSub.V1;
using Google.Protobuf;
using Grpc.Net.Client;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Relayer.GooglePubSub;
using Relayer.Pipeline;
using Relayer.Serialization;
using Relayer.Transports;

namespace Relayer.Benchmarks;

/// <summary>Native acknowledged publishes versus the full direct send path, with matching JSON and CloudEvents preparation.</summary>
[MemoryDiagnoser]
public class GooglePubSubSendBenchmarks
{
    private GrpcChannel _channel = null!;
    private PublisherServiceApiClient _api = null!;
    private PublisherClient _publisher = null!;
    private IHost _host = null!;
    private IServiceScope _scope = null!;
    private ISender _sender = null!;
    private IMessageSerializer _serializer = null!;
    private MeterListener? _durationListener;
    private string _topic = null!;
    private readonly ArrayBufferWriter<byte> _buffer = new();
    private readonly PubSubBenchmarkOrder _order = new(42);
    private readonly MessageEnvelope _envelope = new(new("benchmarks.pubsub.order", 1), "/", new("ce-"));

    [ParamsAllValues]
    public bool RecordPublishDuration
    {
        get; set;
    }

    [GlobalSetup]
    public async Task Setup()
    {
        var address = Environment.GetEnvironmentVariable("PUBSUB_EMULATOR_HOST") ?? throw new InvalidOperationException("Set PUBSUB_EMULATOR_HOST to a Pub/Sub emulator.");
        _channel = GrpcChannel.ForAddress("http://" + address);
        _api = new PublisherServiceApiClientImpl(new Publisher.PublisherClient(_channel), null, null);
        var subscriber = new SubscriberServiceApiClientImpl(new Subscriber.SubscriberClient(_channel), null, null);
        _topic = TopicName.FromProjectTopic("relayer-test", "benchmark-" + Guid.NewGuid().ToString("N")).ToString();
        var settings = new PublisherClient.Settings { BatchingSettings = new(1, null, null) };
        _publisher = new PublisherClientImpl(TopicName.Parse(_topic), [_api], settings, static () => Task.CompletedTask);
        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddRelayer(relayer =>
        {
            relayer.AddMessageTypesFromThisAssembly();
            relayer.UseJsonSerializer(PubSubBenchmarkJson.Default);
            relayer.GooglePubSub("benchmark", transport =>
            {
                transport.TopologyMode = TopologyMode.Provision;
                transport.Options.Configure(options =>
                {
                    options.Publisher = _api;
                    options.Subscriber = subscriber;
                    options.PublisherSettings = settings;
                    options.Topics.Add(TopicName.Parse(_topic));
                });
                transport.Send<PubSubBenchmarkOrder>(_topic);
            });
        });
        _host = builder.Build();
        await _host.StartAsync();
        _scope = _host.Services.CreateScope();
        _sender = _scope.ServiceProvider.GetRequiredService<ISender>();
        _serializer = _host.Services.GetMessageSerializer("benchmark");

        if (RecordPublishDuration)
        {
            var instrumentation = _host.Services.GetRequiredService<RelayerInstrumentation>();
            _durationListener = new()
            {
                InstrumentPublished = (instrument, listener) =>
                {
                    if (ReferenceEquals(instrument.Meter.Scope, instrumentation) && instrument.Name == "messaging.client.operation.duration")
                    {
                        listener.EnableMeasurementEvents(instrument);
                    }
                }
            };
            _durationListener.SetMeasurementEventCallback<double>(static (_, _, _, _) =>
            {
            });
            _durationListener.Start();
        }

        await NativeJsonSend();
        await RelayerJsonSend();
    }

    [Benchmark(Baseline = true)]
    public Task<string> NativeJsonSend()
    {
        _buffer.Clear();
        _serializer.Serialize(_order, _buffer);
        var message = new PubsubMessage { Data = UnsafeByteOperations.UnsafeWrap(_buffer.WrittenMemory) };
        message.Attributes["content-type"] = _serializer.ContentType;
        _envelope.Write<PubsubMessage, Headers>(message, MessageEnvelopeMetadata.Create(default));
        return _publisher.PublishAsync(message);
    }

    [Benchmark]
    public ValueTask RelayerJsonSend()
    {
        return _sender.SendWithoutOutboxAsync(_order);
    }

    [GlobalCleanup]
    public async Task Cleanup()
    {
        _durationListener?.Dispose();
        _scope?.Dispose();

        if (_host is not null)
        {
            await _host.StopAsync();
            await ((IAsyncDisposable)_host).DisposeAsync();
        }

        if (_publisher is not null)
        {
            await _publisher.DisposeAsync();
        }

        if (_topic is not null && _api is not null)
        {
            await _api.DeleteTopicAsync(_topic);
        }

        _channel?.Dispose();
    }

    private readonly struct Headers : IMessageHeaderWriter<PubsubMessage>
    {
        public static void SetHeader(PubsubMessage message, string name, ReadOnlySpan<char> value)
        {
            message.Attributes[name] = value.ToString();
        }

        public static void RemoveHeader(PubsubMessage message, string name)
        {
            message.Attributes.Remove(name);
        }
    }
}

/// <summary>Alternating-topic native and Relayer batches with identical payloads and publisher settings.</summary>
[MemoryDiagnoser]
public class GooglePubSubMixedBatchBenchmarks
{
    private GrpcChannel _channel = null!;
    private ServiceProvider _provider = null!;
    private PublisherServiceApiClient _api = null!;
    private ITransport _transport = null!;
    private TopicName _firstTopic = null!;
    private TopicName _secondTopic = null!;
    private PublisherClient _firstPublisher = null!;
    private PublisherClient _secondPublisher = null!;
    private OutgoingMessage[] _messages = null!;
    private PubsubMessage[] _nativeMessages = null!;

    [GlobalSetup]
    public async Task Setup()
    {
        var address = Environment.GetEnvironmentVariable("PUBSUB_EMULATOR_HOST")
            ?? throw new InvalidOperationException("Set PUBSUB_EMULATOR_HOST to a Pub/Sub emulator.");
        _channel = GrpcChannel.ForAddress("http://" + address);
        _api = new PublisherServiceApiClientImpl(new Publisher.PublisherClient(_channel), null, null);
        var subscriber = new SubscriberServiceApiClientImpl(new Subscriber.SubscriberClient(_channel), null, null);
        var topicPrefix = "mixed-batch-" + Guid.NewGuid().ToString("N");
        _firstTopic = TopicName.FromProjectTopic("relayer-test", topicPrefix + "-first");
        _secondTopic = TopicName.FromProjectTopic("relayer-test", topicPrefix + "-second");
        var services = new ServiceCollection();
        services.AddRelayer(relayer => relayer.GooglePubSub("benchmark", transport =>
        {
            transport.Options.Configure(options =>
            {
                options.Publisher = _api;
                options.Subscriber = subscriber;
                // A delay also lets the benchmark run on an adapter that serializes destination runs.
                options.PublisherSettings.BatchingSettings = new(2, null, TimeSpan.FromMilliseconds(10));
                options.Topics.Add(_firstTopic);
                options.Topics.Add(_secondTopic);
            });
        }));
        _provider = services.BuildServiceProvider();
        _transport = _provider.GetRequiredService<ITransport>();
        await _transport.ApplyTopologyAsync(TopologyMode.Provision, default);
        var firstDestination = _firstTopic.ToString();
        var secondDestination = _secondTopic.ToString();
        var publishers = (IGooglePubSubClients)_transport;
        _firstPublisher = publishers.PublisherFor(firstDestination);
        _secondPublisher = publishers.PublisherFor(secondDestination);
        byte[] body = [1, 2, 3, 4];
        _messages = [new(firstDestination, body), new(secondDestination, body), new(firstDestination, body), new(secondDestination, body)];
        _nativeMessages = Array.ConvertAll(_messages,
            static message => new PubsubMessage { Data = UnsafeByteOperations.UnsafeWrap(message.Body) });
    }

    [Benchmark(Baseline = true)]
    public async ValueTask NativeMixedBatch()
    {
        var first = _firstPublisher.PublishAsync(_nativeMessages[0]);
        var second = _secondPublisher.PublishAsync(_nativeMessages[1]);
        var third = _firstPublisher.PublishAsync(_nativeMessages[2]);
        var fourth = _secondPublisher.PublishAsync(_nativeMessages[3]);
        await first;
        await second;
        await third;
        await fourth;
    }

    [Benchmark]
    public ValueTask RelayerMixedBatch()
    {
        return _transport.SendBatchAsync(_messages, default);
    }

    [GlobalCleanup]
    public async Task Cleanup()
    {
        await _provider.DisposeAsync();
        await _api.DeleteTopicAsync(_firstTopic);
        await _api.DeleteTopicAsync(_secondTopic);
        _channel.Dispose();
    }
}

/// <summary>Already-delivered native messages: both paths deserialize JSON and resolve the same scoped handler.</summary>
[MemoryDiagnoser]
public class GooglePubSubReceiveBenchmarks
{
    private ServiceProvider _provider = null!;
    private IMessageSerializer _serializer = null!;
    private ReceivePipeline<GooglePubSubDelivery, GooglePubSubBinding> _receive = null!;
    private GooglePubSubDelivery _delivery;
    private ReadOnlySequence<byte> _body;

    [GlobalSetup]
    public void Setup()
    {
        var services = new ServiceCollection();
        services.AddRelayer(relayer =>
        {
            relayer.AddMessageTypesFromThisAssembly();
            relayer.UseJsonSerializer(PubSubBenchmarkJson.Default);
            new ConsumerBuilder<object>(relayer.Services, relayer.Infrastructure().Handlers, "manual").Handler<PubSubBenchmarkHandler>();
        });
        _provider = services.BuildServiceProvider();
        _serializer = _provider.GetRequiredService<IMessageSerializer>();
        var message = new PubsubMessage { Data = ByteString.CopyFromUtf8("{\"Value\":42}") };
        message.Attributes["ce-type"] = "benchmarks.pubsub.order;v=1";
        message.Attributes["ce-id"] = "benchmark";
        message.Attributes["ce-source"] = "/";
        message.Attributes["ce-specversion"] = "1.0";
        _delivery = new(message, null!);
        _body = new(message.Data.Memory);
        var pipeline = new ConsumePipelineBuilder(_provider.GetRequiredService<IServiceScopeFactory>(),
            _provider.GetRequiredService<MessageTypeRegistry>(), _serializer, static (_, error, _) => ValueTask.FromException(error))
            .Handler(_provider.GetRequiredService<HandlerRegistry>().GetHandler<PubSubBenchmarkOrder, PubSubBenchmarkHandler>()).Build();
        _receive = new(pipeline, new("ce-"));
    }

    [Benchmark(Baseline = true)]
    public async ValueTask NativeJsonReceive()
    {
        await using var scope = _provider.CreateAsyncScope();
        var message = _serializer.Deserialize<PubSubBenchmarkOrder>(_body);
        await scope.ServiceProvider.GetRequiredService<PubSubBenchmarkHandler>().HandleAsync(message, null!, default).ConfigureAwait(false);
    }

    [Benchmark]
    public ValueTask<Disposition> RelayerJsonReceive()
    {
        return _receive.ProcessAsync(_delivery);
    }

    [GlobalCleanup]
    public ValueTask Cleanup()
    {
        return _provider.DisposeAsync();
    }
}

[Message("benchmarks.pubsub.order")]
public sealed record PubSubBenchmarkOrder(int Value);

public sealed class PubSubBenchmarkHandler : IHandler<PubSubBenchmarkOrder>
{
    public ValueTask HandleAsync(PubSubBenchmarkOrder message, MessageContext context, CancellationToken cancellationToken)
    {
        return default;
    }
}

[JsonSerializable(typeof(PubSubBenchmarkOrder))]
internal partial class PubSubBenchmarkJson : JsonSerializerContext;
