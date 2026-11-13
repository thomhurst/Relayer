using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Configs;
using Microsoft.Extensions.DependencyInjection;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using Relayer.RabbitMQ;
using Relayer.Transports;

namespace Relayer.Benchmarks;

/// <summary>Compares native confirmed publishing with the serialized boundary and full JSON sender on the same broker.</summary>
[MemoryDiagnoser]
[CategoriesColumn]
[GroupBenchmarksBy(BenchmarkLogicalGroupRule.ByCategory)]
public class RabbitMqSendBenchmarks
{
    private IConnection _connection = null!;
    private IChannel _channel = null!;
    private IConnection _consumerConnection = null!;
    private IChannel _consumerChannel = null!;
    private ServiceProvider _provider = null!;
    private ITransport _transport = null!;
    private BasicProperties _properties = null!;
    private OutgoingMessage _message;
    private OutgoingMessage[] _batch = null!;
    private readonly ValueTask[] _batchSends = new ValueTask[100];
    private BasicProperties[] _batchProperties = null!;
    private string _queue = null!;
    private RelayerInstrumentation _instrumentation = null!;
    private DirectJsonSend _json = null!;
    private readonly BasicProperties _jsonProperties = new() { Persistent = true, Headers = new Dictionary<string, object?>() };

    [GlobalSetup]
    public async Task Setup()
    {
        var uri = Environment.GetEnvironmentVariable("RELAYER_RABBITMQ_URI")
            ?? throw new InvalidOperationException("Set RELAYER_RABBITMQ_URI to a test RabbitMQ broker.");
        var factory = new ConnectionFactory { Uri = new Uri(uri) };
        _connection = await factory.CreateConnectionAsync();
        _channel = await _connection.CreateChannelAsync(new(true, true));
        _consumerConnection = await factory.CreateConnectionAsync();
        _consumerChannel = await _consumerConnection.CreateChannelAsync();
        _queue = "relayer-benchmark-" + Guid.NewGuid().ToString("N");
        await _consumerChannel.QueueDeclareAsync(_queue, false, true, true);
        // Consume both variants identically; do not grow an unbounded queue during measurement.
        await _consumerChannel.BasicConsumeAsync(_queue, true, new AsyncEventingBasicConsumer(_consumerChannel));
        var services = new ServiceCollection();
        services.AddRelayer(builder => builder.RabbitMQ("benchmark", rabbit =>
        {
            rabbit.Options.Configure(options => { options.ConnectionFactory = factory; options.PublisherCount = 1; });
            rabbit.Send<Payload>(_queue);
        }));
        _provider = services.BuildServiceProvider();
        _transport = _provider.GetRequiredService<ITransport>();
        await _transport.StartAsync(default);
        var id = new byte[128];
        "019a3c1b-97c0-7000-8000-000000000001"u8.CopyTo(id);
        KeyValuePair<string, WireString>[] headers = [new("cloudEvents:id", new(id.AsMemory(0, 36))),
            new("cloudEvents:type", new("benchmarks.order;v=1")), new("cloudEvents:source", new("/benchmarks")),
            new("cloudEvents:specversion", new("1.0")), new("content-type", new("application/json"))];
        _message = new(_queue, "{\"orderId\":42}"u8.ToArray(), headers);
        _properties = new() { Persistent = true, ContentType = "application/json", Headers = new Dictionary<string, object?>() };
        foreach (var header in headers)
            if (header.Key != "content-type") _properties.Headers[header.Key] = header.Value.Text ?? (object)header.Value.Utf8.ToArray();
        _batch = new OutgoingMessage[100];
        _batchProperties = new BasicProperties[100];
        for (var i = 0; i < _batch.Length; i++)
        {
            _batch[i] = _message;
            _batchProperties[i] = new(_properties) { Headers = new Dictionary<string, object?>(_properties.Headers!) };
        }
        _instrumentation = new();
        _json = new(_instrumentation, builder => builder.RabbitMQ("benchmark-json", rabbit =>
        {
            rabbit.TopologyMode = TopologyMode.None;
            rabbit.Options.Configure(options => { options.ConnectionFactory = factory; options.PublisherCount = 1; });
            rabbit.Send<DirectJsonOrder>(_queue);
        }), "cloudEvents:");
        await _json.StartAsync();
        await RawJsonSingle();
        await RelayerJsonSingle();
        await Raw();
        await Relayer();
    }

    [Benchmark(Baseline = true), BenchmarkCategory("Single")]
    public async Task Raw() => await _channel.BasicPublishAsync("", _queue, true, _properties, _message.Body);

    [Benchmark, BenchmarkCategory("Single")]
    public async Task Relayer() => await _transport.SendAsync(_message, default);

    [Benchmark(Baseline = true), BenchmarkCategory("DefaultJson")]
    public ValueTask RawJsonSingle()
    {
        var body = _json.Serialize();
        _jsonProperties.ContentType = _json.ContentType;
        _json.WriteEnvelope<BasicProperties, NativeHeaders>(_jsonProperties);
        return _channel.BasicPublishAsync("", _queue, true, _jsonProperties, body);
    }

    [Benchmark, BenchmarkCategory("DefaultJson")]
    public ValueTask RelayerJsonSingle() => _json.Sender.SendAsync(_json.Order);

    [Benchmark(Baseline = true, OperationsPerInvoke = 100), BenchmarkCategory("Batch")]
    public async Task RawBatch()
    {
        Exception? error = null;
        var admitted = 0;
        try
        {
            for (var i = 0; i < _batch.Length; i++)
            {
                _batchSends[admitted] = _channel.BasicPublishAsync("", _queue, true, _batchProperties[i], _batch[i].Body);
                admitted++;
            }
        }
        catch (Exception exception) { error = exception; }
        for (var i = 0; i < admitted; i++)
        {
            try { await _batchSends[i]; }
            catch (Exception exception) { error ??= exception; }
            _batchSends[i] = default;
        }
        if (error is not null) System.Runtime.ExceptionServices.ExceptionDispatchInfo.Throw(error);
    }

    [Benchmark(OperationsPerInvoke = 100), BenchmarkCategory("Batch")]
    public async Task RelayerBatch() => await _transport.SendBatchAsync(_batch, default);

    private sealed class NativeHeaders : IMessageHeaderWriter<BasicProperties>
    {
        public static void SetHeader(BasicProperties properties, string name, ReadOnlySpan<char> value) =>
            properties.Headers![name] = value.ToString();
        public static void RemoveHeader(BasicProperties properties, string name) => properties.Headers!.Remove(name);
    }

    [GlobalCleanup]
    public async Task Cleanup()
    {
        if (_json is not null) await _json.DisposeAsync();
        _instrumentation?.Dispose();
        if (_provider is not null) await _provider.DisposeAsync();
        if (_channel is not null) await _channel.DisposeAsync();
        if (_consumerChannel is not null) await _consumerChannel.DisposeAsync();
        if (_consumerConnection is not null) await _consumerConnection.DisposeAsync();
        if (_connection is not null) await _connection.DisposeAsync();
    }

    [Message("benchmarks.rabbitmq.payload")]
    public sealed record Payload(int OrderId);
}
