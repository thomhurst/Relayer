using BenchmarkDotNet.Attributes;
using Microsoft.Extensions.DependencyInjection;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using Relayer.RabbitMQ;
using Relayer.Transports;

namespace Relayer.Benchmarks;

/// <summary>Compares native confirmed publishing with the serialized Relayer send boundary on the same broker.</summary>
[MemoryDiagnoser]
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
    private string _queue = null!;

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
        await Raw();
        await Relayer();
    }

    [Benchmark(Baseline = true)]
    public async Task Raw() => await _channel.BasicPublishAsync("", _queue, true, _properties, _message.Body);

    [Benchmark]
    public async Task Relayer() => await _transport.SendAsync(_message, default);

    [GlobalCleanup]
    public async Task Cleanup()
    {
        if (_provider is not null) await _provider.DisposeAsync();
        if (_channel is not null) await _channel.DisposeAsync();
        if (_consumerChannel is not null) await _consumerChannel.DisposeAsync();
        if (_consumerConnection is not null) await _consumerConnection.DisposeAsync();
        if (_connection is not null) await _connection.DisposeAsync();
    }

    [Message("benchmarks.rabbitmq.payload")]
    public sealed record Payload(int OrderId);
}
