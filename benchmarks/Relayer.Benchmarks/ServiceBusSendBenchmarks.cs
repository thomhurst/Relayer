using Azure.Messaging.ServiceBus;
using Azure.Messaging.ServiceBus.Administration;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Configs;
using Relayer.AzureServiceBus;

namespace Relayer.Benchmarks;

/// <summary>Acknowledged SDK sends to a fresh queue per case. Both paths include native message/header preparation.</summary>
[MemoryDiagnoser]
[CategoriesColumn]
[GroupBenchmarksBy(BenchmarkLogicalGroupRule.ByCategory)]
public class ServiceBusSendBenchmarks
{
    private ServiceBusClient _client = null!;
    private ServiceBusSender _raw = null!;
    private ServiceBusSendTransport _transport = null!;
    private ServiceBusAdministrationClient _admin = null!;
    private string _queue = null!;
    private byte[] _body = null!;
    private KeyValuePair<string, WireString>[] _headers = null!;
    private OutgoingMessage _message;
    private OutgoingMessage[] _batch = null!;

    [GlobalSetup]
    public async Task Setup()
    {
        var connection = Environment.GetEnvironmentVariable("RELAYER_SERVICEBUS_CONNECTION_STRING")
            ?? throw new InvalidOperationException("Set RELAYER_SERVICEBUS_CONNECTION_STRING to a test Service Bus namespace.");
        var administration = Environment.GetEnvironmentVariable("RELAYER_SERVICEBUS_ADMIN_CONNECTION_STRING") ?? connection;
        _admin = new(administration);
        _queue = "relayer-benchmark-" + Guid.NewGuid().ToString("N");
        await _admin.CreateQueueAsync(new CreateQueueOptions(_queue));
        _client = new(connection);
        _raw = _client.CreateSender(_queue);
        var options = new AzureServiceBusTransportOptions { Client = _client };
        options.Queues.Add(new(_queue));
        _transport = new("benchmark", _client, false, options, [_queue], TopologyMode.None);
        _body = "{\"orderId\":42,\"customer\":\"customer-1\"}"u8.ToArray();
        _headers = [new("cloudEvents:type", new("benchmarks.order;v=1")), new("cloudEvents:source", new("/benchmarks")),
            new("cloudEvents:specversion", new("1.0")), new("cloudEvents:id", new("019a3c1b-97c0-7000-8000-000000000001"u8.ToArray())),
            new("traceparent", new("00-0123456789abcdef0123456789abcdef-0123456789abcdef-01"u8.ToArray()))];
        _message = new(_queue, _body, _headers);
        _batch = Enumerable.Repeat(_message, 32).ToArray();
        await _raw.SendMessageAsync(NativeMessage());
        await _transport.SendAsync(_message, default);
    }

    private ServiceBusMessage NativeMessage()
    {
        var message = new ServiceBusMessage(_body);
        foreach (var header in _headers) message.ApplicationProperties[header.Key] = header.Value.ToString();
        return message;
    }

    [Benchmark(Baseline = true), BenchmarkCategory("Single")]
    public Task RawSingle() => _raw.SendMessageAsync(NativeMessage());

    [Benchmark, BenchmarkCategory("Single")]
    public ValueTask RelayerSingle() => _transport.SendAsync(_message, default);

    [Benchmark(Baseline = true, OperationsPerInvoke = 32), BenchmarkCategory("Batch")]
    public async Task RawBatch()
    {
        using var batch = await _raw.CreateMessageBatchAsync();
        for (var i = 0; i < 32; i++)
            if (!batch.TryAddMessage(NativeMessage())) throw new InvalidOperationException("The benchmark batch exceeds the native byte limit.");
        await _raw.SendMessagesAsync(batch);
    }

    [Benchmark(OperationsPerInvoke = 32), BenchmarkCategory("Batch")]
    public ValueTask RelayerBatch() => _transport.SendBatchAsync(_batch, default);

    [GlobalCleanup]
    public async Task Cleanup()
    {
        if (_transport is not null) await _transport.DisposeAsync();
        if (_raw is not null) await _raw.DisposeAsync();
        if (_client is not null) await _client.DisposeAsync();
        if (_admin is not null && _queue is not null) await _admin.DeleteQueueAsync(_queue);
    }
}
