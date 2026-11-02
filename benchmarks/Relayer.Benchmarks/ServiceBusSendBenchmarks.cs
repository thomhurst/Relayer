using System.Buffers;
using System.Data.Common;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using Microsoft.Extensions.Options;
using Azure.Messaging.ServiceBus;
using Azure.Messaging.ServiceBus.Administration;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Configs;
using Microsoft.Extensions.DependencyInjection;
using Relayer.AzureServiceBus;
using Relayer.Serialization;

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
    private ServiceProvider _provider = null!;
    private IServiceScope _scope = null!;
    private ISender _sender = null!;
    private IMessageSerializer _serializer = null!;
    private readonly ArrayBufferWriter<byte> _jsonBuffer = new();
    private readonly ServiceBusJsonOrder _order = new(42, "customer-1");
    private readonly MessageEnvelope _envelope = new(new("benchmarks.servicebus-json", 1), "/", new("cloudEvents:"));

    private RelayerInstrumentation _instrumentation = null!;
    private MessageTracing _tracing = null!;
    private MeterListener? _metrics;

    [Params(false, true)]
    public bool Metrics { get; set; }

    [GlobalSetup]
    public async Task Setup()
    {
        _instrumentation = new();
        _tracing = new(Options.Create(new TracingOptions()), [], instrumentation: _instrumentation);
        if (Metrics)
        {
            _metrics = new()
            {
                InstrumentPublished = (instrument, listener) =>
                {
                    if (ReferenceEquals(instrument.Meter.Scope, _instrumentation)) listener.EnableMeasurementEvents(instrument);
                },
            };
            _metrics.SetMeasurementEventCallback<long>(static (_, _, _, _) => { });
            _metrics.SetMeasurementEventCallback<double>(static (_, _, _, _) => { });
            _metrics.Start();
        }
        var connection = Environment.GetEnvironmentVariable("RELAYER_SERVICEBUS_CONNECTION_STRING")
            ?? throw new InvalidOperationException("Set RELAYER_SERVICEBUS_CONNECTION_STRING to a test Service Bus namespace.");
        var administration = Environment.GetEnvironmentVariable("RELAYER_SERVICEBUS_ADMIN_CONNECTION_STRING") ?? connection;
        _admin = new(administration);
        _queue = "relayer-benchmark-" + Guid.NewGuid().ToString("N");
        var queueOptions = new CreateQueueOptions(_queue)
        {
            DefaultMessageTimeToLive = TimeSpan.FromHours(1),
            DuplicateDetectionHistoryTimeWindow = TimeSpan.FromMinutes(10)
        };
        // The SDK's parsed emulator flag is internal; inspect the explicit connection-string option.
        var connectionOptions = new DbConnectionStringBuilder { ConnectionString = administration };
        if (connectionOptions.TryGetValue("UseDevelopmentEmulator", out var emulatorOption)
            && bool.TryParse(emulatorOption.ToString(), out var useEmulator) && useEmulator)
            queueOptions.MaxSizeInMegabytes = 100;
        await _admin.CreateQueueAsync(queueOptions);
        _client = new(connection);
        _raw = _client.CreateSender(_queue);
        var options = new AzureServiceBusTransportOptions { Client = _client };
        options.Queues.Add(new(_queue));
        _transport = new("benchmark", _client, false, options, [_queue], TopologyMode.None, tracing: _tracing);
        _body = "{\"orderId\":42,\"customer\":\"customer-1\"}"u8.ToArray();
        _headers = [new("cloudEvents:type", new("benchmarks.order;v=1")), new("cloudEvents:source", new("/benchmarks")),
            new("cloudEvents:specversion", new("1.0")), new("cloudEvents:id", new("019a3c1b-97c0-7000-8000-000000000001"u8.ToArray())),
            new("traceparent", new("00-0123456789abcdef0123456789abcdef-0123456789abcdef-01"u8.ToArray()))];
        _message = new(_queue, _body, _headers);
        _batch = Enumerable.Repeat(_message, 32).ToArray();
        var services = new ServiceCollection();
        services.AddSingleton(_instrumentation);
        services.AddRelayer(builder =>
        {
            builder.AddMessageTypesFromThisAssembly();
            builder.ServiceBus("benchmark-json", bus =>
            {
                bus.TopologyMode = TopologyMode.None;
                bus.Options.Configure(options => { options.Client = _client; options.Queues.Add(new(_queue)); });
                bus.Send<ServiceBusJsonOrder>(_queue);
            });
        });
        _provider = services.BuildServiceProvider();
        _scope = _provider.CreateScope();
        _sender = _scope.ServiceProvider.GetRequiredService<ISender>();
        _serializer = _provider.GetMessageSerializer("benchmark-json");
        await _raw.SendMessageAsync(NativeMessage());
        await _transport.SendAsync(_message, default);
        await RawJsonSingle();
        await RelayerJsonSingle();
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

    // Both paths serialize the same model and create a fresh CloudEvents envelope per send.
    // The raw path writes directly to SDK strings, without the transport-neutral UTF-8 buffer.
    [Benchmark(Baseline = true), BenchmarkCategory("DefaultJson")]
    public Task RawJsonSingle()
    {
        _jsonBuffer.Clear();
        _serializer.Serialize(_order, _jsonBuffer);
        var native = new ServiceBusMessage(_jsonBuffer.WrittenMemory) { ContentType = _serializer.ContentType };
        _envelope.Write<ServiceBusMessage, NativeHeaders>(native,
            MessageEnvelopeMetadata.Create(Activity.Current?.Context ?? default));
        return _raw.SendMessageAsync(native);
    }

    [Benchmark, BenchmarkCategory("DefaultJson")]
    public ValueTask RelayerJsonSingle() => _sender.SendAsync(_order);

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
        if (_scope is IAsyncDisposable asyncScope) await asyncScope.DisposeAsync();
        else _scope?.Dispose();
        if (_provider is not null) await _provider.DisposeAsync();
        if (_transport is not null) await _transport.DisposeAsync();
        if (_raw is not null) await _raw.DisposeAsync();
        if (_client is not null) await _client.DisposeAsync();
        if (_admin is not null && _queue is not null) await _admin.DeleteQueueAsync(_queue);
        _metrics?.Dispose();
        _tracing?.Dispose();
        _instrumentation?.Dispose();
    }

    private sealed class NativeHeaders : IMessageHeaderWriter<ServiceBusMessage>
    {
        public static void SetHeader(ServiceBusMessage message, string name, ReadOnlySpan<char> value) =>
            message.ApplicationProperties[name] = value.ToString();
        public static void RemoveHeader(ServiceBusMessage message, string name) => message.ApplicationProperties.Remove(name);
    }
}

[Message("benchmarks.servicebus-json")]
public sealed record ServiceBusJsonOrder(int OrderId, string Customer);
