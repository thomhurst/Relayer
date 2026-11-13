using System.Buffers;
using System.Runtime.CompilerServices;
using System.Text.Json.Serialization;
using BenchmarkDotNet.Attributes;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using NATS.Client.Core;
using NATS.Client.JetStream;
using NATS.Client.JetStream.Models;
using Relayer.Nats;
using Relayer.Serialization;
using Relayer.Transports;

namespace Relayer.Benchmarks;

/// <summary>Confirmed native publishes versus the full JSON and CloudEvents direct send path.</summary>
[MemoryDiagnoser]
public class NatsSendBenchmarks
{
    private NatsConnection _connection = null!;
    private NatsJSContext _js = null!;
    private IHost _host = null!;
    private IServiceScope _scope = null!;
    private ISender _sender = null!;
    private ITransport _transport = null!;
    private IMessageSerializer _serializer = null!;
    private string _stream = null!;
    private readonly ArrayBufferWriter<byte> _buffer = new();
    private readonly NativeHeaders _headers = new();
    private readonly SendHeaders _adapterHeaders = new();
    private readonly CancellationTokenSource _nativeTimeout = new();
    private readonly NatsBenchmarkOrder _order = new(42);
    private readonly MessageEnvelope _envelope = new(new("benchmarks.nats.order", 1), "/", new("ce-"));

    [GlobalSetup]
    public async Task Setup()
    {
        var options = new NatsOpts { Url = Environment.GetEnvironmentVariable("NATS_URL") ?? "nats://localhost:4222" };
        _connection = new(options);
        _js = new(_connection);
        _stream = "benchmark_" + Guid.NewGuid().ToString("N");
        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddRelayer(relayer =>
        {
            relayer.AddMessageTypesFromThisAssembly();
            relayer.UseJsonSerializer(NatsBenchmarkJson.Default);
            relayer.Nats("benchmark", transport =>
            {
                transport.TopologyMode = TopologyMode.Provision;
                transport.Options.Configure(settings =>
                {
                    settings.ConnectionOptions = options;
                    settings.Streams.Add(new(_stream) { Configuration = new(_stream, [_stream]) { Storage = StreamConfigStorage.Memory, MaxMsgs = 1000 } });
                });
                transport.Send<NatsBenchmarkOrder>(_stream);
            });
        });
        _host = builder.Build();
        await _host.StartAsync();
        _scope = _host.Services.CreateScope();
        _sender = _scope.ServiceProvider.GetRequiredService<ISender>();
        _transport = _host.Services.GetServices<ITransport>().Single(transport => transport.Name == "benchmark");
        _serializer = _host.Services.GetMessageSerializer("benchmark");
        await NativeJsonSend();
        await RelayerJsonSend();
        await AdapterJsonSend();
    }
    // Exclude benchmark-owned continuation boxes from native-client allocation attribution.
    // Both wrappers use the same pooled builder as the transport under measurement.
    [Benchmark(Baseline = true)]
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    public async ValueTask NativeJsonSend()
    {
        _buffer.Clear();
        _headers.Clear();
        _serializer.Serialize(_order, _buffer);
        _headers["content-type"] = _serializer.ContentType;
        // Native NatsHeaders requires strings for changing CloudEvents attributes on both paths (D52).
        _envelope.Write<NatsHeaders, Headers>(_headers, MessageEnvelopeMetadata.Create(default));
        // Match the cancelable route deadline: native clients register cancellation only for cancelable tokens.
        _nativeTimeout.CancelAfter(TimeSpan.FromSeconds(30));
        try
        {
            var ack = await _js.PublishAsync(_stream, _buffer.WrittenMemory, _headers,
                headers: _headers, cancellationToken: _nativeTimeout.Token);
            ack.EnsureSuccess();
        }
        finally
        {
            _nativeTimeout.CancelAfter(Timeout.InfiniteTimeSpan);
            if (!_nativeTimeout.TryReset()) throw new InvalidOperationException("Native benchmark deadline expired.");
        }
    }
    [Benchmark]
    public ValueTask RelayerJsonSend() => _sender.SendNowAsync(_order);
    // Isolate native invocation/header conversion from the full sender's route and continuation.
    // Use the same UTF-8 header storage as the full sender, native serializer, JSON and deadline.
    // Pool the diagnostic continuation too; an ordinary builder adds a per-call benchmark box.
    // This diagnostic is not a replacement for the end-to-end acceptance comparison above.
    [Benchmark]
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    public async ValueTask AdapterJsonSend()
    {
        _buffer.Clear();
        _adapterHeaders.Clear();
        _serializer.Serialize(_order, _buffer);
        _envelope.Write<SendHeaders, SendHeaders>(_adapterHeaders, MessageEnvelopeMetadata.Create(default));
        _adapterHeaders.Add("content-type", new WireString(_serializer.ContentType));
        _nativeTimeout.CancelAfter(TimeSpan.FromSeconds(30));
        try
        {
            await _transport.SendAsync(new OutgoingMessage(_stream, _buffer.WrittenMemory, _adapterHeaders.Written)
                .WithTracingStarted(), _nativeTimeout.Token);
        }
        finally
        {
            _nativeTimeout.CancelAfter(Timeout.InfiniteTimeSpan);
            if (!_nativeTimeout.TryReset()) throw new InvalidOperationException("Adapter benchmark deadline expired.");
        }
    }
    [GlobalCleanup]
    public async Task Cleanup()
    {
        _scope?.Dispose();
        _nativeTimeout.Dispose();
        if (_host is not null)
        {
            await _host.StopAsync();
            await ((IAsyncDisposable)_host).DisposeAsync();
        }
        if (_js is not null) await _js.DeleteStreamAsync(_stream);
        if (_connection is not null) await _connection.DisposeAsync();
    }
    // A raw caller preserving stored creation context must use the same public native hook:
    // native instrumentation injects its own context before serialization. No Relayer send code is called.
    private sealed class NativeHeaders() : NatsHeaders(16), INatsSerializeWithContext<ReadOnlyMemory<byte>>
    {
        public void Serialize(IBufferWriter<byte> writer, ReadOnlyMemory<byte> body) =>
            NatsRawSerializer<ReadOnlyMemory<byte>>.Default.Serialize(writer, body);
        public void Serialize(IBufferWriter<byte> writer, ReadOnlyMemory<byte> body, in NatsMsgContext context)
        {
            context.Headers!.Remove("traceparent");
            context.Headers.Remove("tracestate");
            Serialize(writer, body);
        }
    }
    private readonly struct Headers : IMessageHeaderWriter<NatsHeaders>
    {
        public static void SetHeader(NatsHeaders message, string name, ReadOnlySpan<char> value) => message[name] = value.ToString();
        public static void RemoveHeader(NatsHeaders message, string name) => message.Remove(name);
    }
}

/// <summary>Naturally awaited send loops, alongside the single-call synchronous-driver comparisons above.</summary>
[MemoryDiagnoser]
public class NatsAsyncSendBenchmarks
{
    private const int Operations = 256;
    private readonly NatsSendBenchmarks _send = new();

    [GlobalSetup]
    public Task Setup() => _send.Setup();

    // BenchmarkDotNet blocks its caller for each ValueTask invocation. Waiting inside a sequence
    // exercises an asynchronous caller instead, so pooled inner transport state can return to the
    // worker cache before the next send. Keep the original single-call cases: their pool misses
    // are real for a synchronously driven caller and must remain visible, not be subtracted away.
    // Each operation still awaits one native confirmed publish; this is no broker-side batching.
    [Benchmark(Baseline = true, OperationsPerInvoke = Operations)]
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    public async ValueTask NativeJsonSend()
    {
        for (var i = 0; i < Operations; i++) await _send.NativeJsonSend();
    }

    [Benchmark(OperationsPerInvoke = Operations)]
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    public async ValueTask RelayerJsonSend()
    {
        for (var i = 0; i < Operations; i++) await _send.RelayerJsonSend();
    }

    [Benchmark(OperationsPerInvoke = Operations)]
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    public async ValueTask AdapterJsonSend()
    {
        for (var i = 0; i < Operations; i++) await _send.AdapterJsonSend();
    }

    [GlobalCleanup]
    public Task Cleanup() => _send.Cleanup();
}

/// <summary>Already-delivered native messages: both paths deserialize JSON and resolve the same scoped handler.</summary>
[MemoryDiagnoser]
public class NatsReceiveBenchmarks
{
    private ServiceProvider _provider = null!;
    private IMessageSerializer _serializer = null!;
    private ReceivePipeline<NatsDelivery, NatsBinding> _receive = null!;
    private NatsDelivery _delivery;
    private ReadOnlySequence<byte> _body;

    [GlobalSetup]
    public void Setup()
    {
        var services = new ServiceCollection();
        services.AddRelayer(relayer =>
        {
            relayer.AddMessageTypesFromThisAssembly();
            relayer.UseJsonSerializer(NatsBenchmarkJson.Default);
            new ConsumerBuilder<object>(relayer.Services, relayer.Handlers, "manual").Handler<NatsBenchmarkHandler>();
        });
        _provider = services.BuildServiceProvider();
        _serializer = _provider.GetRequiredService<IMessageSerializer>();
        var headers = new NatsHeaders();
        headers["ce-type"] = "benchmarks.nats.order;v=1";
        headers["ce-id"] = "benchmark";
        headers["ce-source"] = "/";
        headers["ce-specversion"] = "1.0";
        var message = new NatsJSMsg<ReadOnlyMemory<byte>>(new NatsMsg<ReadOnlyMemory<byte>>("orders", null, 0, headers,
            "{\"Value\":42}"u8.ToArray(), null), null!);
        _delivery = new(message, null!);
        _body = new(message.Data);
        var pipeline = new ConsumePipelineBuilder(_provider.GetRequiredService<IServiceScopeFactory>(),
            _provider.GetRequiredService<MessageTypeRegistry>(), _serializer, static (_, error, _) => ValueTask.FromException(error))
            .Handler(_provider.GetRequiredService<HandlerRegistry>().GetHandler<NatsBenchmarkOrder, NatsBenchmarkHandler>()).Build();
        _receive = new(pipeline, new("ce-"));
    }

    [Benchmark(Baseline = true)]
    public async ValueTask NativeJsonReceive()
    {
        await using var scope = _provider.CreateAsyncScope();
        var message = _serializer.Deserialize<NatsBenchmarkOrder>(_body);
        await scope.ServiceProvider.GetRequiredService<NatsBenchmarkHandler>().HandleAsync(message, null!, default).ConfigureAwait(false);
    }
    [Benchmark]
    public ValueTask<Disposition> RelayerJsonReceive() => _receive.ProcessAsync(_delivery);
    [GlobalCleanup]
    public ValueTask Cleanup() => _provider.DisposeAsync();
}

[Message("benchmarks.nats.order")]
public sealed record NatsBenchmarkOrder(int Value);
public sealed class NatsBenchmarkHandler : IHandler<NatsBenchmarkOrder>
{
    public ValueTask HandleAsync(NatsBenchmarkOrder message, MessageContext context, CancellationToken cancellationToken) => default;
}
[JsonSerializable(typeof(NatsBenchmarkOrder))]
internal partial class NatsBenchmarkJson : JsonSerializerContext;

/// <summary>Real native pull/JSON/confirmed-ACK paths; message publication is outside the measurement.</summary>
[MemoryDiagnoser]
public class NatsWireReceiveBenchmarks
{
    // IterationSetup disables BenchmarkDotNet's automatic invocation-count calibration.
    // 128 confirmed ACKs took only ~35 ms on the hosted runner, below its 100 ms
    // minimum recommendation. Keep enough work in each sample to amortize scheduling noise.
    private const int Count = 4096;
    // Keep native pull flow control independent of measurement length.
    private const int PullBatchSize = 128;
    private NatsConnection _connection = null!;
    private NatsJSContext _js = null!;
    private INatsJSConsumer _consumer = null!;
    private IAsyncEnumerator<INatsJSMsg<ReadOnlyMemory<byte>>> _messages = null!;
    private ServiceProvider _provider = null!;
    private IMessageSerializer _serializer = null!;
    private ReceivePipeline<NatsDelivery, NatsBinding> _receive = null!;
    private string _stream = null!;
    private readonly NatsHeaders _headers = new()
    {
        ["ce-type"] = "benchmarks.nats.order;v=1", ["ce-id"] = "benchmark",
        ["ce-source"] = "/", ["ce-specversion"] = "1.0",
    };
    private readonly byte[] _body = "{\"Value\":42}"u8.ToArray();

    [GlobalSetup]
    public async Task Setup()
    {
        _connection = new(new NatsOpts { Url = Environment.GetEnvironmentVariable("NATS_URL") ?? "nats://localhost:4222" });
        _js = new(_connection);
        _stream = "receive_" + Guid.NewGuid().ToString("N");
        await _js.CreateStreamAsync(new(_stream, [_stream]) { Storage = StreamConfigStorage.Memory });
        _consumer = await _js.CreateConsumerAsync(_stream, new ConsumerConfig("worker"));
        _messages = _consumer.ConsumeAsync(NatsRawSerializer<ReadOnlyMemory<byte>>.Default,
            new NatsJSConsumeOpts { MaxMsgs = PullBatchSize }).GetAsyncEnumerator();
        var services = new ServiceCollection();
        services.AddRelayer(relayer =>
        {
            relayer.AddMessageTypesFromThisAssembly();
            relayer.UseJsonSerializer(NatsBenchmarkJson.Default);
            new ConsumerBuilder<object>(relayer.Services, relayer.Handlers, "manual").Handler<NatsBenchmarkHandler>();
        });
        _provider = services.BuildServiceProvider();
        _serializer = _provider.GetRequiredService<IMessageSerializer>();
        var worker = new NatsConsumerWorker(_stream, "worker", new(), 1, _provider.GetRequiredService<MessageTracing>());
        var pipeline = new ConsumePipelineBuilder(_provider.GetRequiredService<IServiceScopeFactory>(),
            _provider.GetRequiredService<MessageTypeRegistry>(), _serializer, static (_, error, _) => ValueTask.FromException(error))
            .Handler(_provider.GetRequiredService<HandlerRegistry>().GetHandler<NatsBenchmarkOrder, NatsBenchmarkHandler>())
            .Settle(worker.SettleContextAsync).Build(1);
        _receive = new(pipeline, new("ce-"), 1);
    }
    [IterationSetup]
    public void PublishIteration() => PublishAsync().GetAwaiter().GetResult();
    // BenchmarkDotNet requires synchronous iteration hooks. This preparation runs outside measurements.
    private async Task PublishAsync()
    {
        for (var i = 0; i < Count; i++)
        {
            var ack = await _js.PublishAsync(_stream, _body, NatsRawSerializer<byte[]>.Default, headers: _headers);
            ack.EnsureSuccess();
        }
    }
    [Benchmark(Baseline = true, OperationsPerInvoke = Count)]
    public async Task NativeJsonReceive()
    {
        for (var i = 0; i < Count; i++)
        {
            if (!await _messages.MoveNextAsync()) throw new InvalidOperationException("Native consumer ended.");
            var native = _messages.Current;
            native.EnsureSuccess();
            _ = native.Metadata;
            await using var scope = _provider.CreateAsyncScope();
            var message = _serializer.Deserialize<NatsBenchmarkOrder>(new(native.Data));
            await scope.ServiceProvider.GetRequiredService<NatsBenchmarkHandler>().HandleAsync(message, null!, default);
            await native.AckAsync(new() { DoubleAck = true });
        }
    }
    [Benchmark(OperationsPerInvoke = Count)]
    public async Task RelayerJsonReceive()
    {
        for (var i = 0; i < Count; i++)
        {
            if (!await _messages.MoveNextAsync()) throw new InvalidOperationException("Native consumer ended.");
            var native = _messages.Current;
            native.EnsureSuccess();
            await _receive.ProcessSettledAsync(new(native, _consumer));
        }
    }
    [GlobalCleanup]
    public async Task Cleanup()
    {
        if (_messages is not null) await _messages.DisposeAsync();
        if (_js is not null) await _js.DeleteStreamAsync(_stream);
        if (_connection is not null) await _connection.DisposeAsync();
        if (_provider is not null) await _provider.DisposeAsync();
    }
}
