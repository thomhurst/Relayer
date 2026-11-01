using System.Buffers;
using System.Diagnostics;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Configs;
using Microsoft.Extensions.DependencyInjection;
using Relayer.Serialization;
using Relayer.Transports;

namespace Relayer.Benchmarks;

/// <summary>Compares the real send route and native batch tracing seam with synchronous native sinks, not broker throughput.</summary>
[MemoryDiagnoser]
[GroupBenchmarksBy(BenchmarkLogicalGroupRule.ByCategory)]
public class SendTracingBenchmarks
{
    private ServiceProvider _provider = null!;
    private IServiceScope _scope = null!;
    private ISender _sender = null!;
    private Sink _sink = null!;
    private IMessageSerializer _serializer = null!;
    private ActivityListener? _listener;
    private readonly SendOrder _message = new(42);
    private readonly ArrayBufferWriter<byte> _body = new();
    private readonly OutgoingMessage[] _batch = [new("orders", new byte[] { 1 }), new("orders", new byte[] { 2 })];

    [Params("NoListener", "Drop", "Record")]
    public string Mode { get; set; } = "NoListener";

    [GlobalSetup]
    public void Setup()
    {
        var services = new ServiceCollection();
        services.AddSingleton<ITransport>(provider => new Sink(provider.GetRequiredService<MessageTracing>()));
        services.AddRelayer(builder =>
        {
            builder.AddMessageTypesFromThisAssembly();
            builder.AddTransport<SendBenchmarkOptions, SendBenchmarkOptions, SendBenchmarkOptions>("sink",
                transport => transport.Send<SendOrder>("orders"));
        });
        _provider = services.BuildServiceProvider();
        var tracing = _provider.GetRequiredService<MessageTracing>();
        if (Mode != "NoListener")
        {
            _listener = new()
            {
                ShouldListenTo = source => ReferenceEquals(source, tracing.Source),
                Sample = (ref ActivityCreationOptions<ActivityContext> _) =>
                    Mode == "Drop" ? ActivitySamplingResult.None : ActivitySamplingResult.AllDataAndRecorded,
            };
            ActivitySource.AddActivityListener(_listener);
        }
        _scope = _provider.CreateScope();
        _sender = _scope.ServiceProvider.GetRequiredService<ISender>();
        _sink = (Sink)_provider.GetRequiredService<ITransport>();
        _serializer = _provider.GetRequiredService<IMessageSerializer>();
    }

    [Benchmark(Baseline = true), BenchmarkCategory("Single")]
    public ValueTask SerializeAndSendNative()
    {
        _body.Clear();
        _serializer.Serialize(_message, _body);
        return _sink.SendNative(new("orders", _body.WrittenMemory), default);
    }

    [Benchmark, BenchmarkCategory("Single")]
    public ValueTask DirectSend() => _sender.SendNowAsync(_message);

    [Benchmark(Baseline = true), BenchmarkCategory("Batch")]
    public ValueTask NativeBatch() => _sink.SendBatchNative(_batch, default);

    [Benchmark, BenchmarkCategory("Batch")]
    public ValueTask TracedBatch() => _sink.SendBatchAsync(_batch, default);

    [GlobalCleanup]
    public void Cleanup()
    {
        _scope.Dispose();
        _listener?.Dispose();
        _provider.Dispose();
    }

    private sealed class Sink : ITransport, ITransportSendTracing, IDisposable
    {
        private readonly SendTracing _tracing;
        public int Bytes { get; private set; }
        internal Sink(MessageTracing tracing) => _tracing = new(tracing,
            new() { System = "relayer.inmemory", Destination = "orders" }, SendNative, SendBatchNative);
        public string Name => "sink";
        public SendTracing GetSendTracing(string destination) => _tracing;
        public TransportCapabilities GetCapabilities(string endpoint) => new() { MaxBatchSize = 100 };
        public ValueTask SendAsync(OutgoingMessage message, CancellationToken cancellationToken) => _tracing.SendAsync(message, cancellationToken);
        public ValueTask SendBatchAsync(ReadOnlyMemory<OutgoingMessage> messages, CancellationToken cancellationToken) => _tracing.SendBatchAsync(messages, cancellationToken);
        internal ValueTask SendNative(OutgoingMessage message, CancellationToken cancellationToken)
        {
            Bytes = message.Body.Length;
            return default;
        }
        internal ValueTask SendBatchNative(ReadOnlyMemory<OutgoingMessage> messages, CancellationToken cancellationToken)
        {
            Bytes = messages.Length;
            return default;
        }
        public ValueTask<TopologyReport> ApplyTopologyAsync(TopologyMode mode, CancellationToken cancellationToken) => new(new TopologyReport([]));
        public Task StartAsync(CancellationToken cancellationToken) => Task.CompletedTask;
        public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
        public ValueTask DisposeAsync() => default;
        public void Dispose() { }
    }
}
