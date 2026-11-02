using BenchmarkDotNet.Attributes;
using System.Buffers;
using Microsoft.Extensions.DependencyInjection;
using Relayer.Transports;
using Relayer.Serialization;

namespace Relayer.Benchmarks;

/// <summary>Compares ordinary sending with bounded request orchestration and synchronous correlated reply dispatch.
/// This isolates core cost; it is not a broker latency or transport throughput comparison.</summary>
[MemoryDiagnoser]
public class RequestResponseBenchmarks
{
    private ServiceProvider _provider = null!;
    private IServiceScope _scope = null!;
    private ISender _sender = null!;
    private IRequestClient<BenchmarkRequest, BenchmarkResponse> _client = null!;
    private readonly BenchmarkRequest _request = new(42);

    [GlobalSetup]
    public void Setup()
    {
        var services = new ServiceCollection();
        services.AddSingleton<ITransport, ReplyTransport>();
        services.AddRelayer(builder =>
        {
            builder.AddMessageTypesFromThisAssembly();
            builder.AddTransport<SendBenchmarkOptions, SendBenchmarkOptions, SendBenchmarkOptions>("rpc", transport =>
                transport.Request<BenchmarkRequest, BenchmarkResponse>("requests", "replies"));
        });
        _provider = services.BuildServiceProvider();
        _scope = _provider.CreateScope();
        _sender = _scope.ServiceProvider.GetRequiredService<ISender>();
        _client = _scope.ServiceProvider.GetRequiredService<IRequestClient<BenchmarkRequest, BenchmarkResponse>>();
        var descriptor = _provider.GetRequiredService<HandlerRegistry>().Descriptors.Single();
        var pipeline = new ConsumePipelineBuilder(_provider.GetRequiredService<IServiceScopeFactory>(),
            _provider.GetRequiredService<MessageTypeRegistry>(), _provider.GetRequiredService<IMessageSerializer>(),
            static (_, error, _) => ValueTask.FromException(error)).Handler(descriptor).Build();
        var receive = new ReceivePipeline<OutgoingMessage, ReplyBinding>(pipeline, new());
        _provider.GetServices<ITransport>().OfType<ReplyTransport>().Single().Reply = message => receive.ProcessAsync(message);
    }

    [Benchmark(Baseline = true)]
    public ValueTask DirectSend() => _sender.SendAsync(_request);

    [Benchmark]
    public ValueTask<BenchmarkResponse> RequestReply() => _client.RequestAsync(_request, TimeSpan.FromSeconds(30));

    [GlobalCleanup]
    public async ValueTask Cleanup() { _scope.Dispose(); await _provider.DisposeAsync(); }

    private sealed class ReplyTransport : ITransport
    {
        internal Func<OutgoingMessage, ValueTask<Disposition>> Reply { get; set; } = null!;
        public string Name => "rpc";
        public TransportCapabilities GetCapabilities(string endpoint) => new();
        public async ValueTask SendAsync(OutgoingMessage message, CancellationToken cancellationToken)
        {
            foreach (var header in message.Headers.Span)
                if (header.Key == "ce_correlationid")
                {
                    await Reply(message).ConfigureAwait(false);
                    return;
                }
        }
        public ValueTask SendBatchAsync(ReadOnlyMemory<OutgoingMessage> messages, CancellationToken cancellationToken) => throw new NotSupportedException();
        public ValueTask<TopologyReport> ApplyTopologyAsync(TopologyMode mode, CancellationToken cancellationToken) => new(new TopologyReport([]));
        public Task StartAsync(CancellationToken cancellationToken) => Task.CompletedTask;
        public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
        public ValueTask DisposeAsync() => default;
    }

    private readonly struct ReplyBinding : IMessageBinding<OutgoingMessage>
    {
        private static readonly byte[] Body = "{\"Value\":42}"u8.ToArray();
        public static ReadOnlySequence<byte> GetBody(OutgoingMessage message) => new(Body);
        public static int? GetDeliveryCount(OutgoingMessage message) => 1;
        public static bool TryGetHeader(OutgoingMessage message, string name, out WireString value)
        {
            if (name == "ce_type") { value = new("benchmark.response;v=1"); return true; }
            foreach (var header in message.Headers.Span)
                if (header.Key == name) { value = header.Value; return true; }
            value = default;
            return false;
        }
    }
}

[Message("benchmark.request")]
public sealed record BenchmarkRequest(int Value);
[Message("benchmark.response")]
public sealed record BenchmarkResponse(int Value);
