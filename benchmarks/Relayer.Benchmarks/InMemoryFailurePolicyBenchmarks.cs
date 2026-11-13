#pragma warning disable RLY004 // The benchmark composes its generated handler into the receive pipeline below.
using System.Buffers;
using System.Runtime.CompilerServices;
using System.Threading.Channels;
using BenchmarkDotNet.Attributes;
using Microsoft.Extensions.DependencyInjection;
using Relayer.InMemory;
using Relayer.Serialization;
using Relayer.Transports;

namespace Relayer.Benchmarks;

/// <summary>Attributes steady-state receive cost to the common failure executor versus the former in-memory delegate.</summary>
[MemoryDiagnoser]
public class InMemoryFailurePolicyBenchmarks
{
    private ServiceProvider _provider = null!;
    private InMemoryTransport _transport = null!;
    private readonly Channel<int> _received = Channel.CreateBounded<int>(1);
    private OutgoingMessage _message;

    [Params(false, true)]
    public bool PortableFailures { get; set; }

    [GlobalSetup]
    public async Task Setup()
    {
        var services = new ServiceCollection();
        services.AddSingleton(_received);
        services.AddSingleton<InMemoryBenchmarkHandler>();
        services.AddRelayer(builder =>
        {
            builder.AddMessageTypesFromThisAssembly();
            builder.AddTransport<Options, Options, Options>("benchmark", transport => transport.Consume("orders", consumer => consumer.Handler<InMemoryBenchmarkHandler>()));
        });
        _transport = new("benchmark", new Dictionary<string, InMemoryEndpointOptions> { ["orders"] = new() });
        services.AddSingleton<ITransport>(_transport);
        _provider = services.BuildServiceProvider();
        var serializer = _provider.GetRequiredService<IMessageSerializer>();
        var scopes = _provider.GetRequiredService<IServiceScopeFactory>();
        var messages = _provider.GetRequiredService<MessageTypeRegistry>();
        var composer = PortableFailures
            ? new ConsumePipelineBuilder(scopes, messages, serializer, new FailurePipeline(_provider, "benchmark", "orders", "orders"))
            : new ConsumePipelineBuilder(scopes, messages, serializer, static (_, error, _) => ValueTask.FromException(error));
        var consumer = composer.Handler(_provider.GetRequiredService<HandlerRegistry>()
            .GetHandler<InMemoryBenchmarkOrder, InMemoryBenchmarkHandler>()).Build(1);
        var receive = new ReceivePipeline<InMemoryDelivery, InMemoryMessageBinding>(consumer, new HeaderNames(), 1);
        _transport.RegisterConsumer("orders", receive.ProcessAsync);
        var buffer = new ArrayBufferWriter<byte>();
        serializer.Serialize(new InMemoryBenchmarkOrder(42), buffer);
        _message = new("orders", buffer.WrittenMemory, new KeyValuePair<string, WireString>[] { new("ce_id", new("benchmark")), new("ce_type", new("bench.inmemory-order;v=1")) });
        await _transport.StartAsync(default);
    }

    [Benchmark]
#if NET11_0_OR_GREATER
    [RuntimeAsyncMethodGeneration(false)]
#endif
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    public async ValueTask<int> RoundTrip()
    {
        await _transport.SendAsync(_message, default);
        return await _received.Reader.ReadAsync();
    }

    [GlobalCleanup]
    public async Task Cleanup()
    {
        await _transport.StopAsync(default);
        await _provider.DisposeAsync();
    }

    public sealed class Options;
}
