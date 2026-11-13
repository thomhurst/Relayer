using BenchmarkDotNet.Attributes;
using Microsoft.Extensions.DependencyInjection;
using Relayer.Serialization;

namespace Relayer.Benchmarks;

/// <summary>Builds a fresh dispatch table and context pool for 100 distinct generated handlers.</summary>
[MemoryDiagnoser]
[BenchmarkCategory("RegressionGate")]
public class StartupBenchmarks
{
    private ServiceProvider _provider = null!;
    private IServiceScopeFactory _scopes = null!;
    private MessageTypeRegistry _messages = null!;
    private IMessageSerializer _serializer = null!;
    private IReadOnlyList<HandlerDescriptor> _handlers = null!;

    [GlobalSetup]
    public void Setup()
    {
        var services = new ServiceCollection();
        services.AddRelayer(builder =>
        {
            builder.AddMessageTypesFromThisAssembly();
            builder.AddTransport<object, object, object>("startup", transport =>
                transport.Consume("source", consumer => StartupHandlers.Register(consumer)));
        });
        _provider = services.BuildServiceProvider();
        _scopes = _provider.GetRequiredService<IServiceScopeFactory>();
        _messages = _provider.GetRequiredService<MessageTypeRegistry>();
        _serializer = _provider.GetRequiredService<IMessageSerializer>();
        _handlers = _provider.GetRequiredService<HandlerRegistry>().Descriptors;
        if (_handlers.Count != 100) throw new InvalidOperationException("Startup fixture requires exactly 100 handlers.");
    }

    [Benchmark]
    public ConsumePipeline Build100Handlers()
    {
        var builder = new ConsumePipelineBuilder(_scopes, _messages, _serializer,
            static (_, error, _) => ValueTask.FromException(error));
        foreach (var handler in _handlers) builder.Handler(handler);
        return builder.Build();
    }

    [GlobalCleanup]
    public void Cleanup() => _provider.Dispose();
}
