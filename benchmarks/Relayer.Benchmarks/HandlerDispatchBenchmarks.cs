#pragma warning disable RLY001 // This dispatch-only benchmark uses int and does not send wire messages.
using BenchmarkDotNet.Attributes;
using Microsoft.Extensions.DependencyInjection;

namespace Relayer.Benchmarks;

/// <summary>Generated dispatch versus the same resolved handler invoked directly.</summary>
[MemoryDiagnoser]
public class HandlerDispatchBenchmarks
{
    private ServiceProvider _provider = null!;
    private IServiceScope _scope = null!;
    private HandlerDescriptor<int> _descriptor = null!;
    private IHandlerMarker _handler = null!;
    private IHandler<int> _direct = null!;
    private MessageContext _context = null!;
    private int _message = 42;

    [GlobalSetup]
    public void Setup()
    {
        var services = new ServiceCollection();
        services.AddRelayer(r => r.Handler<Handler>());
        _provider = services.BuildServiceProvider();
        _scope = _provider.CreateScope();
        _descriptor = _provider.GetRequiredService<HandlerRegistry>().GetHandler<int, Handler>();
        _handler = _descriptor.Resolve(_scope.ServiceProvider);
        _direct = (IHandler<int>)_handler;
        _context = new Context(_scope.ServiceProvider);
    }

    [Benchmark(Baseline = true)]
    public ValueTask Direct() => _direct.HandleAsync(_message, _context, default);

    [Benchmark]
    public ValueTask Generated() => _descriptor.DispatchAsync(_handler, _message, _context, default);

    [GlobalCleanup]
    public void Cleanup()
    {
        _scope.Dispose();
        _provider.Dispose();
    }

    public sealed class Handler : IHandler<int>
    {
        public int LastMessage { get; private set; }

        public ValueTask HandleAsync(int message, MessageContext context, CancellationToken cancellationToken)
        {
            LastMessage = message;
            return default;
        }
    }

    private sealed class Context(IServiceProvider services) : MessageContext
    {
        public override string MessageId => "benchmark";
        public override MessageType MessageType => new("benchmark", 1);
        public override IReadOnlyDictionary<string, string> Headers { get; } = new Dictionary<string, string>();
        public override int Attempt => 1;
        public override IServiceProvider Services { get; } = services;
        public override MessageFeatures Features { get; } = new();
        public override CancellationToken CancellationToken => default;
    }
}
