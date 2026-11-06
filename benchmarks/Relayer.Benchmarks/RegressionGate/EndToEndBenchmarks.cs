using System.Runtime.CompilerServices;
using System.Threading.Channels;
using BenchmarkDotNet.Attributes;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Relayer.InMemory;
using Relayer.Transports;

namespace Relayer.Benchmarks;

/// <summary>Public ISender, default generated JSON, hosted receive composition and native queue ownership.</summary>
/// <remarks>Totals include fresh receive scopes. Driver channels and native scheduling are included, not subtracted.</remarks>
[MemoryDiagnoser]
[BenchmarkCategory("RegressionGate")]
public class EndToEndBenchmarks
{
    private const int ConcurrentMessages = 8;
    private readonly EndToEndSignals _signals = new();
    private readonly EndToEndOrder _order = new(42);
    private readonly ConcurrentOrder _concurrent = new(42);
    private readonly SendOnlyOrder _sendOnly = new(42);
    private readonly CancellationTokenSource _limit = new();
    private ServiceProvider _provider = null!;
    private AsyncServiceScope _scope;
    private ISender _sender = null!;
    private IHostedService[] _hosted = null!;
    private long _sent;
    private long _received;

    [GlobalSetup]
    public async Task Setup()
    {
        // A deadlock or delivery failure must fail the benchmark rather than leave CI waiting indefinitely.
        _limit.CancelAfter(TimeSpan.FromMinutes(15));
        var services = new ServiceCollection();
        services.AddSingleton(_signals);
        services.AddRelayer(builder =>
        {
            builder.AddMessageTypesFromThisAssembly();
            builder.InMemory("benchmark", transport =>
            {
                transport.Send<EndToEndOrder>("synchronous");
                transport.Send<ConcurrentOrder>("concurrent");
                transport.Send<SendOnlyOrder>("send-only");
                transport.Consume("synchronous", consumer => consumer.Handler<EndToEndHandler>());
                transport.Consume("concurrent", consumer => consumer.Handler<ConcurrentHandler>().Concurrency(ConcurrentMessages));
            });
        });
        _provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true });
        var native = (InMemoryTransport)_provider.GetRequiredService<ITransport>();
        // Native drain only: DirectSend measures send admission and backpressure, without core receive work.
        native.RegisterConsumer("send-only", (_, _) =>
        {
            Interlocked.Increment(ref _received);
            return new(Disposition.Complete);
        });
        _hosted = _provider.GetServices<IHostedService>().ToArray();
        foreach (var lifecycle in _hosted.OfType<IHostedLifecycleService>()) await lifecycle.StartingAsync(_limit.Token);
        foreach (var service in _hosted) await service.StartAsync(_limit.Token);
        _scope = _provider.CreateAsyncScope();
        _sender = _scope.ServiceProvider.GetRequiredService<ISender>();
    }

    [Benchmark]
    public ValueTask DirectSend()
    {
        Interlocked.Increment(ref _sent);
        return _sender.SendAsync(_sendOnly, _limit.Token);
    }

    [Benchmark]
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    public async ValueTask<int> SynchronousRoundTrip()
    {
        await _sender.SendAsync(_order, _limit.Token);
        return await _signals.Completed.Reader.ReadAsync(_limit.Token);
    }

    [Benchmark(OperationsPerInvoke = ConcurrentMessages)]
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    public async ValueTask<int> ConcurrentRoundTrip()
    {
        // This loop is the workload: eight live deliveries, not eight serial benchmark repetitions.
        for (var i = 0; i < ConcurrentMessages; i++) await _sender.SendAsync(_concurrent, _limit.Token);
        for (var i = 0; i < ConcurrentMessages; i++) await _signals.Entered.Reader.ReadAsync(_limit.Token);
        // All handlers have reached an incomplete await before any can finish.
        for (var i = 0; i < ConcurrentMessages; i++) _signals.Release.Writer.TryWrite(0);
        var total = 0;
        for (var i = 0; i < ConcurrentMessages; i++) total += await _signals.Completed.Reader.ReadAsync(_limit.Token);
        return total;
    }

    [GlobalCleanup]
    public async Task Cleanup()
    {
        // Verify direct sends reached the native receiver before stopping owned services.
        while (Interlocked.Read(ref _received) != Interlocked.Read(ref _sent))
            await Task.Delay(1, _limit.Token);
        for (var i = _hosted.Length - 1; i >= 0; i--) await _hosted[i].StopAsync(_limit.Token);
        await _scope.DisposeAsync();
        await _provider.DisposeAsync();
        _limit.Dispose();
    }
}

public sealed class EndToEndSignals
{
    public Channel<int> Completed { get; } = Channel.CreateBounded<int>(8);
    public Channel<byte> Entered { get; } = Channel.CreateBounded<byte>(8);
    public Channel<byte> Release { get; } = Channel.CreateBounded<byte>(8);
}

[Message("bench.end-to-end")]
public sealed record EndToEndOrder(int Id);
[Message("bench.concurrent")]
public sealed record ConcurrentOrder(int Id);
[Message("bench.send-only")]
public sealed record SendOnlyOrder(int Id);

public sealed class EndToEndHandler(EndToEndSignals signals) : IHandler<EndToEndOrder>
{
    public ValueTask HandleAsync(EndToEndOrder message, MessageContext context, CancellationToken cancellationToken)
    {
        if (!signals.Completed.Writer.TryWrite(message.Id)) throw new InvalidOperationException("Completion channel is full.");
        return default;
    }
}

public sealed class ConcurrentHandler(EndToEndSignals signals) : IHandler<ConcurrentOrder>
{
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    public async ValueTask HandleAsync(ConcurrentOrder message, MessageContext context, CancellationToken cancellationToken)
    {
        // Publish entry only after the release continuation is registered. Merely observing
        // an incomplete read before signalling leaves a race with the driver's release.
        await new PendingReadBarrier(signals.Release.Reader.ReadAsync(cancellationToken), signals.Entered.Writer);
        if (!signals.Completed.Writer.TryWrite(message.Id)) throw new InvalidOperationException("Completion channel is full.");
    }
}
