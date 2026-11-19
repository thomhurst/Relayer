using BenchmarkDotNet.Attributes;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Relayer.InMemory;
using Relayer.Outbox.EntityFrameworkCore;

namespace Relayer.Benchmarks;

/// <summary>Compares application send APIs for a selected EF context, before database I/O.</summary>
[MemoryDiagnoser]
public class ContextOutboxSendBenchmarks
{
    private readonly DirectJsonOrder _message = new(42, "customer-1");
    private ServiceProvider _provider = null!;
    private AsyncServiceScope _scope;
    private DbContext _db = null!;
    private IOutboxSelection _selection = null!;
    private ISender _sender = null!;
    private OutboxContext _state = null!;

    [GlobalSetup]
    public void Setup()
    {
        var services = new ServiceCollection().AddRelayerEntityFrameworkCoreOutbox();
        services.AddRelayer(builder =>
        {
            builder.AddMessageTypesFromThisAssembly();
            builder.InMemory("local", transport => transport.Send<DirectJsonOrder>("orders"));
        });
        _provider = services.BuildServiceProvider();
        _scope = _provider.CreateAsyncScope();
        var options = new DbContextOptionsBuilder().UseNpgsql("Host=unused").AddRelayerOutbox(_scope.ServiceProvider);
        _db = new DbContext(options.Options);
        _selection = _db.EnlistRelayerOutbox();
        _sender = _scope.ServiceProvider.GetRequiredService<ISender>();
        _state = _scope.ServiceProvider.GetRequiredService<EntityFrameworkCoreOutbox>().Get(_db, new PostgreSqlOutboxConfiguration(new()));
    }

    [Benchmark(Baseline = true)]
    public async ValueTask ScopedSender()
    {
        await _sender.SendViaOutboxAsync(_message);
        _state.Clear();
    }

    [Benchmark]
    public async ValueTask SelectedContext()
    {
        await _selection.EnqueueAsync(_message);
        _state.Clear();
    }

    [GlobalCleanup]
    public async Task Cleanup()
    {
        _selection.Dispose();
        await _db.DisposeAsync();
        await _scope.DisposeAsync();
        await _provider.DisposeAsync();
    }
}
