using Relayer.Transports;
using BenchmarkDotNet.Attributes;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Relayer.Outbox.EntityFrameworkCore;
using Relayer.Outbox.PostgreSql;

namespace Relayer.Benchmarks;

/// <summary>Default capability discovery in the delivery scope, without a DbContext or database I/O.</summary>
[MemoryDiagnoser]
public class OutboxScopeResolutionBenchmarks
{
    private ServiceProvider _legacy = null!;
    private ServiceProvider _cached = null!;

    [GlobalSetup]
    public void Setup()
    {
        var legacy = Services();
        // Reproduce the previous factory, including snapshot validation and SQL generation per scope.
        legacy.AddScoped<EntityFrameworkCoreOutbox>(static provider =>
        {
            var configuration = new OutboxConfiguration(
                provider.GetRequiredService<IOptionsSnapshot<EntityFrameworkCoreOutboxOptions>>().Value,
                workers: provider.GetServices<PostgreSqlSchedulingRegistration>());
            return new(configuration.SupportsScheduling, configuration.SupportsOrderedScheduling);
        });
        _legacy = legacy.BuildServiceProvider();
        _cached = Services().BuildServiceProvider();
        _ = Resolve(_legacy);
        _ = Resolve(_cached);
    }

    private static IServiceCollection Services()
    {
        var services = new ServiceCollection().AddRelayerEntityFrameworkCoreOutbox();
        services.AddSingleton(new PostgreSqlSchedulingRegistration(null, "relayer_"));
        return services;
    }

    [Benchmark(Baseline = true)]
    public bool PreviousFactory() => Resolve(_legacy);

    [Benchmark]
    public bool CachedCapabilities() => Resolve(_cached);

    private static bool Resolve(ServiceProvider provider)
    {
        using var scope = provider.CreateScope();
        return scope.ServiceProvider.GetRequiredService<IOutboxEnlistment>().SupportsScheduling;
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        _legacy.Dispose();
        _cached.Dispose();
    }
}
