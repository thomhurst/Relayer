using BenchmarkDotNet.Attributes;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Npgsql;
using Relayer.Outbox.EntityFrameworkCore;
using Relayer.Outbox.PostgreSql;
using Relayer.Transports;

namespace Relayer.Benchmarks;

/// <summary>Default capability discovery in the delivery scope, without a DbContext or database I/O.</summary>
[MemoryDiagnoser]
public class OutboxScopeResolutionBenchmarks
{
    private NpgsqlDataSource _dataSource = null!;
    private ServiceProvider _legacy = null!;
    private ServiceProvider _cached = null!;

    [GlobalSetup]
    public void Setup()
    {
        _dataSource = NpgsqlDataSource.Create("Host=unused");
        var legacy = Services(_dataSource);
        // Reproduce the previous factory, including snapshot validation and SQL generation per scope.
        legacy.AddScoped<EntityFrameworkCoreOutbox>(static provider =>
        {
            var configuration = new PostgreSqlOutboxConfiguration(
                provider.GetRequiredService<IOptionsSnapshot<EntityFrameworkCoreOutboxOptions>>().Value,
                workers: provider.GetServices<PostgreSqlSchedulingRegistration>());
            var (unordered, ordered) = configuration.RegisteredScheduling;
            return new(unordered, ordered);
        });
        _legacy = legacy.BuildServiceProvider();
        _cached = Services(_dataSource).BuildServiceProvider();
        _ = Resolve(_legacy);
        _ = Resolve(_cached);
    }

    private static IServiceCollection Services(NpgsqlDataSource dataSource)
    {
        var services = new ServiceCollection().AddRelayerEntityFrameworkCoreOutbox();
        services.AddSingleton(new PostgreSqlSchedulingRegistration(dataSource, null, "relayer_"));
        return services;
    }

    [Benchmark(Baseline = true)]
    public bool PreviousFactory()
    {
        return Resolve(_legacy);
    }

    [Benchmark]
    public bool CachedCapabilities()
    {
        return Resolve(_cached);
    }

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
        _dataSource.Dispose();
    }
}
