using BenchmarkDotNet.Attributes;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Relayer.Outbox.EntityFrameworkCore;

namespace Relayer.Benchmarks;

/// <summary>Measures configuration binding and validation when an EF Core writer scope first uses outbox options.</summary>
[MemoryDiagnoser]
public class EntityFrameworkCoreOutboxOptionsBenchmarks
{
    private ServiceProvider _provider = null!;

    [Params(false, true)]
    public bool LayeredConfiguration
    {
        get; set;
    }

    [GlobalSetup]
    public void Setup()
    {
        var configuration = new ConfigurationBuilder().AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["TablePrefix"] = "orders_",
            ["MaxDispatchAttempts"] = "4"
        }).Build();
        var services = new ServiceCollection().AddRelayerEntityFrameworkCoreOutbox(configuration);

        if (LayeredConfiguration)
        {
            var overrides = new ConfigurationBuilder().AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["MaxDispatchAttempts"] = "7"
            }).Build();
            services.AddRelayerEntityFrameworkCoreOutbox(overrides);
        }

        _provider = services.BuildServiceProvider();
    }

    [Benchmark]
    public int ResolveSnapshot()
    {
        using var scope = _provider.CreateScope();
        return scope.ServiceProvider.GetRequiredService<IOptionsSnapshot<EntityFrameworkCoreOutboxOptions>>().Value.MaxDispatchAttempts;
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        _provider.Dispose();
    }
}
