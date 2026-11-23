using BenchmarkDotNet.Attributes;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Relayer.Outbox.MySql;

namespace Relayer.Benchmarks;

/// <summary>Resolves configured writer options in a fresh delivery scope without opening a connection.</summary>
[MemoryDiagnoser]
public class ConfiguredOutboxWriterOptionsBenchmarks
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
        var defaults = new ConfigurationBuilder().AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["TablePrefix"] = "orders_",
            ["Lanes"] = LayeredConfiguration ? "8" : "16",
            ["MaxDispatchAttempts"] = "4"
        }).Build();
        var services = new ServiceCollection().AddRelayerMySqlOutbox(defaults);

        if (LayeredConfiguration)
        {
            var overrides = new ConfigurationBuilder().AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["Lanes"] = "16"
            }).Build();
            services.AddRelayerMySqlOutbox(overrides);
        }

        _provider = services.BuildServiceProvider();
        _ = ResolveSnapshot();
    }

    [Benchmark]
    public MySqlOutboxOptions ResolveSnapshot()
    {
        using var scope = _provider.CreateScope();
        return scope.ServiceProvider.GetRequiredService<IOptionsSnapshot<MySqlOutboxOptions>>().Value;
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        _provider.Dispose();
    }
}
