using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

namespace Relayer.TestFixtures;

public abstract class OutboxWriterConfigurationTests<TOptions> where TOptions : class
{
    protected abstract void Register(IServiceCollection services, IConfiguration configuration);

    protected abstract void Register(IServiceCollection services, int maxDispatchAttempts);

    protected abstract (int Attempts, int Lanes) ReadSettings(TOptions options);

    protected abstract string ReadPrefix(TOptions options);

    [Test]
    public async Task Sections_apply_in_registration_order_and_each_section_reloads()
    {
        var first = new ConfigurationBuilder().AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["MaxDispatchAttempts"] = "4",
            ["Lanes"] = "8"
        }).Build();
        var second = new ConfigurationBuilder().AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["Lanes"] = "16"
        }).Build();
        var services = new ServiceCollection();
        Register(services, first);
        Register(services, second);

        await using var provider = services.BuildServiceProvider();
        var monitor = provider.GetRequiredService<IOptionsMonitor<TOptions>>();
        using var existingScope = provider.CreateScope();
        var snapshot = existingScope.ServiceProvider.GetRequiredService<IOptionsSnapshot<TOptions>>();
        var notifications = new List<(int Attempts, int Lanes)>();
        using var subscription = monitor.OnChange((options, _) => notifications.Add(ReadSettings(options)));

        await Assert.That(ReadSettings(monitor.CurrentValue)).IsEqualTo((4, 16));
        await Assert.That(ReadSettings(snapshot.Value)).IsEqualTo((4, 16));

        first["MaxDispatchAttempts"] = "7";
        first["Lanes"] = "32";
        first.Reload();

        await Assert.That(ReadSettings(monitor.CurrentValue)).IsEqualTo((7, 16));

        second["Lanes"] = "64";
        second.Reload();

        await Assert.That(ReadSettings(monitor.CurrentValue)).IsEqualTo((7, 64));
        await Assert.That(notifications.ToArray()).IsEquivalentTo(new[] { (7, 16), (7, 64) });
        await Assert.That(ReadSettings(snapshot.Value)).IsEqualTo((4, 16));
        using var newScope = provider.CreateScope();
        await Assert.That(ReadSettings(newScope.ServiceProvider.GetRequiredService<IOptionsSnapshot<TOptions>>().Value))
            .IsEqualTo((7, 64));
    }

    [Test]
    public async Task Explicit_options_keep_their_registration_precedence()
    {
        var first = new ConfigurationBuilder().AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["MaxDispatchAttempts"] = "4"
        }).Build();
        var second = new ConfigurationBuilder().AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["Lanes"] = "16"
        }).Build();
        var services = new ServiceCollection();
        Register(services, first);
        Register(services, 7);

        await using var explicitProvider = services.BuildServiceProvider();
        var explicitOptions = explicitProvider.GetRequiredService<IOptions<TOptions>>().Value;
        await Assert.That(ReadSettings(explicitOptions).Attempts).IsEqualTo(7);

        Register(services, second);

        await using var configuredProvider = services.BuildServiceProvider();
        var configuredOptions = configuredProvider.GetRequiredService<IOptions<TOptions>>().Value;
        await Assert.That(ReadSettings(configuredOptions)).IsEqualTo((10, 16));
    }

    [Test]
    public async Task New_snapshot_resolves_fresh_transient_validator_dependencies()
    {
        var configuration = new ConfigurationBuilder().AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["Outbox:TablePrefix"] = "before_",
            ["ExpectedPrefix"] = "before_"
        }).Build();
        var services = new ServiceCollection();
        services.AddSingleton<IConfiguration>(configuration);
        services.AddTransient<PrefixPolicy>();
        Register(services, configuration.GetSection("Outbox"));
        services.AddOptions<TOptions>().Validate<PrefixPolicy>(
            (options, policy) => ReadPrefix(options) == policy.Prefix, "Use the configured table prefix.");

        await using var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true });
        using var firstScope = provider.CreateScope();
        var first = firstScope.ServiceProvider.GetRequiredService<IOptionsSnapshot<TOptions>>();
        await Assert.That(ReadPrefix(first.Value)).IsEqualTo("before_");

        configuration["Outbox:TablePrefix"] = "after_";
        configuration["ExpectedPrefix"] = "after_";
        configuration.Reload();

        using var secondScope = provider.CreateScope();
        var second = secondScope.ServiceProvider.GetRequiredService<IOptionsSnapshot<TOptions>>();
        await Assert.That(ReadPrefix(second.Value)).IsEqualTo("after_");
        await Assert.That(ReadPrefix(first.Value)).IsEqualTo("before_");
    }

    private sealed class PrefixPolicy(IConfiguration configuration)
    {
        public string? Prefix
        {
            get;
        } = configuration["ExpectedPrefix"];
    }
}
