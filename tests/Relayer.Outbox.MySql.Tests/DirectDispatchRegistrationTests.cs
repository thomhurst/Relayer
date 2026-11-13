using Relayer.Transports;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;

namespace Relayer.Outbox.MySql.Tests;

public sealed class DirectDispatchRegistrationTests
{
    private const string ConnectionString = "Server=localhost;Database=orders";

    [Test]
    public async Task Named_workers_bind_immutable_options_and_register_hosted_services()
    {
        using var source = new MySqlConnector.MySqlDataSource(ConnectionString);
        var configuration = new ConfigurationBuilder().AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["QueueCapacity"] = "3", ["BatchSize"] = "7", ["ClaimDuration"] = "00:02:00"
        }).Build();
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddRelayerMySqlDirectDispatch("first", source, configuration);
        services.AddRelayerMySqlDirectDispatch("second", source, dispatchOptions: new() { BatchSize = 2 });
        await using var provider = services.BuildServiceProvider();
        var monitor = provider.GetRequiredService<IOptionsMonitor<MySqlDirectDispatchOptions>>();
        await Assert.That(monitor.Get("first").QueueCapacity).IsEqualTo(3);
        await Assert.That(monitor.Get("first").BatchSize).IsEqualTo(7);
        await Assert.That(monitor.Get("first").ClaimDuration).IsEqualTo(TimeSpan.FromMinutes(2));
        await Assert.That(monitor.Get("second").BatchSize).IsEqualTo(2);
        await Assert.That(provider.GetServices<IHostedService>().Count()).IsEqualTo(2);
        using var scope = provider.CreateScope();
        await Assert.That(scope.ServiceProvider.GetRequiredKeyedService<IOutboxStore>("first")).IsNotNull();
    }

    [Test]
    public async Task Duplicate_names_invalid_limits_and_implicit_database_fail_at_registration()
    {
        using var source = new MySqlConnector.MySqlDataSource(ConnectionString);
        var services = new ServiceCollection();
        services.AddRelayerMySqlDirectDispatch("store", source);
        await Assert.That(() => services.AddRelayerMySqlDirectDispatch("store", source)).Throws<InvalidOperationException>();
        await Assert.That(() => new ServiceCollection().AddRelayerMySqlDirectDispatch("store", source,
            dispatchOptions: new() { QueueCapacity = 0 })).Throws<ArgumentOutOfRangeException>();
        await Assert.That(() => new ServiceCollection().AddRelayerMySqlDirectDispatch("store", source,
            dispatchOptions: new() { BatchSize = 0 })).Throws<ArgumentOutOfRangeException>();
        await Assert.That(() => new ServiceCollection().AddRelayerMySqlDirectDispatch("store", source,
            dispatchOptions: new() { ClaimDuration = TimeSpan.Zero })).Throws<ArgumentOutOfRangeException>();
        using var noDatabase = new MySqlConnector.MySqlDataSource("Server=localhost");
        await Assert.That(() => new ServiceCollection().AddRelayerMySqlDirectDispatch("store", noDatabase))
            .Throws<ArgumentException>();
    }

    [Test]
    public async Task Missing_worker_and_mismatched_prefix_fail_before_transaction()
    {
        using var source = new MySqlConnector.MySqlDataSource(ConnectionString);
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddRelayerMySqlOutbox(new MySqlOutboxOptions { DispatchStoreName = "store", TablePrefix = "writer" });
        services.AddRelayerMySqlDirectDispatch("store", source, new MySqlOutboxOptions { TablePrefix = "dispatcher" });
        await using var provider = services.BuildServiceProvider();
        using var scope = provider.CreateScope();
        await Assert.That(() => scope.ServiceProvider.GetRequiredService<MySqlOutbox>()).Throws<InvalidOperationException>();
        var missing = new ServiceCollection();
        missing.AddRelayerMySqlOutbox(new MySqlOutboxOptions { DispatchStoreName = "missing" });
        await using var missingProvider = missing.BuildServiceProvider();
        using var missingScope = missingProvider.CreateScope();
        await Assert.That(() => missingScope.ServiceProvider.GetRequiredService<MySqlOutbox>()).Throws<InvalidOperationException>();
    }
}
