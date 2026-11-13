using Relayer.Transports;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;

namespace Relayer.Outbox.SqlServer.Tests;

public sealed class DirectDispatchRegistrationTests
{
    private const string ConnectionString = "Server=localhost;Database=orders;Integrated Security=True;Encrypt=False";

    [Test]
    public async Task Named_workers_bind_immutable_options_and_register_hosted_services()
    {
        var configuration = new ConfigurationBuilder().AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["QueueCapacity"] = "3", ["BatchSize"] = "7", ["ClaimDuration"] = "00:02:00"
        }).Build();
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddRelayerSqlServerDirectDispatch("first", ConnectionString, configuration);
        services.AddRelayerSqlServerDirectDispatch("second", ConnectionString, dispatchOptions: new() { BatchSize = 2 });
        await using var provider = services.BuildServiceProvider();
        var monitor = provider.GetRequiredService<IOptionsMonitor<SqlServerDirectDispatchOptions>>();
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
        var services = new ServiceCollection();
        services.AddRelayerSqlServerDirectDispatch("store", ConnectionString);
        await Assert.That(() => services.AddRelayerSqlServerDirectDispatch("store", ConnectionString)).Throws<InvalidOperationException>();
        await Assert.That(() => new ServiceCollection().AddRelayerSqlServerDirectDispatch("store", ConnectionString,
            dispatchOptions: new() { QueueCapacity = 0 })).Throws<ArgumentOutOfRangeException>();
        await Assert.That(() => new ServiceCollection().AddRelayerSqlServerDirectDispatch("store", ConnectionString,
            dispatchOptions: new() { BatchSize = 0 })).Throws<ArgumentOutOfRangeException>();
        await Assert.That(() => new ServiceCollection().AddRelayerSqlServerDirectDispatch("store", ConnectionString,
            dispatchOptions: new() { ClaimDuration = TimeSpan.Zero })).Throws<ArgumentOutOfRangeException>();
        await Assert.That(() => new ServiceCollection().AddRelayerSqlServerDirectDispatch("store", "Server=localhost;Integrated Security=True"))
            .Throws<ArgumentException>();
    }

    [Test]
    public async Task Missing_worker_and_mismatched_schema_fail_before_transaction()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddRelayerSqlServerOutbox(new SqlServerOutboxOptions { DispatchStoreName = "store", Schema = "writer" });
        services.AddRelayerSqlServerDirectDispatch("store", ConnectionString, new SqlServerOutboxOptions { Schema = "dispatcher" });
        await using var provider = services.BuildServiceProvider();
        using var scope = provider.CreateScope();
        await Assert.That(() => scope.ServiceProvider.GetRequiredService<SqlServerOutbox>()).Throws<InvalidOperationException>();
        var missing = new ServiceCollection();
        missing.AddRelayerSqlServerOutbox(new SqlServerOutboxOptions { DispatchStoreName = "missing" });
        await using var missingProvider = missing.BuildServiceProvider();
        using var missingScope = missingProvider.CreateScope();
        await Assert.That(() => missingScope.ServiceProvider.GetRequiredService<SqlServerOutbox>()).Throws<InvalidOperationException>();
    }
}
