using DotNet.Testcontainers.Builders;
using Azure.Messaging.ServiceBus.Administration;
using DotNet.Testcontainers.Containers;
using DotNet.Testcontainers.Networks;
using Testcontainers.MsSql;
using Testcontainers.ServiceBus;

namespace Relayer.Tests;

// Broker containers shared by the whole test session. Each starts on first use, so a filtered
// run starts only what it needs, and tests isolate themselves inside it.
public static class SharedBrokers
{
    internal static SessionResource<IContainer> RabbitMq { get; } = new(() => StartRabbitMqAsync(false));
    internal static SessionResource<IContainer> RabbitMqWithDelayedPlugin { get; } = new(() => StartRabbitMqAsync(true));

    internal static SessionResource<IContainer> LocalStack { get; } = new(async () =>
    {
        using var provisioning = new CancellationTokenSource(TimeSpan.FromMinutes(5));
        var builder = new ContainerBuilder("localstack/localstack:4.10.0")
            .WithEnvironment("SERVICES", "sqs,sns")
            .WithPortBinding(4566, true)
            .WithWaitStrategy(Wait.ForUnixContainer().UntilMessageIsLogged("Ready\\."));
        if (OperatingSystem.IsLinux()) builder = builder.WithPortBinding("20000-29999", "4566");
        var container = builder.Build();
        try { await container.StartAsync(provisioning.Token); return container; }
        catch { await container.DisposeAsync(); throw; }
    });

    internal static async Task<string> LocalStackUrlAsync(CancellationToken token)
    {
        var container = await LocalStack.Value.WaitAsync(token);
        return $"http://{container.Hostname}:{container.GetMappedPublicPort(4566)}";
    }

    internal static SessionResource<ServiceBusStack> ServiceBus { get; } = new(ServiceBusStack.StartAsync);

    private static async Task<IContainer> StartRabbitMqAsync(bool delayedPlugin)
    {
        using var provisioning = new CancellationTokenSource(TimeSpan.FromMinutes(5));
        return await RabbitMqTopologyTests.Broker.StartContainerAsync(delayedPlugin, shared: true, provisioning.Token);
    }

    [After(TestSession)]
    public static async Task StopAsync()
    {
        await RabbitMq.ResetAsync();
        await RabbitMqWithDelayedPlugin.ResetAsync();
        await LocalStack.ResetAsync();
        await ServiceBus.ResetAsync();
    }
}

internal sealed class SessionResource<T>(Func<Task<T>> start) where T : IAsyncDisposable
{
    private Lazy<Task<T>> _value = new(start);

    internal Task<T> Value => Volatile.Read(ref _value).Value;

    // Like TUnit's own per-session instances, a later session in this process starts afresh.
    internal async Task ResetAsync()
    {
        var value = Interlocked.Exchange(ref _value, new(start));
        if (!value.IsValueCreated) return;
        // Wait for a startup still in flight (every test may have given up on it) so its containers are not left behind.
        T resource;
        try { resource = await value.Value; }
        // A failed startup already cleaned up and failed the tests that needed it.
        catch { return; }
        await resource.DisposeAsync();
    }
}

// The session's Service Bus emulator and its SQL Server. Tests create their own entities on it through
// the administration client; a test that restarts the emulator starts a dedicated one instead.
internal sealed class ServiceBusStack(INetwork network, MsSqlContainer database, ServiceBusContainer emulator) : IAsyncDisposable
{
    internal ServiceBusContainer Emulator => emulator;

    internal static async Task<ServiceBusStack> StartAsync()
    {
        // Image pulls and dependency readiness have their own bounded setup budget.
        using var provisioning = new CancellationTokenSource(TimeSpan.FromMinutes(15));
        var network = new NetworkBuilder().Build();
        var databaseBuilder = new MsSqlBuilder("mcr.microsoft.com/mssql/server:2022-CU14-ubuntu-22.04")
            .WithNetwork(network).WithNetworkAliases(ServiceBusBuilder.DatabaseNetworkAlias);
        var emulatorBuilder = new ServiceBusBuilder("mcr.microsoft.com/azure-messaging/servicebus-emulator:2.0.0")
            .WithAcceptLicenseAgreement(true).WithConfig(Path.Combine(AppContext.BaseDirectory, "servicebus-config.json"));
        if (OperatingSystem.IsLinux())
        {
            // Keep every published dependency port outside Linux's outbound ephemeral range,
            // matching the Kafka fixture's mitigation for moby/moby#54079.
            databaseBuilder = databaseBuilder.WithPortBinding("20000-29999", "1433");
            emulatorBuilder = emulatorBuilder.WithPortBinding("20000-29999", "5672").WithPortBinding("20000-29999", "5300");
        }
        var database = databaseBuilder.Build();
        var stack = new ServiceBusStack(network, database,
            emulatorBuilder.WithMsSqlContainer(network, database, ServiceBusBuilder.DatabaseNetworkAlias).Build());
        try { await ServiceBusStartupDiagnostics.StartAsync(stack.Emulator.StartAsync, database, stack.Emulator, provisioning.Token); return stack; }
        catch (OperationCanceledException error) when (provisioning.IsCancellationRequested)
        {
            await stack.DisposeAsync();
            throw new TimeoutException("Service Bus emulator provisioning exceeded 15 minutes (image pulls and container readiness).", error);
        }
        catch { await stack.DisposeAsync(); throw; }
    }

    // The properties of the emulator's configured queues, within its documented TTL and size quotas.
    internal Task CreateQueueAsync(string name, bool requiresSession, CancellationToken token) =>
        new ServiceBusAdministrationClient(emulator.GetHttpConnectionString()).CreateQueueAsync(new CreateQueueOptions(name)
        {
            RequiresSession = requiresSession, LockDuration = TimeSpan.FromMinutes(1), MaxDeliveryCount = 10,
            DefaultMessageTimeToLive = TimeSpan.FromHours(1), MaxSizeInMegabytes = 100,
            DuplicateDetectionHistoryTimeWindow = TimeSpan.FromMinutes(10)
        }, token);

    public async ValueTask DisposeAsync()
    {
        try { await emulator.DisposeAsync(); }
        finally
        {
            try { await database.DisposeAsync(); }
            finally { await network.DisposeAsync(); }
        }
    }
}
