using Microsoft.Extensions.DependencyInjection;
using RabbitMQ.Stream.Client;
using Relayer.Transports;

namespace Relayer.RabbitMQ.Streams.Tests;

public class SuperStreamTopologyTests
{
    [Test]
    public async Task Provision_creates_native_partitions_and_preserves_existing_topology()
    {
        var broker = await RabbitMqStreamsTestBroker.Current;
        var name = TestContext.Current!.Isolation.GetIsolatedName("events");
        var spec = new BindingsSuperStreamSpec(name, ["a", "b"])
        {
            MaxLengthBytes = 987654321,
        };
        await using var provider = ConfigurationTests.Provider(options =>
        {
            options.Configuration = broker.Configuration();
            options.SuperStreams.Add(new(name)
            {
                Configuration = spec,
            });
        });
        await using var native = await StreamSystem.Create(broker.Configuration());
        var transport = provider.GetRequiredService<ITransport>();

        await Assert.That((await transport.ApplyTopologyAsync(TopologyMode.Provision, default)).Differences.Count).IsEqualTo(0);
        await Assert.That((await transport.ApplyTopologyAsync(TopologyMode.Provision, default)).Differences.Count).IsEqualTo(0);
        await Assert.That(await native.QueryPartition(name)).IsEquivalentTo(new[] { $"{name}-a", $"{name}-b" });
        var queues = await broker.CommandAsync(["rabbitmqctl", "list_queues", "name", "arguments", "--formatter=json"], default);
        await Assert.That(queues).Contains("987654321");

        await using var changed = ConfigurationTests.Provider(options =>
        {
            options.Configuration = broker.Configuration();
            options.SuperStreams.Add(new(name)
            {
                Configuration = new PartitionsSuperStreamSpec(name, 3),
            });
        });
        var changedTransport = changed.GetRequiredService<ITransport>();

        await Assert.That((await changedTransport.ApplyTopologyAsync(TopologyMode.Provision, default)).Differences.Count).IsEqualTo(0);
        await Assert.That(await native.QueryPartition(name)).IsEquivalentTo(new[] { $"{name}-a", $"{name}-b" });
        var validation = await changedTransport.ApplyTopologyAsync(TopologyMode.Validate, default);
        await Assert.That(validation.Differences.Single()).Contains("configuration equivalence checks are unavailable");
    }

    [Test]
    public async Task Name_only_topology_checks_native_partitions_without_creating_any()
    {
        var broker = await RabbitMqStreamsTestBroker.Current;
        var name = TestContext.Current!.Isolation.GetIsolatedName("events");
        var missing = TestContext.Current!.Isolation.GetIsolatedName("missing");
        const string EmptyExchange = "amq.direct";
        await using var native = await StreamSystem.Create(broker.Configuration());
        await native.CreateSuperStream(new PartitionsSuperStreamSpec(name, 1));
        await using var provider = ConfigurationTests.Provider(options =>
        {
            options.Configuration = broker.Configuration();
            options.SuperStreams.Add(new(name));
            options.SuperStreams.Add(new(missing));
            options.SuperStreams.Add(new(EmptyExchange));
        });
        var transport = provider.GetRequiredService<ITransport>();

        foreach (var mode in new[] { TopologyMode.Validate, TopologyMode.Provision })
        {
            var result = await transport.ApplyTopologyAsync(mode, default);
            await Assert.That(result.Differences.Count).IsEqualTo(2);
            await Assert.That(result.Differences[0]).Contains(missing);
            await Assert.That(result.Differences[1]).Contains(EmptyExchange);
        }

        await Assert.That(await native.SuperStreamExists(missing)).IsFalse();
    }
}

internal static class RabbitMqStreamsTestBroker
{
    private static Lazy<Task<TopologyTests.Broker>> s_shared = new(StartAsync);

    internal static Task<TopologyTests.Broker> Current => Volatile.Read(ref s_shared).Value;

    private static async Task<TopologyTests.Broker> StartAsync()
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(3));
        return await TopologyTests.Broker.StartAsync(timeout.Token);
    }

    [After(TestSession)]
    public static async Task StopAsync()
    {
        var shared = Interlocked.Exchange(ref s_shared, new(StartAsync));

        if (!shared.IsValueCreated)
        {
            return;
        }

        TopologyTests.Broker broker;

        try
        {
            broker = await shared.Value;
        }
        catch
        {
            return;
        } // Failed startup already disposes its container.

        await broker.DisposeAsync();
    }
}
