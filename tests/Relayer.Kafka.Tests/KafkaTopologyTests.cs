using Dekaf;
using Dekaf.Admin;
using Dekaf.Consumer;
using Dekaf.Errors;
using Dekaf.Protocol;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Relayer.Transports;

namespace Relayer.Kafka.Tests;

[ClassDataSource<KafkaHeaderSnapshotBroker>(Shared = SharedType.PerTestSession)]
public class KafkaTopologyTests(KafkaHeaderSnapshotBroker broker)
{
    [Test]
    public async Task Validate_reports_missing_send_receive_and_dead_letter_topics_without_creating_them()
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(2));
        var token = timeout.Token;
        await broker.Started.WaitAsync(token);
        var send = TestContext.Current!.Isolation.GetIsolatedName("topology").ToLowerInvariant();
        var receive = send + "-receive";
        var deadLetter = receive + ".dead-letter";
        var configured = 0;
        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddRelayer(relayer => relayer.Kafka("events", kafka =>
        {
            kafka.Options.Configure(options =>
            {
                options.BootstrapServers.Add(broker.Bootstrap);
                options.ConfigureAdmin = admin => { configured++; admin.WithClientId("topology-test"); };
            });
            kafka.Send<Order>(send);
            kafka.Consume(receive, consumer =>
            {
                consumer.Options.Group = send;
                consumer.Handler<KafkaTopologyHandler>();
            });
        }));
        using var host = builder.Build();
        var transport = host.Services.GetServices<ITransport>().Single();
        await Assert.That((await transport.ApplyTopologyAsync(TopologyMode.None, token)).Differences.Count).IsEqualTo(0);
        await Assert.That(configured).IsEqualTo(0);
        using var cancelled = new CancellationTokenSource();
        cancelled.Cancel();
        await Assert.That(async () => await transport.ApplyTopologyAsync(TopologyMode.Validate, cancelled.Token))
            .Throws<OperationCanceledException>();
        await Assert.That(configured).IsEqualTo(0);
        var missing = await transport.ApplyTopologyAsync(TopologyMode.Validate, token);
        await Assert.That(missing.Differences.Count).IsEqualTo(3);
        foreach (var topic in new[] { send, receive, deadLetter })
            await Assert.That(missing.Differences.Any(diff => diff.Contains("'" + topic + "'", StringComparison.Ordinal))).IsTrue();
        await using var admin = new AdminClientBuilder().WithBootstrapServers(broker.Bootstrap).Build();
        var listed = await admin.ListTopicsAsync(cancellationToken: token);
        await Assert.That(listed.Any(topic => topic.Name == send || topic.Name == receive || topic.Name == deadLetter)).IsFalse();
        await admin.CreateTopicsAsync([new() { Name = send, NumPartitions = 1, ReplicationFactor = 1 },
            new() { Name = receive, NumPartitions = 1, ReplicationFactor = 1 },
            new() { Name = deadLetter, NumPartitions = 1, ReplicationFactor = 1 }], cancellationToken: token);
        try
        {
            await Assert.That((await transport.ApplyTopologyAsync(TopologyMode.Validate, token)).Differences.Count).IsEqualTo(0);
            // Each of the two Validate calls builds one native admin client.
            await Assert.That(configured).IsEqualTo(2);
        }
        finally { await admin.DeleteTopicsAsync([send, receive, deadLetter], cancellationToken: token); }
    }

    [Test]
    public async Task Provision_starts_consumers_after_creating_send_receive_and_dead_letter_topics()
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(2));
        var token = timeout.Token;
        await broker.Started.WaitAsync(token);
        var topic = TopicName();
        var sendOnly = topic + "-send";
        var handled = new TaskCompletionSource<Order>(TaskCreationOptions.RunContinuationsAsynchronously);
        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddSingleton(handled);
        builder.Services.AddRelayer(relayer =>
        {
            relayer.AddMessageTypesFromThisAssembly();
            relayer.UseJsonSerializer(KafkaJson.Default);
            relayer.Kafka("events", kafka =>
            {
                kafka.TopologyMode = TopologyMode.Provision;
                kafka.Options.Configure(options =>
                {
                    options.BootstrapServers.Add(broker.Bootstrap);
                    options.ConfigureConsumer = consumer => consumer.WithAutoOffsetReset(AutoOffsetReset.Earliest);
                });
                kafka.Send<Order>(topic);
                kafka.Send<OtherOrder>(sendOnly);
                kafka.Consume(topic, consumer =>
                {
                    consumer.Options.Group = topic;
                    consumer.Handler<KafkaProvisioningHandler>();
                });
            });
        });
        using var host = builder.Build();
        try
        {
            await host.StartAsync(token);
            await host.Services.GetRequiredService<ISender>().SendAsync(new Order("provisioned", 1), token);
            await Assert.That(await handled.Task.WaitAsync(token)).IsEqualTo(new Order("provisioned", 1));
            await using var admin = Admin();
            await admin.ListTopicsAsync(cancellationToken: token);
            foreach (var name in new[] { topic, sendOnly, topic + ".dead-letter" })
            {
                var actual = admin.Metadata.GetTopic(name)!;
                await Assert.That(actual.PartitionCount).IsEqualTo(1);
                await Assert.That(actual.Partitions[0].ReplicaNodes.Count).IsEqualTo(1);
            }
        }
        finally
        {
            using var cleanup = new CancellationTokenSource(TimeSpan.FromSeconds(30));
            await host.StopAsync(cleanup.Token);
            await using var admin = Admin();
            await admin.DeleteTopicsAsync([topic, sendOnly, topic + ".dead-letter"], cancellationToken: cleanup.Token);
        }
    }

    [Test]
    [Arguments(2)]
    [Arguments(4)]
    [Arguments(8)]
    public async Task Provision_passes_native_settings_and_is_idempotent_with_concurrent_callers(int callers)
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(2));
        var token = timeout.Token;
        await broker.Started.WaitAsync(token);
        var topic = TopicName();
        using var host = CreateHost(kafka => kafka.Options.Configure(options => options.Topics.Add(new()
        {
            Name = topic,
            NumPartitions = 3,
            ReplicationFactor = 1,
            Configs = new Dictionary<string, string> { ["retention.ms"] = "86400000" },
        })));
        var transport = host.Services.GetServices<ITransport>().Single();
        try
        {
            var start = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var requests = new Task<TopologyReport>[callers];
            for (var i = 0; i < callers; i++) requests[i] = ProvisionAsync();
            start.SetResult();
            var reports = await Task.WhenAll(requests);
            foreach (var report in reports) await Assert.That(report.Differences).IsEmpty();
            await Assert.That((await transport.ApplyTopologyAsync(TopologyMode.Provision, token)).Differences.Count).IsEqualTo(0);
            await using var admin = Admin();
            await admin.ListTopicsAsync(cancellationToken: token);
            await Assert.That(admin.Metadata.GetTopic(topic)!.PartitionCount).IsEqualTo(3);
            var resource = ConfigResource.Topic(topic);
            var configs = await admin.DescribeConfigsAsync([resource], cancellationToken: token);
            await Assert.That(configs[resource].Single(entry => entry.Name == "retention.ms").Value).IsEqualTo("86400000");

            async Task<TopologyReport> ProvisionAsync()
            {
                await start.Task;
                return await transport.ApplyTopologyAsync(TopologyMode.Provision, token);
            }
        }
        finally
        {
            await using var admin = Admin();
            await admin.DeleteTopicsAsync([topic], cancellationToken: token);
        }
    }

    [Test]
    [Arguments(TopologyMode.Validate)]
    [Arguments(TopologyMode.Provision)]
    public async Task Explicit_settings_report_differences_without_modifying_existing_topics(TopologyMode mode)
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(2));
        var token = timeout.Token;
        await broker.Started.WaitAsync(token);
        var topic = TopicName();
        await using var admin = Admin();
        await admin.CreateTopicsAsync([new()
        {
            Name = topic, NumPartitions = 2, ReplicationFactor = 1,
            Configs = new Dictionary<string, string> { ["retention.ms"] = "86400000" },
        }], cancellationToken: token);
        using var host = CreateHost(kafka => kafka.Options.Configure(options => options.Topics.Add(new()
        {
            Name = topic, NumPartitions = 3, ReplicationFactor = 2,
            Configs = new Dictionary<string, string> { ["retention.ms"] = "60000" },
        })));
        try
        {
            var report = await host.Services.GetServices<ITransport>().Single().ApplyTopologyAsync(mode, token);
            await Assert.That(report.Differences.Count).IsEqualTo(4);
            await Assert.That(report.Differences.Any(diff => diff.Contains("NumPartitions expected '3', actual '2'"))).IsTrue();
            await Assert.That(report.Differences.Count(diff => diff.Contains("ReplicationFactor expected '2', actual '1'"))).IsEqualTo(2);
            await Assert.That(report.Differences.Any(diff => diff.Contains("retention.ms expected '60000', actual '86400000'"))).IsTrue();
            // A fresh client verifies both modes preserved the topic and its settings.
            await using var verification = Admin();
            await verification.ListTopicsAsync(cancellationToken: token);
            await Assert.That(verification.Metadata.GetTopic(topic)!.PartitionCount).IsEqualTo(2);
            var resource = ConfigResource.Topic(topic);
            var configs = await verification.DescribeConfigsAsync([resource], cancellationToken: token);
            await Assert.That(configs[resource].Single(entry => entry.Name == "retention.ms").Value).IsEqualTo("86400000");
        }
        finally { await admin.DeleteTopicsAsync([topic], cancellationToken: token); }
    }

    [Test]
    public async Task Validate_checks_explicit_topics_without_creating_them()
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(2));
        var token = timeout.Token;
        await broker.Started.WaitAsync(token);
        var topic = TopicName();
        using var host = CreateHost(kafka => kafka.Options.Configure(options => options.Topics.Add(new() { Name = topic })));
        var report = await host.Services.GetServices<ITransport>().Single().ApplyTopologyAsync(TopologyMode.Validate, token);
        await Assert.That(report.Differences.Single()).Contains(topic);
        await using var admin = Admin();
        await Assert.That((await admin.ListTopicsAsync(cancellationToken: token)).Any(item => item.Name == topic)).IsFalse();
    }

    [Test]
    public async Task Provision_keeps_existing_route_topics_without_enforcing_creation_defaults()
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(2));
        var token = timeout.Token;
        await broker.Started.WaitAsync(token);
        var topic = TopicName();
        await using var admin = Admin();
        await admin.CreateTopicsAsync([new() { Name = topic, NumPartitions = 3 }], cancellationToken: token);
        using var host = CreateHost(kafka => kafka.Send<Order>(topic));
        try
        {
            await Assert.That((await host.Services.GetServices<ITransport>().Single()
                .ApplyTopologyAsync(TopologyMode.Provision, token)).Differences.Count).IsEqualTo(0);
        }
        finally { await admin.DeleteTopicsAsync([topic], cancellationToken: token); }
    }

    [Test]
    public async Task Native_replica_assignments_override_counts_and_are_verified()
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(2));
        var token = timeout.Token;
        await broker.Started.WaitAsync(token);
        var topic = TopicName();
        await using var admin = Admin();
        await admin.ListTopicsAsync(cancellationToken: token);
        var brokerId = admin.Metadata.GetBrokers().First().NodeId;
        using var host = CreateHost(kafka => kafka.Options.Configure(options => options.Topics.Add(new()
        {
            Name = topic, NumPartitions = -10, ReplicationFactor = -10,
            ReplicaAssignments = new Dictionary<int, IReadOnlyList<int>> { [0] = new[] { brokerId }, [1] = new[] { brokerId } },
        })));
        try
        {
            await Assert.That((await host.Services.GetServices<ITransport>().Single()
                .ApplyTopologyAsync(TopologyMode.Provision, token)).Differences.Count).IsEqualTo(0);
            using var mismatch = CreateHost(kafka => kafka.Options.Configure(options => options.Topics.Add(new()
            {
                Name = topic,
                ReplicaAssignments = new Dictionary<int, IReadOnlyList<int>> { [0] = new[] { brokerId + 1 }, [1] = new[] { brokerId } },
            })));
            var report = await mismatch.Services.GetServices<ITransport>().Single().ApplyTopologyAsync(TopologyMode.Validate, token);
            await Assert.That(report.Differences.Single()).Contains($"partition 0 replicas expected [{brokerId + 1}], actual [{brokerId}]");
        }
        finally { await admin.DeleteTopicsAsync([topic], cancellationToken: token); }
    }

    [Test]
    public async Task Native_broker_defaults_are_not_compared_to_literal_minus_one()
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(2));
        var token = timeout.Token;
        await broker.Started.WaitAsync(token);
        var topic = TopicName();
        using var host = CreateHost(kafka => kafka.Options.Configure(options => options.Topics.Add(new()
        {
            Name = topic, NumPartitions = -1, ReplicationFactor = -1,
        })));
        try
        {
            var transport = host.Services.GetServices<ITransport>().Single();
            await Assert.That((await transport.ApplyTopologyAsync(TopologyMode.Provision, token)).Differences.Count).IsEqualTo(0);
            await Assert.That((await transport.ApplyTopologyAsync(TopologyMode.Validate, token)).Differences.Count).IsEqualTo(0);
        }
        finally
        {
            await using var admin = Admin();
            await admin.DeleteTopicsAsync([topic], cancellationToken: token);
        }
    }

    [Test]
    public async Task Provision_propagates_native_creation_errors()
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(2));
        var token = timeout.Token;
        await broker.Started.WaitAsync(token);
        using var host = CreateHost(kafka => kafka.Options.Configure(options => options.Topics.Add(new()
        {
            Name = TopicName(), ReplicationFactor = 2, // The shared broker has one node.
        })));
        var error = await Assert.ThrowsAsync<KafkaException>(async () =>
            await host.Services.GetServices<ITransport>().Single().ApplyTopologyAsync(TopologyMode.Provision, token));
        await Assert.That(error!.ErrorCode).IsEqualTo(ErrorCode.InvalidReplicationFactor);
    }

    [Test]
    public async Task Provision_bounds_verification_with_the_native_creation_timeout()
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(2));
        var token = timeout.Token;
        await broker.Started.WaitAsync(token);
        var topic = TopicName();
        await using var admin = Admin();
        await admin.CreateTopicsAsync([new() { Name = topic }], cancellationToken: token);
        using var host = CreateHost(kafka =>
        {
            kafka.Send<Order>(topic);
            kafka.Options.Configure(options => options.TopicCreationOptions = new() { TimeoutMs = 0 });
        });
        try
        {
            await Assert.That(async () => await host.Services.GetServices<ITransport>().Single()
                .ApplyTopologyAsync(TopologyMode.Provision, token))
                .Throws<TimeoutException>().WithMessageContaining("within 0 ms");
            await Assert.That(token.IsCancellationRequested).IsFalse();
        }
        finally { await admin.DeleteTopicsAsync([topic], cancellationToken: token); }
    }

    private static string TopicName() => TestContext.Current!.Isolation.GetIsolatedName("topology").ToLowerInvariant();

    private IAdminClient Admin() => new AdminClientBuilder().WithBootstrapServers(broker.Bootstrap).Build();

    private IHost CreateHost(Action<TransportBuilder<KafkaTransportOptions, KafkaConsumerOptions>> configure)
    {
        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddRelayer(relayer => relayer.Kafka("events", kafka =>
        {
            kafka.Options.Configure(options => options.BootstrapServers.Add(broker.Bootstrap));
            configure(kafka);
        }));
        return builder.Build();
    }
}

public sealed class KafkaTopologyHandler : IHandler<Order>
{
    public ValueTask HandleAsync(Order message, MessageContext context, CancellationToken cancellationToken) => default;
}

public sealed class KafkaProvisioningHandler(TaskCompletionSource<Order> handled) : IHandler<Order>
{
    public ValueTask HandleAsync(Order message, MessageContext context, CancellationToken cancellationToken)
    {
        handled.TrySetResult(message);
        return default;
    }
}
