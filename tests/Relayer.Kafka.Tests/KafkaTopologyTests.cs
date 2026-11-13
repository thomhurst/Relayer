using Dekaf;
using Dekaf.Admin;
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
        await Assert.That(async () => await transport.ApplyTopologyAsync(TopologyMode.Provision, token))
            .Throws<NotSupportedException>().WithMessageContaining("Create topics externally");
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
            await Assert.That(configured).IsEqualTo(2);
        }
        finally { await admin.DeleteTopicsAsync([send, receive, deadLetter], cancellationToken: token); }
    }
}

public sealed class KafkaTopologyHandler : IHandler<Order>
{
    public ValueTask HandleAsync(Order message, MessageContext context, CancellationToken cancellationToken) => default;
}
