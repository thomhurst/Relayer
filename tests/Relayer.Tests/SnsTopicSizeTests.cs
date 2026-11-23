using System.Globalization;
using Relayer.AmazonSqs;
using Relayer.Transports;

namespace Relayer.Tests;

public class SnsTopicSizeTests
{
    [Test]
    public async Task Native_topic_limit_applies_to_single_messages_and_batch_totals()
    {
        using var client = new SnsTestClient();
        client.TopicMaximumSizes[SnsTestClient.Arn("orders")] = "1048576";
        await using var transport = new SnsSendTransport("sns", client, false, SnsSendTests.Options(client));
        await transport.StartAsync(default);

        await Assert.That(transport.GetCapabilities("orders").MaxMessageBytes).IsEqualTo(1_048_576L);
        await Assert.That(transport.GetCapabilities("ordered.fifo").MaxMessageBytes).IsEqualTo(262_144L);
        var message = new OutgoingMessage("orders", new byte[300 * 1024]);
        await transport.SendAsync(message, default);
        await transport.SendBatchAsync(new[] { message, message, message, message }, default);

        await Assert.That(client.Sent.Count).IsEqualTo(1);
        await Assert.That(client.Batches.Select(batch => batch.Length).SequenceEqual(new[] { 3, 1 })).IsTrue();
    }

    [Test]
    public async Task Topic_limit_counts_attributes_and_refreshes_from_topology()
    {
        using var client = new SnsTestClient();
        var options = SnsSendTests.Options(client);
        options.Topics[0].Attributes = new()
        {
            ["MaximumMessageSize"] = "1048576"
        };
        await using var transport = new SnsSendTransport("sns", client, false, options);
        await Assert.That(transport.GetCapabilities("orders").MaxMessageBytes).IsEqualTo(1_048_576L);
        await transport.StartAsync(default);
        await Assert.That(transport.GetCapabilities("orders").MaxMessageBytes).IsEqualTo(262_144L);

        const int Maximum = 1024;
        client.TopicMaximumSizes[SnsTestClient.Arn("orders")] = Maximum.ToString(CultureInfo.InvariantCulture);
        var report = await transport.ApplyTopologyAsync(TopologyMode.Validate, default);
        await Assert.That(report.Differences.Single()).Contains("MaximumMessageSize differs");
        await Assert.That(transport.GetCapabilities("orders").MaxMessageBytes).IsEqualTo((long)Maximum);

        KeyValuePair<string, WireString>[] attributes = [new("h", new("é"))];
        var exact = new OutgoingMessage("orders", new byte[Maximum - 9], attributes);
        var oversized = new OutgoingMessage("orders", new byte[Maximum - 8], attributes);
        await transport.SendAsync(exact, default);
        await Assert.That(async () => await transport.SendAsync(oversized, default)).Throws<InvalidOperationException>();
        await Assert.That(async () => await transport.SendBatchAsync(new[] { oversized }, default)).Throws<InvalidOperationException>();
        await transport.SendBatchAsync(new[] { exact, exact }, default);

        await Assert.That(client.Sent.Count).IsEqualTo(1);
        await Assert.That(client.Batches.Select(batch => batch.Length).SequenceEqual(new[] { 1, 1 })).IsTrue();
    }

    [Test]
    [Arguments("262144", null, null)]
    [Arguments("262144", "262144", null)]
    [Arguments("262144", "524288", "524288")]
    [Arguments("262144", "1048576", "1048576")]
    [Arguments("524288", null, "262144")]
    public async Task Topology_compares_and_reports_effective_topic_size(string declared, string? observed, string? differentSize)
    {
        using var client = new SnsTestClient();
        var options = SnsSendTests.Options(client);
        options.Topics[0].Attributes = new()
        {
            ["MaximumMessageSize"] = declared
        };

        if (observed is not null)
        {
            client.TopicMaximumSizes[SnsTestClient.Arn("orders")] = observed;
        }

        await using var transport = new SnsSendTransport("sns", client, false, options);

        foreach (var mode in new[] { TopologyMode.Validate, TopologyMode.Provision })
        {
            var report = await transport.ApplyTopologyAsync(mode, default);

            if (differentSize is null)
            {
                await Assert.That(report.Differences).IsEmpty();
            }
            else
            {
                await Assert.That(report.Differences.Single())
                    .Contains($"MaximumMessageSize differs (expected '{declared}', observed '{differentSize}')");
            }
        }

        await Assert.That(client.Creates).IsEmpty();
    }

    [Test]
    public async Task Batch_keeps_its_topic_limit_through_a_topology_refresh()
    {
        using var client = new SnsTestClient();
        client.TopicMaximumSizes[SnsTestClient.Arn("orders")] = "1048576";
        await using var transport = new SnsSendTransport("sns", client, false, SnsSendTests.Options(client));
        await transport.StartAsync(default);
        client.Gate = new(TaskCreationOptions.RunContinuationsAsynchronously);
        var message = new OutgoingMessage("orders", new byte[300 * 1024]);
        var send = transport.SendBatchAsync(new[] { message, message, message, message, message }, default).AsTask();

        try
        {
            await Assert.That(client.Batches.Single().Length).IsEqualTo(3);
            client.TopicMaximumSizes[SnsTestClient.Arn("orders")] = "262144";
            await transport.ApplyTopologyAsync(TopologyMode.Validate, default);
            await Assert.That(transport.GetCapabilities("orders").MaxMessageBytes).IsEqualTo(262_144L);
        }
        finally
        {
            client.Gate.SetResult();
            await send;
        }

        await Assert.That(client.Batches.Select(batch => batch.Length).SequenceEqual(new[] { 3, 2 })).IsTrue();
        await Assert.That(async () => await transport.SendAsync(message, default)).Throws<InvalidOperationException>();
    }
}
