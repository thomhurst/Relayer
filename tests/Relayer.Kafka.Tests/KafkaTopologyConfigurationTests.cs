using Dekaf.Admin;

namespace Relayer.Kafka.Tests;

public class KafkaTopologyConfigurationTests
{
    [Test]
    public async Task None_skips_native_admin_calls_even_with_explicit_topics()
    {
        var options = new KafkaTransportOptions { ConfigureAdmin = _ => throw new InvalidOperationException("Unexpected admin client.") };
        options.Topics.Add(new()
        {
            Name = "orders"
        });
        await Assert.That((await KafkaTopology.ApplyAsync("events", options, ["orders"], TopologyMode.None, default))
            .Differences.Count).IsEqualTo(0);
    }

    [Test]
    [Arguments(TopologyMode.Validate)]
    [Arguments(TopologyMode.Provision)]
    public async Task Cancellation_is_observed_before_native_admin_calls(TopologyMode mode)
    {
        using var cancellation = new CancellationTokenSource();
        cancellation.Cancel();
        await Assert.That(async () => await KafkaTopology.ApplyAsync("events", null, ["orders"], mode, cancellation.Token))
            .Throws<OperationCanceledException>();
    }

    [Test]
    public async Task Invalid_mode_fails_before_native_admin_calls()
    {
        await Assert.That(async () => await KafkaTopology.ApplyAsync("events", null, [], (TopologyMode)99, default))
            .Throws<ArgumentOutOfRangeException>();
    }

    [Test]
    [Arguments(TopologyMode.Validate)]
    [Arguments(TopologyMode.Provision)]
    public async Task Duplicate_declarations_fail_before_native_admin_calls(TopologyMode mode)
    {
        var options = new KafkaTransportOptions();
        options.Topics.Add(new NewTopic { Name = "orders" });
        options.Topics.Add(new NewTopic { Name = "orders", NumPartitions = 3 });
        await Assert.That(async () => await KafkaTopology.ApplyAsync("events", options, [], mode, default))
            .Throws<ArgumentException>().WithMessageContaining("duplicate topic declaration 'orders'");
    }

    [Test]
    public async Task Provision_rejects_native_validate_only_before_native_admin_calls()
    {
        var options = new KafkaTransportOptions { TopicCreationOptions = new() { ValidateOnly = true } };
        await Assert.That(async () => await KafkaTopology.ApplyAsync("events", options, ["orders"], TopologyMode.Provision, default))
            .Throws<ArgumentException>().WithMessageContaining("TopicCreationOptions.ValidateOnly = false");
    }
}
