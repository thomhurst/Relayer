using System.Text;
using System.Text.Json;
using Amazon.SimpleNotificationService.Model;
using Amazon.SQS.Model;
using Microsoft.Extensions.DependencyInjection;
using Relayer.AmazonSqs;

namespace Relayer.Tests;

public class AmazonTerraformTests
{
    private const string TopicArn = "arn:aws:sns:eu-west-1:123456789012:orders.fifo";
    private const string QueueArn = "arn:aws:sqs:eu-west-1:123456789012:orders.fifo";

    [Test]
    public async Task Exports_native_fifo_tags_policies_and_relationships_without_clients()
    {
        const string policy = "{\"Version\":\"2012-10-17\",\"Statement\":[{\"Effect\":\"Allow\",\"Principal\":{\"Service\":\"sns.amazonaws.com\"},\"Action\":\"sqs:SendMessage\",\"Resource\":\"" + QueueArn + "\",\"Condition\":{\"ArnEquals\":{\"aws:SourceArn\":\"" + TopicArn + "\"}}}]}";
        using var provider = Create(options =>
        {
            options.QueueTopology.Queues.Add(new CreateQueueRequest("orders.fifo")
            {
                Attributes = new() { ["FifoQueue"] = "true", ["VisibilityTimeout"] = "45", ["Policy"] = policy },
                Tags = new() { ["purpose"] = "line\n\"quoted\"\\${var.secret}%{if true}" }
            });
            options.Topics.Add(new CreateTopicRequest("orders.fifo")
            {
                Attributes = new() { ["FifoTopic"] = "true", ["ContentBasedDeduplication"] = "false" },
                Tags = [new Tag { Key = "environment", Value = "test" }]
            });
            options.Subscriptions.Add(new SubscribeRequest
            {
                TopicArn = TopicArn, Endpoint = QueueArn, Protocol = "sqs", ReturnSubscriptionArn = true,
                Attributes = new() { ["RawMessageDelivery"] = "true", ["FilterPolicy"] = "{\"kind\":[\"order\"]}" }
            });
        });
        using var document = JsonDocument.Parse(Export(provider));
        var resources = document.RootElement.GetProperty("resource");
        var queue = resources.GetProperty("aws_sqs_queue").EnumerateObject().Single();
        var topic = resources.GetProperty("aws_sns_topic").EnumerateObject().Single();
        var access = resources.GetProperty("aws_sqs_queue_policy").EnumerateObject().Single();
        var subscription = resources.GetProperty("aws_sns_topic_subscription").EnumerateObject().Single().Value;
        await Assert.That(queue.Value.GetProperty("fifo_queue").GetBoolean()).IsTrue();
        await Assert.That(queue.Value.GetProperty("visibility_timeout_seconds").GetInt32()).IsEqualTo(45);
        await Assert.That(queue.Value.GetProperty("tags").GetProperty("purpose").GetString()).IsEqualTo("line\n\"quoted\"\\$${var.secret}%%{if true}");
        await Assert.That(topic.Value.GetProperty("fifo_topic").GetBoolean()).IsTrue();
        await Assert.That(access.Value.GetProperty("policy").GetString()).IsEqualTo(policy);
        await Assert.That(access.Value.GetProperty("queue_url").GetString()).IsEqualTo("${aws_sqs_queue." + queue.Name + ".id}");
        await Assert.That(subscription.GetProperty("topic_arn").GetString()).IsEqualTo("${aws_sns_topic." + topic.Name + ".arn}");
        await Assert.That(subscription.GetProperty("endpoint").GetString()).IsEqualTo("${aws_sqs_queue." + queue.Name + ".arn}");
        await Assert.That(subscription.GetProperty("depends_on")[0].GetString()).IsEqualTo("aws_sqs_queue_policy." + access.Name);
        await Assert.That(subscription.GetProperty("raw_message_delivery").GetBoolean()).IsTrue();
    }

    [Test]
    [Arguments(null, 1_048_576)]
    [Arguments("262144", 262_144)]
    [Arguments("1048576", 1_048_576)]
    public async Task Queue_message_size_preserves_native_default_and_explicit_values(string? maximumMessageSize, int expected)
    {
        using var provider = Create(options =>
        {
            var queue = new CreateQueueRequest("queue");
            if (maximumMessageSize is not null)
                queue.Attributes = new() { ["MaximumMessageSize"] = maximumMessageSize };
            options.QueueTopology.Queues.Add(queue);
        });
        using var document = JsonDocument.Parse(Export(provider));
        var queue = document.RootElement.GetProperty("resource").GetProperty("aws_sqs_queue").EnumerateObject().Single().Value;
        await Assert.That(queue.GetProperty("max_message_size").GetInt32()).IsEqualTo(expected);
    }

    [Test]
    public async Task Output_is_independent_of_declaration_and_dictionary_order_and_preserves_json_export()
    {
        using var first = Create(options =>
        {
            options.Topics.Add(new("z")); options.Topics.Add(new("a"));
            options.QueueTopology.Queues.Add(new("queue") { Attributes = new() { ["DelaySeconds"] = "2", ["VisibilityTimeout"] = "3" } });
        });
        using var second = Create(options =>
        {
            options.QueueTopology.Queues.Add(new("queue") { Attributes = new() { ["VisibilityTimeout"] = "3", ["DelaySeconds"] = "2" } });
            options.Topics.Add(new("a")); options.Topics.Add(new("z"));
        });
        var topology = first.GetRequiredService<ITopology>();
        using var before = new MemoryStream(); topology.ExportJson(before);
        await Assert.That(Export(first)).IsEqualTo(Export(second));
        using var after = new MemoryStream(); topology.ExportJson(after);
        await Assert.That(Convert.ToHexString(before.ToArray())).IsEqualTo(Convert.ToHexString(after.ToArray()));
    }

    [Test]
    public async Task Standalone_sqs_and_dead_letter_dependencies_use_existing_declarations()
    {
        var services = new ServiceCollection();
        services.AddRelayer(builder => builder.AmazonSqs("sqs", transport => transport.Options.Configure(options =>
        {
            options.Queues.Add(new("dead"));
            options.Queues.Add(new("live") { Attributes = new() { ["RedrivePolicy"] = "{\"deadLetterTargetArn\":\"arn:aws:sqs:eu-west-1:123456789012:dead\",\"maxReceiveCount\":5}" } });
        })));
        using var provider = services.BuildServiceProvider();
        using var document = JsonDocument.Parse(Export(provider));
        var queues = document.RootElement.GetProperty("resource").GetProperty("aws_sqs_queue").EnumerateObject().ToArray();
        var dead = queues.Single(queue => queue.Value.GetProperty("name").GetString() == "dead");
        var live = queues.Single(queue => queue.Value.GetProperty("name").GetString() == "live");
        await Assert.That(live.Value.GetProperty("depends_on")[0].GetString()).IsEqualTo("aws_sqs_queue." + dead.Name);
    }

    [Test]
    [Arguments("Unknown", "value")]
    [Arguments("FifoTopic", "yes")]
    [Arguments("SignatureVersion", "-1")]
    [Arguments("Policy", "not json")]
    public async Task Unsupported_or_malformed_native_attributes_leave_output_untouched(string key, string value)
    {
        using var provider = Create(options =>
        {
            options.QueueTopology.Queues.Add(new("valid"));
            options.Topics.Add(new("invalid") { Attributes = new() { [key] = value } });
        });
        using var output = new MemoryStream(); output.WriteByte(42);
        await Assert.That(() => provider.GetRequiredService<ITopology>().ExportAmazonTerraform(provider, output, Options()))
            .Throws<InvalidOperationException>().WithMessageContaining(key);
        await Assert.That(output.Length).IsEqualTo(1);
        await Assert.That(output.Position).IsEqualTo(1);
    }

    [Test]
    public async Task Rejects_unsupported_topic_request_settings_and_duplicate_entities()
    {
        using var policy = Create(options => options.Topics.Add(new("topic") { DataProtectionPolicy = "{}" }));
        await Assert.That(() => Export(policy)).Throws<InvalidOperationException>().WithMessageContaining("DataProtectionPolicy");
        using var duplicate = Create(options => { options.Topics.Add(new("same")); options.Topics.Add(new("same")); });
        await Assert.That(() => Export(duplicate)).Throws<InvalidOperationException>().WithMessageContaining("Duplicate topic");
    }

    [Test]
    public async Task External_arns_remain_literal_and_non_sqs_subscriptions_fail()
    {
        using var external = Create(options => options.Subscriptions.Add(new() { TopicArn = TopicArn, Endpoint = QueueArn, Protocol = "sqs" }));
        using var document = JsonDocument.Parse(Export(external));
        var subscription = document.RootElement.GetProperty("resource").GetProperty("aws_sns_topic_subscription").EnumerateObject().Single().Value;
        await Assert.That(subscription.GetProperty("topic_arn").GetString()).IsEqualTo(TopicArn);
        await Assert.That(subscription.GetProperty("endpoint").GetString()).IsEqualTo(QueueArn);
        using var unsupported = Create(options => options.Subscriptions.Add(new() { Protocol = "email" }));
        await Assert.That(() => Export(unsupported)).Throws<InvalidOperationException>().WithMessageContaining("Protocol=sqs");
    }

    [Test]
    public async Task Native_region_mismatch_fails_offline()
    {
        using var provider = Create(options => options.Sns.RegionEndpoint = Amazon.RegionEndpoint.USEast1);
        await Assert.That(() => Export(provider)).Throws<InvalidOperationException>().WithMessageContaining("differs from export region");
    }

    [Test]
    public async Task Unsupported_queue_settings_and_other_transports_fail_before_output()
    {
        using var unsupported = Create(options => options.QueueTopology.Queues.Add(new("queue")
        {
            Attributes = new() { ["RedriveAllowPolicy"] = "{}" }
        }));
        await Assert.That(() => Export(unsupported)).Throws<InvalidOperationException>().WithMessageContaining("RedriveAllowPolicy");
        var services = new ServiceCollection();
        services.AddRelayer(builder => builder.AddTransport<OtherOptions, OtherOptions, OtherOptions>("other", _ => { }));
        using var provider = services.BuildServiceProvider();
        using var output = new MemoryStream();
        await Assert.That(() => provider.GetRequiredService<ITopology>().ExportAmazonTerraform(provider, output, Options()))
            .Throws<InvalidOperationException>().WithMessageContaining("not SQS/SNS");
        await Assert.That(output.Length).IsEqualTo(0);
    }

    private sealed class OtherOptions;

    private static AmazonTerraformOptions Options() => new() { AccountId = "123456789012", Region = "eu-west-1" };
    private static ServiceProvider Create(Action<AmazonSnsTransportOptions> configure)
    {
        var services = new ServiceCollection();
        services.AddRelayer(builder => builder.AmazonSns("sns", transport => transport.Options.Configure(configure)));
        return services.BuildServiceProvider();
    }
    private static string Export(ServiceProvider provider)
    {
        using var stream = new MemoryStream();
        provider.GetRequiredService<ITopology>().ExportAmazonTerraform(provider, stream, Options());
        return Encoding.UTF8.GetString(stream.ToArray());
    }
}
