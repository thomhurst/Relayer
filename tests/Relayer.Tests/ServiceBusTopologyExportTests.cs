using Relayer.Transports;
using System.Text;
using Azure.Messaging.ServiceBus.Administration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Relayer.AzureServiceBus;

namespace Relayer.Tests;

public sealed class ServiceBusTopologyExportTests
{
    [Test]
    [Arguments(ServiceBusTopologyFormat.Bicep)]
    [Arguments(ServiceBusTopologyFormat.Terraform)]
    public async Task Export_is_deterministic_offline_and_preserves_native_properties(ServiceBusTopologyFormat format)
    {
        using var provider = Provider(options =>
        {
            // Neither this invalid connection string nor any runtime adapter may be resolved by export.
            options.ConnectionString = "not a connection string";
            options.Queues.Add(new("orders")
            {
                AutoDeleteOnIdle = TimeSpan.FromHours(2), DefaultMessageTimeToLive = TimeSpan.FromDays(3),
                DuplicateDetectionHistoryTimeWindow = TimeSpan.FromMinutes(4), EnableBatchedOperations = false,
                EnablePartitioning = true, RequiresDuplicateDetection = true, RequiresSession = true,
                LockDuration = TimeSpan.FromSeconds(90), MaxDeliveryCount = 7, DeadLetteringOnMessageExpiration = true,
                MaxMessageSizeInKilobytes = 2048, MaxSizeInMegabytes = 5120, Status = EntityStatus.SendDisabled,
                ForwardTo = "archive", ForwardDeadLetteredMessagesTo = "external-dlq"
            });
            options.Queues.Add(new("archive"));
            options.Topics.Add(new("events")
            {
                SupportOrdering = true, MaxMessageSizeInKilobytes = 4096, MaxSizeInMegabytes = 2048,
                AutoDeleteOnIdle = TimeSpan.FromHours(7), DefaultMessageTimeToLive = TimeSpan.FromDays(8),
                DuplicateDetectionHistoryTimeWindow = TimeSpan.FromMinutes(9), EnableBatchedOperations = false,
                EnablePartitioning = true, RequiresDuplicateDetection = true, Status = EntityStatus.Disabled
            });
            options.Subscriptions.Add(new("events", "billing")
            {
                EnableDeadLetteringOnFilterEvaluationExceptions = false, RequiresSession = true,
                AutoDeleteOnIdle = TimeSpan.FromHours(3), DefaultMessageTimeToLive = TimeSpan.FromHours(4),
                EnableBatchedOperations = false, LockDuration = TimeSpan.FromMinutes(2),
                MaxDeliveryCount = 8, DeadLetteringOnMessageExpiration = true, Status = EntityStatus.ReceiveDisabled,
                ForwardTo = "archive", ForwardDeadLetteredMessagesTo = "external-dlq"
            });
        });
        var exporter = provider.GetRequiredService<ServiceBusTopologyExporter>();
        var first = Export(exporter, format);
        await Assert.That(first).IsEqualTo(Export(exporter, format));
        foreach (var expected in new[] { "orders", "archive", "external-dlq", "events", "billing", "PT2H", "P3D", "PT4M", "PT1M30S", "SendDisabled", "ReceiveDisabled", "2048", "4096", "5120" })
            await Assert.That(first).Contains(expected);
        var separator = format == ServiceBusTopologyFormat.Bicep ? ": " : " = ";
        foreach (var property in new[] { "enableBatchedOperations" + separator + "false", "enablePartitioning" + separator + "true",
                     "requiresDuplicateDetection" + separator + "true", "requiresSession" + separator + "true",
                     "maxDeliveryCount" + separator + "7", "maxDeliveryCount" + separator + "8",
                     "deadLetteringOnMessageExpiration" + separator + "true", "supportOrdering" + separator + "true",
                     "deadLetteringOnFilterEvaluationExceptions" + separator + "false" })
            await Assert.That(first).Contains(property);
        await Assert.That(first).Contains(format == ServiceBusTopologyFormat.Bicep ? "parent: topic2" : "parent_id = azapi_resource.topic2.id");
        await Assert.That(first).Contains(format == ServiceBusTopologyFormat.Bicep ? "dependsOn: [\n    queue1" : "depends_on = [\n    azapi_resource.queue1");
        var queue = ResourceBody(first, "queue0", format);
        var topic = ResourceBody(first, "topic2", format);
        var subscription = ResourceBody(first, "subscription3", format);
        foreach (var property in new[] { "maxDeliveryCount" + separator + "7", "maxSizeInMegabytes" + separator + "5120",
                     "requiresSession" + separator + "true", "deadLetteringOnMessageExpiration" + separator + "true" })
            await Assert.That(queue).Contains(property);
        foreach (var duration in new[] { "PT2H", "P3D", "PT4M", "PT1M30S" })
            await Assert.That(queue).Contains(duration);
        foreach (var property in new[] { "maxSizeInMegabytes" + separator + "2048", "maxMessageSizeInKilobytes" + separator + "4096",
                     "enablePartitioning" + separator + "true", "enableBatchedOperations" + separator + "false",
                     "requiresDuplicateDetection" + separator + "true", "supportOrdering" + separator + "true" })
            await Assert.That(topic).Contains(property);
        foreach (var duration in new[] { "PT7H", "P8D", "PT9M" })
            await Assert.That(topic).Contains(duration);
        foreach (var property in new[] { "maxDeliveryCount" + separator + "8", "requiresSession" + separator + "true",
                     "enableBatchedOperations" + separator + "false", "deadLetteringOnFilterEvaluationExceptions" + separator + "false" })
            await Assert.That(subscription).Contains(property);
        foreach (var value in new[] { "PT3H", "PT4H", "PT2M", "ReceiveDisabled", "archive", "external-dlq" })
            await Assert.That(subscription).Contains(value);
        await Assert.That(topic).DoesNotContain("requiresSession");
        await Assert.That(subscription).DoesNotContain("maxSizeInMegabytes");
        await Assert.That(first).DoesNotContain("not a connection string");
        await Assert.That(first).DoesNotContain("\r");
    }

    [Test]
    [Arguments(ServiceBusTopologyFormat.Bicep)]
    [Arguments(ServiceBusTopologyFormat.Terraform)]
    public async Task Forwarding_dependencies_ignore_case_and_preserve_native_addresses(ServiceBusTopologyFormat format)
    {
        using var provider = Provider(options =>
        {
            options.Queues.Add(new("source") { ForwardTo = "archive", ForwardDeadLetteredMessagesTo = "EVENTS" });
            options.Queues.Add(new("Archive"));
            options.Topics.Add(new("Events"));
            options.Subscriptions.Add(new("Events", "sub") { ForwardTo = "ARCHIVE", ForwardDeadLetteredMessagesTo = "archive" });
        });
        var output = Export(provider.GetRequiredService<ServiceBusTopologyExporter>(), format);
        var queue = ResourceBody(output, "queue0", format);
        var subscription = ResourceBody(output, "subscription3", format);
        var dependency = format == ServiceBusTopologyFormat.Bicep ? "dependsOn: [" : "depends_on = [";
        var prefix = format == ServiceBusTopologyFormat.Bicep ? "" : "azapi_resource.";
        await Assert.That(queue).Contains(dependency);
        await Assert.That(queue).Contains("\n    " + prefix + "queue1");
        await Assert.That(queue).Contains("\n    " + prefix + "topic2");
        await Assert.That(subscription).Contains(dependency);
        await Assert.That(subscription).Contains("\n    " + prefix + "queue1");
        var separator = format == ServiceBusTopologyFormat.Bicep ? ": '" : " = \"";
        await Assert.That(queue).Contains("forwardTo" + separator + "archive");
        await Assert.That(queue).Contains("forwardDeadLetteredMessagesTo" + separator + "EVENTS");
        await Assert.That(subscription).Contains("forwardTo" + separator + "ARCHIVE");
        await Assert.That(subscription).Contains("forwardDeadLetteredMessagesTo" + separator + "archive");
    }

    [Test]
    [Arguments(ServiceBusTopologyFormat.Bicep)]
    [Arguments(ServiceBusTopologyFormat.Terraform)]
    public async Task Subscription_names_are_scoped_to_their_topic(ServiceBusTopologyFormat format)
    {
        using var provider = Provider(options =>
        {
            options.Topics.Add(new("first"));
            options.Topics.Add(new("second"));
            options.Subscriptions.Add(new("first", "billing"));
            options.Subscriptions.Add(new("second", "Billing"));
        });
        var text = Export(provider.GetRequiredService<ServiceBusTopologyExporter>(), format);
        await Assert.That(text).Contains("billing");
        await Assert.That(text).Contains("Billing");
    }

    [Test]
    [Arguments(ServiceBusTopologyFormat.Bicep)]
    [Arguments(ServiceBusTopologyFormat.Terraform)]
    public async Task Escapes_literal_names_and_interpolation_without_changing_identity(ServiceBusTopologyFormat format)
    {
        using var provider = Provider(options => options.Queues.Add(new("o'${name}%{literal}雪🚀")));
        var output = Export(provider.GetRequiredService<ServiceBusTopologyExporter>(), format);
        await Assert.That(output).Contains(format == ServiceBusTopologyFormat.Bicep
            ? "'o\\'\\${name}%{literal}雪🚀'" : "\"o'$${name}%%{literal}雪🚀\"");
    }

    [Test]
    [Arguments(ServiceBusTopologyFormat.Bicep)]
    [Arguments(ServiceBusTopologyFormat.Terraform)]
    public async Task Native_sentinel_durations_are_omitted_like_the_SDK_and_other_defaults_are_preserved(ServiceBusTopologyFormat format)
    {
        using var provider = Provider(options =>
        {
            options.Queues.Add(new("queue"));
            options.Topics.Add(new("topic"));
            options.Subscriptions.Add(new("topic", "subscription"));
        });
        var output = Export(provider.GetRequiredService<ServiceBusTopologyExporter>(), format);
        await Assert.That(output).DoesNotContain("autoDeleteOnIdle");
        await Assert.That(output).DoesNotContain("defaultMessageTimeToLive");
        await Assert.That(output).Contains("PT1M");
        await Assert.That(output).DoesNotContain("maxMessageSizeInKilobytes");
    }

    [Test]
    [Arguments("metadata")]
    [Arguments("topic-metadata")]
    [Arguments("subscription-metadata")]
    [Arguments("authorization")]
    [Arguments("topic-authorization")]
    [Arguments("missing-parent")]
    [Arguments("duplicate")]
    [Arguments("duplicate-subscription")]
    [Arguments("case-queues")]
    [Arguments("case-topics")]
    [Arguments("case-queue-topic")]
    [Arguments("case-subscriptions")]
    [Arguments("slash")]
    [Arguments("cycle")]
    [Arguments("case-cycle")]
    [Arguments("parent-cycle")]
    [Arguments("status")]
    public async Task Unsupported_declarations_fail_before_writing_any_output(string scenario)
    {
        using var provider = Provider(options =>
        {
            options.Queues.Add(new("queue"));
            options.Topics.Add(new("topic"));
            options.Subscriptions.Add(new("topic", "subscription"));
            switch (scenario)
            {
                case "metadata": options.Queues[0].UserMetadata = "not representable"; break;
                case "topic-metadata": options.Topics[0].UserMetadata = "not representable"; break;
                case "subscription-metadata": options.Subscriptions[0].UserMetadata = "not representable"; break;
                case "authorization": options.Queues[0].AuthorizationRules.Add(new SharedAccessAuthorizationRule("send", [AccessRights.Send])); break;
                case "topic-authorization": options.Topics[0].AuthorizationRules.Add(new SharedAccessAuthorizationRule("send", [AccessRights.Send])); break;
                case "missing-parent": options.Topics.Clear(); break;
                case "duplicate": options.Topics.Add(new("queue")); break;
                case "case-queues": options.Queues.Add(new("QUEUE")); break;
                case "case-topics": options.Topics.Add(new("TOPIC")); break;
                case "case-queue-topic": options.Topics.Add(new("QUEUE")); break;
                case "case-subscriptions": options.Subscriptions.Add(new("topic", "SUBSCRIPTION")); break;
                case "duplicate-subscription": options.Subscriptions.Add(new("topic", "subscription")); break;
                case "slash": options.Queues.Add(new("path/queue")); break;
                case "case-cycle": options.Queues[0].ForwardDeadLetteredMessagesTo = "QUEUE"; break;
                case "cycle": options.Queues[0].ForwardTo = "queue"; break;
                case "parent-cycle": options.Subscriptions[0].ForwardTo = "topic"; break;
                case "status": options.Queues[0].Status = new EntityStatus("unsupported"); break;
            }
        });
        foreach (var format in new[] { ServiceBusTopologyFormat.Bicep, ServiceBusTopologyFormat.Terraform })
        {
            using var stream = new MemoryStream();
            stream.WriteByte(42);
            await Assert.That(() => provider.GetRequiredService<ServiceBusTopologyExporter>().Export("bus", stream, format))
                .Throws<InvalidOperationException>();
            await Assert.That(stream.ToArray()).IsEquivalentTo(new byte[] { 42 });
        }
    }

    [Test]
    public async Task Named_selection_rejects_missing_or_other_transports_and_unknown_formats()
    {
        using var provider = Provider(_ => { });
        var exporter = provider.GetRequiredService<ServiceBusTopologyExporter>();
        using var stream = new MemoryStream();
        await Assert.That(() => exporter.Export("missing", stream, ServiceBusTopologyFormat.Bicep)).Throws<InvalidOperationException>();
        await Assert.That(() => exporter.Export("other", stream, ServiceBusTopologyFormat.Terraform)).Throws<InvalidOperationException>();
        await Assert.That(() => exporter.Export("bus", stream, (ServiceBusTopologyFormat)42)).Throws<ArgumentOutOfRangeException>();
        await Assert.That(stream.Length).IsEqualTo(0);
        await Assert.That(Export(exporter, ServiceBusTopologyFormat.Bicep)).DoesNotContain("other");
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task Missing_native_consumer_entity_is_not_silently_dropped(bool subscription)
    {
        var definition = new ConsumerDefinition<ServiceBusConsumerOptions>("missing", new() { Subscription = subscription ? "sub" : null },
            [], 1, 1, TimeSpan.FromMilliseconds(100));
        var topology = new DeclarationTopology(new("bus", typeof(ServiceBusTransportOptions), [], [definition], null, (_, _) => { }));
        var exporter = new ServiceBusTopologyExporter(topology, new OptionsMonitor(new()));
        using var stream = new MemoryStream();
        await Assert.That(() => exporter.Export("bus", stream, ServiceBusTopologyFormat.Bicep)).Throws<InvalidOperationException>();
        await Assert.That(stream.Length).IsEqualTo(0);
    }

    [Test]
    public async Task Consumer_subscription_and_queue_entities_export_without_runtime_processors()
    {
        var options = new ServiceBusTransportOptions();
        options.Queues.Add(new("queue"));
        options.Topics.Add(new("topic"));
        options.Subscriptions.Add(new("topic", "sub"));
        var topology = new DeclarationTopology(new("bus", typeof(ServiceBusTransportOptions), [],
            [new ConsumerDefinition<ServiceBusConsumerOptions>("queue", new(), [], 1, 1, TimeSpan.FromMilliseconds(100)),
             new ConsumerDefinition<ServiceBusConsumerOptions>("topic", new() { Subscription = "sub" }, [], 1, 1, TimeSpan.FromMilliseconds(100))],
            null, (_, _) => { }));
        var output = Export(new(topology, new OptionsMonitor(options)), ServiceBusTopologyFormat.Bicep);
        await Assert.That(output).Contains("name: 'sub'");
    }

    private static ServiceProvider Provider(Action<ServiceBusTransportOptions> configure)
    {
        var services = new ServiceCollection();
        services.AddRelayer(builder =>
        {
            builder.ServiceBus("bus", transport => transport.Options.Configure(configure));
            builder.AddTransport<object, object>("other", _ => { });
        });
        return services.BuildServiceProvider();
    }

    private static string Export(ServiceBusTopologyExporter exporter, ServiceBusTopologyFormat format)
    {
        using var output = new MemoryStream();
        exporter.Export("bus", output, format);
        // The caller still owns and can write the stream.
        var result = Encoding.UTF8.GetString(output.ToArray());
        output.WriteByte(0);
        return result;
    }

    private static string ResourceBody(string document, string symbol, ServiceBusTopologyFormat format)
    {
        var marker = format == ServiceBusTopologyFormat.Bicep ? "\nresource " + symbol + " " : "\nresource \"azapi_resource\" \"" + symbol + "\"";
        var start = document.IndexOf(marker, StringComparison.Ordinal);
        if (start < 0) throw new InvalidOperationException($"Missing resource '{symbol}'.");
        var end = document.IndexOf("\nresource ", start + marker.Length, StringComparison.Ordinal);
        return end < 0 ? document[start..] : document[start..end];
    }

    private sealed class OptionsMonitor(ServiceBusTransportOptions options) : IOptionsMonitor<ServiceBusTransportOptions>
    {
        public ServiceBusTransportOptions CurrentValue => options;
        public ServiceBusTransportOptions Get(string? name) => options;
        public IDisposable? OnChange(Action<ServiceBusTransportOptions, string?> listener) => null;
    }

    private sealed class DeclarationTopology(TransportDefinition declaration) : ITopology
    {
        public IReadOnlyList<TransportDefinition> Transports { get; } = [declaration];
        public void ExportJson(Stream destination) => throw new NotSupportedException();
        public void ExportAsyncApi(Stream destination, AsyncApiOptions options) => throw new NotSupportedException();
    }
}
