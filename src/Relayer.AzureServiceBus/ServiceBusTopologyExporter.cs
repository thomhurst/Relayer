using System.Globalization;
using System.Text;
using System.Xml;
using Azure.Messaging.ServiceBus.Administration;
using Microsoft.Extensions.Options;

namespace Relayer.AzureServiceBus;

/// <summary>The infrastructure language used to export Service Bus declarations.</summary>
public enum ServiceBusTopologyFormat
{
    /// <summary>Bicep, targeting an existing namespace in the deployment resource group.</summary>
    Bicep,
    /// <summary>Terraform HCL using the Azure AzAPI v2 provider and an existing namespace ID.</summary>
    Terraform
}

/// <summary>Exports one named transport's native declarations without constructing clients or contacting Azure.</summary>
public sealed class ServiceBusTopologyExporter
{
    private readonly ITopology _topology;
    private readonly IOptionsMonitor<AzureServiceBusTransportOptions> _options;

    /// <summary>Uses the existing topology and its named native options, without resolving runtime transports.</summary>
    public ServiceBusTopologyExporter(ITopology topology, IOptionsMonitor<AzureServiceBusTransportOptions> options)
    {
        ArgumentNullException.ThrowIfNull(topology);
        ArgumentNullException.ThrowIfNull(options);
        _topology = topology;
        _options = options;
    }

    /// <summary>
    /// Writes deterministic UTF-8 infrastructure for one named Service Bus transport. Leaves the stream open.
    /// Bicep takes serviceBusNamespaceName; Terraform takes servicebus_namespace_id. No namespace is created.
    /// Other named transports are outside this explicit selection. Unsupported declarations fail before writing.
    /// </summary>
    public void Export(string transportName, Stream destination, ServiceBusTopologyFormat format)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(transportName);
        ArgumentNullException.ThrowIfNull(destination);
        if (format is not (ServiceBusTopologyFormat.Bicep or ServiceBusTopologyFormat.Terraform))
            throw new ArgumentOutOfRangeException(nameof(format), "Choose Bicep or Terraform for Service Bus topology export.");
        var declaration = _topology.Transports.SingleOrDefault(item => item.Name == transportName)
            ?? throw new InvalidOperationException($"No transport named '{transportName}' is declared.");
        if (declaration.OptionsType != typeof(AzureServiceBusTransportOptions))
            throw new InvalidOperationException($"Transport '{transportName}' is not a Service Bus transport. Select a Service Bus declaration or use its transport's exporter.");
        var options = _options.Get(transportName);
        ServiceBusSendTransport.ValidateDeclarations(transportName, options, declaration.Routes.Select(route => route.Destination));
        ValidateConsumers(declaration, options);
        var resources = Build(options);
        // Render everything before touching the caller's stream: validation never leaves a partial document.
        var text = Render(resources, format);
        using var writer = new StreamWriter(destination, new UTF8Encoding(false), leaveOpen: true);
        writer.Write(text);
    }

    private static void ValidateConsumers(TransportDefinition transport, AzureServiceBusTransportOptions options)
    {
        foreach (var consumer in transport.Consumers)
        {
            if (consumer is not ConsumerDefinition<ServiceBusConsumerOptions> native)
                throw new InvalidOperationException($"Transport '{transport.Name}': unsupported consumer declaration at '{consumer.Source}'.");
            var found = native.Options.Subscription is { } subscription
                ? options.Subscriptions.Any(item => item.TopicName == consumer.Source && item.SubscriptionName == subscription)
                : options.Queues.Any(item => item.Name == consumer.Source);
            if (!found)
                throw new InvalidOperationException($"Transport '{transport.Name}': declare the native queue or subscription for consumer '{consumer.Source}' before exporting.");
        }
    }

    private static List<Resource> Build(AzureServiceBusTransportOptions options)
    {
        var resources = new List<Resource>();
        var topics = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (var queue in options.Queues)
        {
            ValidateEntity(queue.Name, queue.UserMetadata, queue.AuthorizationRules.Count);
            var resource = new Resource("queue" + resources.Count.ToString(CultureInfo.InvariantCulture), "queues", queue.Name, null);
            Common(resource, queue.AutoDeleteOnIdle, queue.DefaultMessageTimeToLive, queue.EnableBatchedOperations, queue.Status);
            resource.Duration("duplicateDetectionHistoryTimeWindow", queue.DuplicateDetectionHistoryTimeWindow);
            resource.Boolean("enablePartitioning", queue.EnablePartitioning);
            resource.Number("maxSizeInMegabytes", queue.MaxSizeInMegabytes);
            if (queue.MaxMessageSizeInKilobytes is { } size) resource.Number("maxMessageSizeInKilobytes", size);
            resource.Boolean("requiresDuplicateDetection", queue.RequiresDuplicateDetection);
            Receiver(resource, queue.LockDuration, queue.MaxDeliveryCount, queue.RequiresSession,
                queue.DeadLetteringOnMessageExpiration, queue.ForwardTo, queue.ForwardDeadLetteredMessagesTo);
            resources.Add(resource);
        }
        foreach (var topic in options.Topics)
        {
            ValidateEntity(topic.Name, topic.UserMetadata, topic.AuthorizationRules.Count);
            var resource = new Resource("topic" + resources.Count.ToString(CultureInfo.InvariantCulture), "topics", topic.Name, null);
            Common(resource, topic.AutoDeleteOnIdle, topic.DefaultMessageTimeToLive, topic.EnableBatchedOperations, topic.Status);
            resource.Duration("duplicateDetectionHistoryTimeWindow", topic.DuplicateDetectionHistoryTimeWindow);
            resource.Boolean("enablePartitioning", topic.EnablePartitioning);
            resource.Number("maxSizeInMegabytes", topic.MaxSizeInMegabytes);
            if (topic.MaxMessageSizeInKilobytes is { } size) resource.Number("maxMessageSizeInKilobytes", size);
            resource.Boolean("requiresDuplicateDetection", topic.RequiresDuplicateDetection);
            resource.Boolean("supportOrdering", topic.SupportOrdering);
            topics.Add(topic.Name, resource.Symbol);
            resources.Add(resource);
        }
        foreach (var subscription in options.Subscriptions)
        {
            ValidateEntity(subscription.SubscriptionName, subscription.UserMetadata, 0);
            var resource = new Resource("subscription" + resources.Count.ToString(CultureInfo.InvariantCulture),
                "topics/subscriptions", subscription.SubscriptionName, topics[subscription.TopicName]);
            Common(resource, subscription.AutoDeleteOnIdle, subscription.DefaultMessageTimeToLive, subscription.EnableBatchedOperations, subscription.Status);
            Receiver(resource, subscription.LockDuration, subscription.MaxDeliveryCount, subscription.RequiresSession,
                subscription.DeadLetteringOnMessageExpiration, subscription.ForwardTo, subscription.ForwardDeadLetteredMessagesTo);
            resource.Boolean("deadLetteringOnFilterEvaluationExceptions", subscription.EnableDeadLetteringOnFilterEvaluationExceptions);
            resources.Add(resource);
        }
        // Native forwarding requires its destination to exist first. Keep external names literal;
        // dependencies on declared destinations are emitted without changing the native address.
        var endpoints = new Dictionary<string, Resource>(StringComparer.OrdinalIgnoreCase);
        foreach (var resource in resources)
        {
            if (resource.Parent is not null) continue;
            if (!endpoints.TryAdd(resource.Name, resource))
                throw new InvalidOperationException($"Entities '{endpoints[resource.Name].Name}' and '{resource.Name}' have the same case-insensitive Service Bus name. Use distinct queue and topic names before exporting.");
        }
        foreach (var resource in resources)
        foreach (var forward in resource.Forwards)
            if (endpoints.TryGetValue(forward, out var target)) resource.Dependencies.Add(target.Symbol);
        ValidateDependencies(resources);
        return resources;
    }

    private static void ValidateEntity(string name, string? metadata, int authorizationRuleCount)
    {
        if (name.Contains('/'))
            throw new InvalidOperationException($"Entity '{name}': slash-containing native names cannot be exported as ARM child resource names. Manage this entity outside this export.");
        if (!string.IsNullOrEmpty(metadata))
            throw new InvalidOperationException($"Entity '{name}': UserMetadata has no ARM 2024-01-01 property. Remove it or manage this entity outside this export.");
        if (authorizationRuleCount != 0)
            throw new InvalidOperationException($"Entity '{name}': AuthorizationRules export is unsupported. Manage authorization separately; keys are never exported.");
    }

    private static void Common(Resource resource, TimeSpan idle, TimeSpan ttl, bool batched, EntityStatus status)
    {
        if (status.ToString() is not ("Active" or "Disabled" or "SendDisabled" or "ReceiveDisabled" or
            "Creating" or "Deleting" or "Renaming" or "Restoring" or "Unknown"))
            throw new InvalidOperationException($"Entity '{resource.Name}': unsupported Status '{status}'. Use a supported native entity status before exporting.");
        // Match the pinned native SDK's serialization: MaxValue means use the service default.
        if (idle != TimeSpan.MaxValue) resource.Duration("autoDeleteOnIdle", idle);
        if (ttl != TimeSpan.MaxValue) resource.Duration("defaultMessageTimeToLive", ttl);
        resource.Boolean("enableBatchedOperations", batched);
        resource.String("status", status.ToString());
    }

    private static void Receiver(Resource resource, TimeSpan duration, int deliveries, bool sessions, bool deadLetter,
        string? forward, string? deadLetterForward)
    {
        resource.Duration("lockDuration", duration);
        resource.Number("maxDeliveryCount", deliveries);
        resource.Boolean("requiresSession", sessions);
        resource.Boolean("deadLetteringOnMessageExpiration", deadLetter);
        if (!string.IsNullOrEmpty(forward)) { resource.String("forwardTo", forward); resource.Forwards.Add(forward); }
        if (!string.IsNullOrEmpty(deadLetterForward)) { resource.String("forwardDeadLetteredMessagesTo", deadLetterForward); resource.Forwards.Add(deadLetterForward); }
    }

    private static void ValidateDependencies(List<Resource> resources)
    {
        var bySymbol = resources.ToDictionary(item => item.Symbol, StringComparer.Ordinal);
        var visiting = new HashSet<string>(StringComparer.Ordinal);
        var visited = new HashSet<string>(StringComparer.Ordinal);
        void Visit(Resource item)
        {
            if (visited.Contains(item.Symbol)) return;
            if (!visiting.Add(item.Symbol))
                throw new InvalidOperationException($"Entity '{item.Name}': cyclic forwarding or parent dependencies cannot be exported. Remove the cycle before exporting.");
            if (item.Parent is { } parent) Visit(bySymbol[parent]);
            foreach (var dependency in item.Dependencies) Visit(bySymbol[dependency]);
            visiting.Remove(item.Symbol);
            visited.Add(item.Symbol);
        }
        foreach (var resource in resources) Visit(resource);
    }

    private static string Render(List<Resource> resources, ServiceBusTopologyFormat format)
    {
        var bicep = format == ServiceBusTopologyFormat.Bicep;
        var text = new StringBuilder();
        text.Append(bicep
            ? "param serviceBusNamespaceName string\n\nresource serviceBusNamespace 'Microsoft.ServiceBus/namespaces@2024-01-01' existing = {\n  name: serviceBusNamespaceName\n}\n"
            : "terraform {\n  required_providers {\n    azapi = {\n      source  = \"Azure/azapi\"\n      version = \"~> 2.0\"\n    }\n  }\n}\n\nprovider \"azapi\" {}\n\nvariable \"servicebus_namespace_id\" {\n  type = string\n}\n");
        foreach (var resource in resources)
        {
            var type = "Microsoft.ServiceBus/namespaces/" + resource.Kind + "@2024-01-01";
            if (bicep)
            {
                text.Append("\nresource ").Append(resource.Symbol).Append(' ').Append(Quote(type, true)).Append(" = {\n  parent: ")
                    .Append(resource.Parent ?? "serviceBusNamespace").Append("\n  name: ").Append(Quote(resource.Name, true)).Append("\n  properties: {\n");
            }
            else
            {
                text.Append("\nresource \"azapi_resource\" \"").Append(resource.Symbol).Append("\" {\n  type      = ").Append(Quote(type, false))
                    .Append("\n  name      = ").Append(Quote(resource.Name, false)).Append("\n  parent_id = ")
                    .Append(resource.Parent is { } parent ? "azapi_resource." + parent + ".id" : "var.servicebus_namespace_id")
                    .Append("\n  body = {\n    properties = {\n");
            }
            foreach (var property in resource.Properties)
                text.Append(bicep ? "    " : "      ").Append(property.Name).Append(bicep ? ": " : " = ")
                    .Append(property.IsString ? Quote(property.Value, bicep) : property.Value).Append('\n');
            text.Append(bicep ? "  }\n" : "    }\n  }\n");
            if (resource.Dependencies.Count != 0)
            {
                text.Append(bicep ? "  dependsOn: [\n" : "  depends_on = [\n");
                foreach (var dependency in resource.Dependencies)
                    text.Append("    ").Append(bicep ? dependency : "azapi_resource." + dependency).Append(bicep ? "\n" : ",\n");
                text.Append("  ]\n");
            }
            text.Append("}\n");
        }
        return text.ToString();
    }

    private static string Quote(string value, bool bicep)
    {
        if (!bicep) return QuoteTerraform(value);
        var text = new StringBuilder("'");
        foreach (var character in value)
            text.Append(character switch
            {
                '\\' => "\\\\", '\'' => "\\'", '$' => "\\$", '\r' => "\\r", '\n' => "\\n", '\t' => "\\t",
                _ when char.IsControl(character) => "\\u{" + ((int)character).ToString("x", CultureInfo.InvariantCulture) + "}",
                _ => character.ToString()
            });
        return text.Append('\'').ToString();
    }

    private static string QuoteTerraform(string value)
    {
        var text = new StringBuilder("\"");
        for (var index = 0; index < value.Length; index++)
        {
            var character = value[index];
            if (character is '$' or '%' && index + 1 < value.Length && value[index + 1] == '{')
                text.Append(character);
            text.Append(character switch
            {
                '\\' => "\\\\", '"' => "\\\"", '\r' => "\\r", '\n' => "\\n", '\t' => "\\t",
                _ when char.IsControl(character) => "\\u" + ((int)character).ToString("x4", CultureInfo.InvariantCulture),
                _ => character.ToString()
            });
        }
        return text.Append('"').ToString();
    }

    private sealed class Resource(string symbol, string kind, string name, string? parent)
    {
        internal string Symbol { get; } = symbol;
        internal string Kind { get; } = kind;
        internal string Name { get; } = name;
        internal string? Parent { get; } = parent;
        internal List<Property> Properties { get; } = [];
        internal List<string> Forwards { get; } = [];
        internal List<string> Dependencies { get; } = [];
        internal void String(string key, string value) => Properties.Add(new(key, value, true));
        internal void Boolean(string key, bool value) => Properties.Add(new(key, value ? "true" : "false", false));
        internal void Number(string key, long value) => Properties.Add(new(key, value.ToString(CultureInfo.InvariantCulture), false));
        internal void Duration(string key, TimeSpan value) => String(key, XmlConvert.ToString(value));
    }

    private sealed record Property(string Name, string Value, bool IsString);
}
