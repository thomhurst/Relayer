using System.Globalization;
using System.Text.Json;
using System.Text.Json.Nodes;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

namespace Relayer.AmazonSqs;

/// <summary>The explicit AWS deployment identity used to resolve declared entity ARNs offline.</summary>
public sealed class AmazonTerraformOptions
{
    /// <summary>The target AWS account ID (twelve digits).</summary>
    public required string AccountId { get; init; }
    /// <summary>The target AWS region. Configure the Terraform AWS provider for this region and account.</summary>
    public required string Region { get; init; }
    /// <summary>The ARN partition, such as aws, aws-cn or aws-us-gov.</summary>
    public string Partition { get; init; } = "aws";
}

/// <summary>Offline Terraform JSON export of native SQS and SNS declarations.</summary>
public static class AmazonTerraformExporter
{
    /// <summary>
    /// Exports all transports in this topology as Terraform JSON (.tf.json), resolving named native options
    /// from services without resolving transport clients. Unsupported transports, attributes and duplicate
    /// entities fail before writing. Leaves destination open. Provider configuration is application-owned.
    /// </summary>
    public static void ExportAmazonTerraform(this ITopology topology, IServiceProvider services, Stream destination,
        AmazonTerraformOptions options)
    {
        ArgumentNullException.ThrowIfNull(topology);
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(destination);
        ArgumentNullException.ThrowIfNull(options);
        if (options.AccountId is null || options.AccountId.Length != 12 || options.AccountId.Any(c => c is < '0' or > '9'))
            throw Error("AccountId must contain twelve digits.");
        ValidateIdentity(options.Region, "Region");
        ValidateIdentity(options.Partition, "Partition");

        var resources = new SortedDictionary<string, SortedDictionary<string, JsonObject>>(StringComparer.Ordinal);
        var queues = new SortedDictionary<string, Amazon.SQS.Model.CreateQueueRequest>(StringComparer.Ordinal);
        var topics = new SortedDictionary<string, Amazon.SimpleNotificationService.Model.CreateTopicRequest>(StringComparer.Ordinal);
        var subscriptions = new SortedDictionary<string, Amazon.SimpleNotificationService.Model.SubscribeRequest>(StringComparer.Ordinal);
        foreach (var transport in topology.Transports)
        {
            if (transport.OptionsType == typeof(AmazonSqsTransportOptions))
                AddQueues(services.GetRequiredService<IOptionsMonitor<AmazonSqsTransportOptions>>().Get(transport.Name));
            else if (transport.OptionsType == typeof(AmazonSnsTransportOptions))
            {
                var native = services.GetRequiredService<IOptionsMonitor<AmazonSnsTransportOptions>>().Get(transport.Name);
                CheckRegion(native.Sns.RegionEndpoint?.SystemName);
                AddQueues(native.QueueTopology);
                foreach (var topic in native.Topics)
                    Add(topics, topic.Name, topic, "topic");
                foreach (var subscription in native.Subscriptions)
                {
                    if (subscription.Protocol != "sqs") throw Error("Only SNS subscriptions with Protocol=sqs are supported.");
                    ValidateArn(subscription.TopicArn, "sns");
                    ValidateArn(subscription.Endpoint, "sqs");
                    Add(subscriptions, subscription.TopicArn + "|" + subscription.Endpoint, subscription, "subscription");
                }
            }
            else throw Error($"Transport '{transport.Name}' is not SQS/SNS. Export a topology containing only AWS transports.");
        }
        var queueRefs = References(queues.Keys, "sqs", "aws_sqs_queue");
        var topicRefs = References(topics.Keys, "sns", "aws_sns_topic");
        foreach (var (name, queue) in queues)
        {
            var body = new JsonObject { ["name"] = Literal(name) };
            Attributes(body, queue.Attributes, "queue", name);
            Tags(body, queue.Tags);
            if (queue.Attributes?.TryGetValue("RedrivePolicy", out var redrive) == true)
            {
                using var policy = JsonDocument.Parse(redrive);
                if (!policy.RootElement.TryGetProperty("deadLetterTargetArn", out var target) || target.ValueKind != JsonValueKind.String)
                    throw Error($"Queue '{name}' RedrivePolicy needs deadLetterTargetArn.");
                var arn = target.GetString()!;
                if (queueRefs.TryGetValue(arn, out var reference))
                {
                    if (arn == Arn("sqs", name)) throw Error($"Queue '{name}' cannot dead-letter to itself.");
                    body["depends_on"] = new JsonArray(reference);
                }
            }
            Resource("aws_sqs_queue", name, body);
            if (queue.Attributes?.TryGetValue("Policy", out var accessPolicy) == true)
                Resource("aws_sqs_queue_policy", name, new JsonObject
                {
                    ["queue_url"] = Expression(queueRefs[Arn("sqs", name)] + ".id"),
                    ["policy"] = Literal(accessPolicy),
                    ["depends_on"] = Dependencies(topicRefs.Values)
                });
        }
        foreach (var (name, topic) in topics)
        {
            if (!string.IsNullOrEmpty(topic.DataProtectionPolicy))
                throw Error($"Topic '{name}' DataProtectionPolicy is unsupported; manage it separately and remove it from the declaration before exporting.");
            var body = new JsonObject { ["name"] = Literal(name) };
            Attributes(body, topic.Attributes, "topic", name);
            if (topic.Tags is { Count: > 0 })
            {
                var tags = new Dictionary<string, string>(StringComparer.Ordinal);
                foreach (var tag in topic.Tags)
                    if (!tags.TryAdd(tag.Key, tag.Value)) throw Error($"Topic '{name}' has duplicate tag '{tag.Key}'.");
                Tags(body, tags);
            }
            Resource("aws_sns_topic", name, body);
        }
        foreach (var (key, subscription) in subscriptions)
        {
            var body = new JsonObject
            {
                ["topic_arn"] = Reference(subscription.TopicArn, topicRefs),
                ["endpoint"] = Reference(subscription.Endpoint, queueRefs),
                ["protocol"] = "sqs"
            };
            Attributes(body, subscription.Attributes, "subscription", key);
            if (resources.TryGetValue("aws_sqs_queue_policy", out var policies))
                body["depends_on"] = Dependencies(policies.Keys.Select(id => "aws_sqs_queue_policy." + id));
            // ReturnSubscriptionArn changes only the SDK response, not infrastructure.
            Resource("aws_sns_topic_subscription", key, body);
        }
        var rootResources = new JsonObject();
        foreach (var (type, instances) in resources)
        {
            var instancesJson = new JsonObject();
            foreach (var (id, body) in instances) instancesJson.Add(id, body);
            rootResources.Add(type, instancesJson);
        }
        var document = new JsonObject { ["resource"] = rootResources };
        // Buffer the entire validated document: failures never leave a partial infrastructure file.
        using var buffer = new MemoryStream();
        using (var writer = new Utf8JsonWriter(buffer, new JsonWriterOptions { Indented = true })) document.WriteTo(writer);
        buffer.Position = 0;
        buffer.CopyTo(destination);

        void AddQueues(AmazonSqsTransportOptions native)
        {
            CheckRegion(native.Sqs.RegionEndpoint?.SystemName);
            foreach (var queue in native.Queues) Add(queues, queue.QueueName, queue, "queue");
        }
        void CheckRegion(string? region)
        {
            if (region is not null && region != options.Region)
                throw Error($"Native region '{region}' differs from export region '{options.Region}'. Export regions separately.");
        }
        string Arn(string service, string name) => $"arn:{options.Partition}:{service}:{options.Region}:{options.AccountId}:{name}";
        Dictionary<string, string> References(IEnumerable<string> names, string service, string resource) =>
            names.ToDictionary(name => Arn(service, name), name => resource + "." + Id(name), StringComparer.Ordinal);
        void Resource(string type, string name, JsonObject body)
        {
            if (!resources.TryGetValue(type, out var instances)) resources.Add(type, instances = new(StringComparer.Ordinal));
            instances.Add(Id(name), body);
        }
    }

    private static void Add<T>(IDictionary<string, T> declarations, string name, T declaration, string kind)
    {
        if (string.IsNullOrWhiteSpace(name)) throw Error($"A {kind} declaration needs a name/ARN.");
        if (!declarations.TryAdd(name, declaration)) throw Error($"Duplicate {kind} declaration '{name}'. Declare each entity once.");
    }

    // Hex encoding is stable, valid in Terraform identifiers and collision-free, including punctuation and Unicode.
    private static string Id(string name) => "r_" + Convert.ToHexString(System.Text.Encoding.UTF8.GetBytes(name)).ToLowerInvariant();
    private static string Literal(string value) => value.Replace("${", "$${", StringComparison.Ordinal).Replace("%{", "%%{", StringComparison.Ordinal);
    private static string Expression(string value) => "${" + value + "}";
    private static string Reference(string arn, Dictionary<string, string> references) =>
        references.TryGetValue(arn, out var reference) ? Expression(reference + ".arn") : Literal(arn);
    private static JsonArray Dependencies(IEnumerable<string> references) => new(references.Order(StringComparer.Ordinal).Select(value => (JsonNode?)JsonValue.Create(value)).ToArray());
    private static InvalidOperationException Error(string message) => new("AWS Terraform export: " + message);

    private static void ValidateIdentity(string value, string field)
    {
        if (string.IsNullOrWhiteSpace(value) || value.Any(c => !char.IsAsciiLetterOrDigit(c) && c != '-'))
            throw Error($"{field} must contain only ASCII letters, digits and hyphens.");
    }

    private static void ValidateArn(string arn, string service)
    {
        var parts = arn?.Split(':');
        if (parts is not { Length: 6 } || parts[0] != "arn" || parts[2] != service || parts.Any(string.IsNullOrWhiteSpace))
            throw Error($"Expected an explicit {service} ARN, received '{arn}'.");
    }

    private static void Tags(JsonObject body, IDictionary<string, string>? tags)
    {
        if (tags is not { Count: > 0 }) return;
        var result = new JsonObject();
        foreach (var (key, value) in tags.OrderBy(pair => pair.Key, StringComparer.Ordinal)) result.Add(Literal(key), Literal(value));
        body.Add("tags", result);
    }

    private static void Attributes(JsonObject body, IDictionary<string, string>? attributes, string kind, string name)
    {
        if (attributes is null) return;
        foreach (var (key, value) in attributes.OrderBy(pair => pair.Key, StringComparer.Ordinal))
        {
            if (kind == "queue" && key == "Policy") { ValidateJson(value, key); continue; }
            var (field, format) = Mapping(kind, key);
            if (field is null) throw Error($"{kind} '{name}' attribute '{key}' is unsupported. Remove it from this export and manage it separately.");
            if (format == 'b')
            {
                if (value is not ("true" or "false")) throw Error($"{kind} '{name}' attribute '{key}' must be true or false.");
                body.Add(field, value == "true");
            }
            else if (format == 'n')
            {
                if (!long.TryParse(value, NumberStyles.None, CultureInfo.InvariantCulture, out var number))
                    throw Error($"{kind} '{name}' attribute '{key}' must be a nonnegative integer.");
                body.Add(field, number);
            }
            else
            {
                if (format == 'j') ValidateJson(value, key);
                body.Add(field, Literal(value));
            }
        }
    }

    private static void ValidateJson(string value, string key)
    {
        try { using var document = JsonDocument.Parse(value); if (document.RootElement.ValueKind != JsonValueKind.Object) throw Error($"{key} must be a JSON object."); }
        catch (JsonException error) { throw Error($"{key} must be valid JSON: {error.Message}"); }
    }

    private static (string? Field, char Format) Mapping(string kind, string key) => (kind, key) switch
    {
        ("queue", "DelaySeconds") => ("delay_seconds", 'n'),
        ("queue", "MaximumMessageSize") => ("max_message_size", 'n'),
        ("queue", "MessageRetentionPeriod") => ("message_retention_seconds", 'n'),
        ("queue", "ReceiveMessageWaitTimeSeconds") => ("receive_wait_time_seconds", 'n'),
        ("queue", "VisibilityTimeout") => ("visibility_timeout_seconds", 'n'),
        ("queue", "FifoQueue") => ("fifo_queue", 'b'),
        ("queue", "ContentBasedDeduplication") => ("content_based_deduplication", 'b'),
        ("queue", "DeduplicationScope") => ("deduplication_scope", 's'),
        ("queue", "FifoThroughputLimit") => ("fifo_throughput_limit", 's'),
        ("queue", "KmsMasterKeyId") => ("kms_master_key_id", 's'),
        ("queue", "KmsDataKeyReusePeriodSeconds") => ("kms_data_key_reuse_period_seconds", 'n'),
        ("queue", "SqsManagedSseEnabled") => ("sqs_managed_sse_enabled", 'b'),
        ("queue", "RedrivePolicy") => ("redrive_policy", 'j'),
        ("topic", "DisplayName") => ("display_name", 's'),
        ("topic", "Policy") => ("policy", 'j'),
        ("topic", "DeliveryPolicy") => ("delivery_policy", 'j'),
        ("topic", "FifoTopic") => ("fifo_topic", 'b'),
        ("topic", "ContentBasedDeduplication") => ("content_based_deduplication", 'b'),
        ("topic", "KmsMasterKeyId") => ("kms_master_key_id", 's'),
        ("topic", "SignatureVersion") => ("signature_version", 'n'),
        ("topic", "TracingConfig") => ("tracing_config", 's'),
        ("topic", "FifoThroughputScope") => ("fifo_throughput_scope", 's'),
        ("subscription", "RawMessageDelivery") => ("raw_message_delivery", 'b'),
        ("subscription", "FilterPolicy") => ("filter_policy", 'j'),
        ("subscription", "FilterPolicyScope") => ("filter_policy_scope", 's'),
        ("subscription", "RedrivePolicy") => ("redrive_policy", 'j'),
        ("subscription", "DeliveryPolicy") => ("delivery_policy", 'j'),
        ("subscription", "SubscriptionRoleArn") => ("subscription_role_arn", 's'),
        _ => (null, default)
    };
}
