using System.Buffers;
using System.Text.Json.Nodes;
using NATS.Client.JetStream;
using NATS.Client.JetStream.Models;

namespace Relayer.Nats;

internal sealed class NatsTopology
{
    private readonly NatsStream[] _streams;
    private readonly NatsConsumer[] _consumers;

    internal NatsTopology(NatsTransportOptions options)
    {
        _streams = options.Streams.ToArray();
        _consumers = options.Consumers.ToArray();
        var streams = new HashSet<string>(StringComparer.Ordinal);
        foreach (var stream in _streams)
        {
            ArgumentNullException.ThrowIfNull(stream);
            ValidateName(stream.Name);
            if (!streams.Add(stream.Name)) throw new ArgumentException($"Duplicate NATS stream '{stream.Name}'.");
            if (stream.Configuration is { } config && config.Name != stream.Name)
                throw new ArgumentException($"NATS stream '{stream.Name}': native StreamConfig.Name must match the declaration.");
        }
        var consumers = new HashSet<(string Stream, string Name)>();
        foreach (var consumer in _consumers)
        {
            ArgumentNullException.ThrowIfNull(consumer);
            ValidateName(consumer.Stream);
            ValidateName(consumer.Name);
            if (!consumers.Add((consumer.Stream, consumer.Name)))
                throw new ArgumentException($"Duplicate NATS consumer '{consumer.Stream}/{consumer.Name}'.");
            if (consumer.Configuration is { } config && (config.Name != consumer.Name ||
                config.DurableName is { } durable && durable != consumer.Name))
                throw new ArgumentException($"NATS consumer '{consumer.Stream}/{consumer.Name}': native ConsumerConfig.Name and any DurableName must match the declaration.");
        }
    }

    internal async ValueTask<TopologyReport> ApplyAsync(NatsJSContext context, TopologyMode mode, CancellationToken token)
    {
        var differences = new List<string>();
        foreach (var stream in _streams)
        {
            token.ThrowIfCancellationRequested();
            var entity = $"Stream '{stream.Name}'";
            try
            {
                if (mode == TopologyMode.Provision && stream.Configuration is { } config)
                    await context.CreateStreamAsync(config, token).ConfigureAwait(false);
                else
                {
                    var actual = await context.GetStreamAsync(stream.Name, cancellationToken: token).ConfigureAwait(false);
                    if (stream.Configuration is { } expected) Compare(entity, Json(expected), Json(actual.Info.Config), false, differences);
                }
            }
            catch (NatsJSApiException error) when (error.Error.Code is 400 or 404)
            { differences.Add($"{entity}: {error.Error.Description}. Supply native Configuration to provision a missing entity; reconcile conflicting settings externally. Relayer never updates or deletes entities."); }
        }
        foreach (var consumer in _consumers)
        {
            token.ThrowIfCancellationRequested();
            var entity = $"Consumer '{consumer.Stream}/{consumer.Name}'";
            try
            {
                if (mode == TopologyMode.Provision && consumer.Configuration is { } config)
                    await context.CreateConsumerAsync(consumer.Stream, config, token).ConfigureAwait(false);
                else
                {
                    var actual = await context.GetConsumerAsync(consumer.Stream, consumer.Name, token).ConfigureAwait(false);
                    if (consumer.Configuration is { } expected) Compare(entity, Json(expected), Json(actual.Info.Config), true, differences);
                }
            }
            catch (NatsJSApiException error) when (error.Error.Code is 400 or 404)
            { differences.Add($"{entity}: {error.Error.Description}. Supply native Configuration to provision a missing entity; reconcile conflicting settings externally. Relayer never updates or deletes entities."); }
        }
        return new(differences);
    }

    // Use the client's generated metadata, including its nanosecond durations and enum wire names.
    // This is startup-only: no reflection, probing writes or second native configuration model.
    private static JsonObject Json<T>(T configuration)
    {
        var buffer = new ArrayBufferWriter<byte>();
        NatsJSJsonSerializer<T>.Default.Serialize(buffer, configuration);
        return JsonNode.Parse(buffer.WrittenSpan)!.AsObject();
    }

    private static void Compare(string entity, JsonObject expected, JsonObject actual, bool consumer, List<string> differences)
    {
        Normalize(expected, consumer);
        Normalize(actual, consumer);
        var names = new HashSet<string>(expected.Select(pair => pair.Key), StringComparer.Ordinal);
        names.UnionWith(actual.Select(pair => pair.Key));
        foreach (var name in names)
        {
            expected.TryGetPropertyValue(name, out var wanted);
            actual.TryGetPropertyValue(name, out var found);
            if (JsonNode.DeepEquals(wanted, found)) continue;
            differences.Add($"{entity}: setting '{name}' expected {wanted?.ToJsonString() ?? "null"}, actual {found?.ToJsonString() ?? "null"}. Reconcile settings externally; Validate is read-only.");
        }
    }

    private static void Normalize(JsonObject config, bool consumer)
    {
        // Server defaults for the pinned JetStream protocol. An explicit non-default remains visible.
        if (consumer)
        {
            if (config["backoff"] is JsonArray { Count: > 0 } backoff) config["ack_wait"] = backoff[0]!.DeepClone();
            else Default(config, "ack_wait", 30_000_000_000L);
            Default(config, "max_deliver", -1);
            Default(config, "max_ack_pending", 1000);
            Default(config, "max_waiting", 512);
        }
        else
        {
            Default(config, "max_consumers", -1);
            Default(config, "max_msgs", -1);
            Default(config, "max_msgs_per_subject", -1);
            Default(config, "max_bytes", -1);
            Default(config, "max_msg_size", -1);
            Default(config, "num_replicas", 1);
            var maximumAge = config["max_age"]?.GetValue<long>() ?? 0;
            Default(config, "duplicate_window", maximumAge > 0 ? Math.Min(maximumAge, 120_000_000_000L) : 120_000_000_000L);
        }
        if (config["metadata"] is JsonObject metadata)
            foreach (var name in metadata.Select(pair => pair.Key).ToArray())
                if (name.StartsWith("_nats.", StringComparison.Ordinal)) metadata.Remove(name);
        // Empty optional fields and omitted scalar defaults are equivalent native JSON.
        foreach (var name in config.Select(pair => pair.Key).ToArray())
        {
            var value = config[name];
            if (value is null || value.ToJsonString() is "false" or "0" or "\"\"" or "[]" or "{}") config.Remove(name);
        }
        foreach (var name in new[] { "subjects", "filter_subjects" })
            if (config[name] is JsonArray subjects)
                config[name] = new JsonArray(subjects.Select(item => item!.GetValue<string>()).Order(StringComparer.Ordinal).Select(item => (JsonNode?)JsonValue.Create(item)).ToArray());
    }

    private static void Default(JsonObject config, string name, long value)
    {
        if (config[name] is null || config[name]!.ToJsonString() == "0") config[name] = value;
    }

    private static void ValidateName(string name)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        if (name.AsSpan().ContainsAny(".*>/\\"))
            throw new ArgumentException($"Invalid NATS entity name '{name}': use a native stream or consumer name without '.', '*', '>', '/' or '\\'.");
    }
}
