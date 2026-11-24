using System.Buffers;
using System.Text.Json;
using System.Text.Json.Nodes;
using NATS.Client.Core;
using NATS.Client.JetStream;
using NATS.Client.JetStream.Models;

namespace Relayer.Nats;

internal sealed class NatsTopology
{
    private const int StreamReplicasNotSupported = 10074;

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

            if (!streams.Add(stream.Name))
            {
                throw new ArgumentException($"Duplicate NATS stream '{stream.Name}'.");
            }

            if (stream.Configuration is { } config && config.Name != stream.Name)
            {
                throw new ArgumentException($"NATS stream '{stream.Name}': native StreamConfig.Name must match the declaration.");
            }
        }

        var consumers = new HashSet<(string Stream, string Name)>();

        foreach (var consumer in _consumers)
        {
            ArgumentNullException.ThrowIfNull(consumer);
            ValidateName(consumer.Stream);
            ValidateName(consumer.Name);

            if (!consumers.Add((consumer.Stream, consumer.Name)))
            {
                throw new ArgumentException($"Duplicate NATS consumer '{consumer.Stream}/{consumer.Name}'.");
            }

            if (consumer.Configuration is { } config && (config.Name != consumer.Name ||
                config.DurableName is { } durable && durable != consumer.Name))
            {
                throw new ArgumentException($"NATS consumer '{consumer.Stream}/{consumer.Name}': native ConsumerConfig.Name and any DurableName must match the declaration.");
            }
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
                {
                    await context.CreateStreamAsync(config, token).ConfigureAwait(false);
                }
                else
                {
                    var actual = await context.GetStreamAsync(stream.Name, cancellationToken: token).ConfigureAwait(false);

                    if (stream.Configuration is { } expected)
                    {
                        Compare(entity, Json(expected), Json(actual.Info.Config), false, differences);
                    }
                }
            }
            catch (NatsJSApiException error) when (error.Error is { Code: 400 or 404 } or { ErrCode: StreamReplicasNotSupported })
            {
                differences.Add(DescribeApiError(entity, error.Error, mode, stream.Configuration is not null));
            }
        }

        var consumerLimitsByStream = new Dictionary<string, (long MaxAckPending, long InactiveThreshold)>(StringComparer.Ordinal);

        foreach (var consumer in _consumers)
        {
            token.ThrowIfCancellationRequested();
            var entity = $"Consumer '{consumer.Stream}/{consumer.Name}'";

            try
            {
                if (mode == TopologyMode.Provision && consumer.Configuration is { } config)
                {
                    await context.CreateConsumerAsync(consumer.Stream, config, token).ConfigureAwait(false);
                }
                else
                {
                    var actual = await context.GetConsumerAsync(consumer.Stream, consumer.Name, token).ConfigureAwait(false);

                    if (consumer.Configuration is { } expected)
                    {
                        var expectedConfig = Json(expected);

                        if (expected.MaxAckPending == 0 || expected.InactiveThreshold == TimeSpan.Zero)
                        {
                            if (!consumerLimitsByStream.TryGetValue(consumer.Stream, out var limits))
                            {
                                limits = await ReadConsumerLimitsAsync(context, consumer.Stream, token).ConfigureAwait(false);
                                consumerLimitsByStream.Add(consumer.Stream, limits);
                            }

                            Default(expectedConfig, "max_ack_pending", limits.MaxAckPending);
                            Default(expectedConfig, "inactive_threshold", limits.InactiveThreshold);
                        }

                        ApplyConsumerDefaults(expected, actual.Info.Config, expectedConfig);
                        Compare(entity, expectedConfig, Json(actual.Info.Config), true, differences);
                    }
                }
            }
            catch (NatsJSApiException error) when (error.Error.Code is 400 or 404)
            {
                differences.Add(DescribeApiError(entity, error.Error, mode, consumer.Configuration is not null));
            }
        }

        return new(differences);
    }

    private static async ValueTask<(long MaxAckPending, long InactiveThreshold)> ReadConsumerLimitsAsync(
        NatsJSContext context, string stream, CancellationToken token)
    {
        // GetStreamAsync discards consumer_limits in NATS.Client.JetStream 3.4.1 (nats.net #1307).
        var serializer = NatsRawSerializer<ReadOnlyMemory<byte>>.Default;
        var reply = await context.Connection.RequestAsync<ReadOnlyMemory<byte>, ReadOnlyMemory<byte>>(
            $"{context.Opts.Prefix}.STREAM.INFO.{stream}", ReadOnlyMemory<byte>.Empty,
            requestSerializer: serializer, replySerializer: serializer,
            replyOpts: new NatsSubOpts { Timeout = context.Opts.RequestTimeout }, cancellationToken: token).ConfigureAwait(false);

        if (reply.Error is { } error)
        {
            throw error;
        }

        using var document = JsonDocument.Parse(reply.Data);
        var root = document.RootElement;

        if (root.TryGetProperty("error", out _))
        {
            var response = NatsJSJsonSerializer<ErrorResponse>.Default.Deserialize(new ReadOnlySequence<byte>(reply.Data));
            throw new NatsJSApiException(response!.Error);
        }

        if (!root.GetProperty("config").TryGetProperty("consumer_limits", out var limits))
        {
            return default;
        }

        return (
            limits.TryGetProperty("max_ack_pending", out var maxAckPending) ? maxAckPending.GetInt64() : 0,
            limits.TryGetProperty("inactive_threshold", out var inactiveThreshold) ? inactiveThreshold.GetInt64() : 0);
    }

    private static string DescribeApiError(string entity, ApiError error, TopologyMode mode, bool hasConfiguration)
    {
        var guidance = (error.Code, mode, hasConfiguration) switch
        {
            (404, TopologyMode.Validate, true) => "Create it externally or select Provision. Validate is read-only.",
            (404, TopologyMode.Validate, false) => "Create it externally or supply native Configuration and select Provision. Validate is read-only.",
            (404, TopologyMode.Provision, false) => "Create it externally or supply native Configuration for Provision.",
            _ => "Reconcile the declaration and broker settings externally. Relayer never updates or deletes entities."
        };

        return $"{entity}: {error.Description}. {guidance}";
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

        if (!consumer && expected["duplicate_window"] is null && expected["mirror"] is null && expected["sources"] is null)
        {
            CompareDefaultDuplicateWindow(entity, expected, actual, differences);
            actual.Remove("duplicate_window");
        }

        var names = new HashSet<string>(expected.Select(pair => pair.Key), StringComparer.Ordinal);
        names.UnionWith(actual.Select(pair => pair.Key));

        foreach (var name in names)
        {
            expected.TryGetPropertyValue(name, out var wanted);
            actual.TryGetPropertyValue(name, out var found);

            if (JsonNode.DeepEquals(wanted, found))
            {
                continue;
            }

            differences.Add($"{entity}: setting '{name}' expected {wanted?.ToJsonString() ?? "null"}, actual {found?.ToJsonString() ?? "null"}. Reconcile settings externally; Validate is read-only.");
        }
    }

    private static void ApplyConsumerDefaults(ConsumerConfig expected, ConsumerConfig actual, JsonObject expectedConfig)
    {
        if (expected.PriorityPolicy == ConsumerConfigPriorityPolicy.PinnedClient)
        {
            Default(expectedConfig, "priority_timeout", 120_000_000_000L);
        }

        if (string.IsNullOrEmpty(expected.DeliverSubject) && actual.MaxBatch > 0)
        {
            Default(expectedConfig, "max_batch", actual.MaxBatch);
        }

        if (expected.AckPolicy != ConsumerConfigAckPolicy.None)
        {
            // Consumer info exposes the effective value. A server or account cap can only lower the default.
            const long DefaultMaxAckPending = 1000;
            var maxAckPending = actual.MaxAckPending is > 0 and <= DefaultMaxAckPending ? actual.MaxAckPending : DefaultMaxAckPending;
            Default(expectedConfig, "max_ack_pending", maxAckPending);
        }
    }

    private static void CompareDefaultDuplicateWindow(string entity, JsonObject expected, JsonObject actual, List<string> differences)
    {
        // Stream info reports the effective window but does not identify the server-wide cap.
        const long MinimumWindow = 100_000_000;
        const long DefaultWindow = 120_000_000_000;
        var maximumAge = expected["max_age"]?.GetValue<long>() ?? 0;
        var maximumWindow = maximumAge > 0 ? Math.Min(maximumAge, DefaultWindow) : DefaultWindow;
        var actualWindow = actual["duplicate_window"]?.GetValue<long>();

        if (actualWindow is null || actualWindow < MinimumWindow || actualWindow > maximumWindow)
        {
            differences.Add($"{entity}: setting 'duplicate_window' expected a JetStream default between {MinimumWindow} and {maximumWindow}, actual {actualWindow?.ToString() ?? "null"}. Reconcile settings externally; Validate is read-only.");
        }
    }

    private static void Normalize(JsonObject config, bool consumer)
    {
        // Server defaults for the pinned JetStream protocol. An explicit non-default remains visible.
        if (consumer)
        {
            if (config["backoff"] is JsonArray { Count: > 0 } backoff)
            {
                config["ack_wait"] = backoff[0]!.DeepClone();
            }
            else
            {
                Default(config, "ack_wait", 30_000_000_000L);
            }

            Default(config, "max_deliver", -1);
            Default(config, "max_waiting", 512);
        }
        else
        {
            NormalizeStreamLimit(config, "max_consumers");
            NormalizeStreamLimit(config, "max_msgs");
            NormalizeStreamLimit(config, "max_msgs_per_subject");
            NormalizeStreamLimit(config, "max_bytes");
            NormalizeStreamLimit(config, "max_msg_size");
            Default(config, "num_replicas", 1);
            config["persist_mode"] ??= "default";

            if (config["mirror"] is null && config["sources"] is null or JsonArray { Count: 0 })
            {
                if (config["subjects"] is null or JsonArray { Count: 0 } && config["name"] is JsonValue streamName)
                {
                    config["subjects"] = new JsonArray(streamName.DeepClone());
                }
            }
        }

        if (config["metadata"] is JsonObject metadata)
        {
            foreach (var name in metadata.Select(pair => pair.Key).ToArray())
            {
                if (name.StartsWith("_nats.", StringComparison.Ordinal))
                {
                    metadata.Remove(name);
                }
            }
        }
        // Empty optional fields and omitted scalar defaults are equivalent native JSON.
        foreach (var name in config.Select(pair => pair.Key).ToArray())
        {
            var value = config[name];

            if (value is null || value.ToJsonString() is "false" or "0" or "\"\"" or "[]" or "{}")
            {
                config.Remove(name);
            }
        }

        foreach (var name in new[] { "subjects", "filter_subjects" })
        {
            if (config[name] is JsonArray subjects)
            {
                config[name] = new JsonArray(subjects.Select(item => item!.GetValue<string>()).Order(StringComparer.Ordinal).Select(item => (JsonNode?)JsonValue.Create(item)).ToArray());
            }
        }
    }

    private static void Default(JsonObject config, string name, long value)
    {
        if (config[name] is null || config[name]!.ToJsonString() == "0")
        {
            config[name] = value;
        }
    }

    private static void NormalizeStreamLimit(JsonObject config, string name)
    {
        if (config[name]?.GetValue<long>() is null or <= 0)
        {
            config[name] = -1;
        }
    }

    private static void ValidateName(string name)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);

        if (name.AsSpan().ContainsAny(".*>/\\"))
        {
            throw new ArgumentException($"Invalid NATS entity name '{name}': use a native stream or consumer name without '.', '*', '>', '/' or '\\'.");
        }
    }
}
