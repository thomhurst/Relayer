using System.Globalization;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Text.Json.Schema;

namespace Relayer;

internal static class AsyncApiExporter
{
    internal static void Export(Stream destination, AsyncApiOptions options,
        IReadOnlyList<TransportDefinition> transports, MessageTypeRegistry registry, HandlerRegistry handlers,
        IEnumerable<JsonSerializerRegistration> registrations)
    {
        ArgumentNullException.ThrowIfNull(destination);
        ArgumentNullException.ThrowIfNull(options);
        ArgumentException.ThrowIfNullOrWhiteSpace(options.Title);
        ArgumentException.ThrowIfNullOrWhiteSpace(options.Version);

        var contexts = registrations.Select(registration => registration.Context).ToArray();
        var metadata = registry.CreateJsonTypes(contexts);
        var messageKeys = new Dictionary<MessageType, string>();
        var messages = new JsonObject();
        foreach (var contract in metadata)
        {
            var type = contract.TypeInfo.Type;
            if (!registry.TryGetMessageType(type, out var identity))
                throw new InvalidOperationException($"No stable message identity exists for '{type}'.");
            // Hex encoding preserves arbitrary wire names without component-key or JSON Pointer collisions.
            var key = "m_" + Convert.ToHexString(Encoding.UTF8.GetBytes(identity.Name)) + "_v" + Number(identity.Version);
            messageKeys.Add(identity, key);
            var schema = contract.TypeInfo.GetJsonSchemaAsNode();
            if (schema is JsonObject schemaObject)
            {
                schemaObject["$schema"] = "https://json-schema.org/draft/2020-12/schema";
                // Embedded schemas are separate resources: recursive '#' references stay inside this payload.
                schemaObject["$id"] = "urn:relayer:message:" + key;
            }
            messages.Add(key, new JsonObject
            {
                ["name"] = identity.Name,
                ["x-relayer-version"] = identity.Version,
                ["x-relayer-wire-type"] = identity.ToString(),
                ["payload"] = new JsonObject
                {
                    ["schemaFormat"] = "application/schema+json;version=draft-2020-12",
                    ["schema"] = schema
                }
            });
        }

        var channels = new JsonObject();
        var operations = new JsonObject();
        var endpoints = new Dictionary<(string Transport, string Address), Channel>();
        foreach (var transport in transports)
        {
            foreach (var route in transport.Routes)
            {
                var channel = GetChannel(channels, endpoints, transport.Name, route.Destination);
                var key = GetMessageKey(registry, messageKeys, route.MessageType);
                AddMessage(channel, key);
                var operation = CreateOperation("send", channel, [key]);
                operation["x-relayer-has-ordering-key"] = route.HasOrderingKey;
                operations.Add("operation" + Number(operations.Count), operation);
            }
            foreach (var consumer in transport.Consumers)
            {
                var channel = GetChannel(channels, endpoints, transport.Name, consumer.Source);
                var keys = new List<string>();
                foreach (var handler in handlers.Descriptors)
                {
                    if (!consumer.Handlers.Contains(handler.HandlerType)) continue;
                    var key = GetMessageKey(registry, messageKeys, handler.MessageType);
                    if (keys.Contains(key)) continue;
                    keys.Add(key);
                    AddMessage(channel, key);
                }
                var operation = CreateOperation("receive", channel, keys);
                operation["x-relayer-concurrency"] = consumer.Concurrency;
                operation["x-relayer-batch-size"] = consumer.BatchSize;
                operations.Add("operation" + Number(operations.Count), operation);
            }
        }

        var document = new JsonObject
        {
            ["asyncapi"] = "3.0.0",
            ["info"] = new JsonObject { ["title"] = options.Title, ["version"] = options.Version },
            ["x-relayer-schema-source"] = "Registered System.Text.Json contracts; custom wire formats and protocol bindings are not exported.",
            ["channels"] = channels,
            ["operations"] = operations,
            ["components"] = new JsonObject { ["messages"] = messages }
        };
        using var writer = new Utf8JsonWriter(destination, new JsonWriterOptions { Indented = true });
        document.WriteTo(writer);
    }

    private static Channel GetChannel(JsonObject channels, Dictionary<(string, string), Channel> endpoints,
        string transport, string address)
    {
        if (endpoints.TryGetValue((transport, address), out var channel)) return channel;
        var key = "channel" + Number(channels.Count);
        var messages = new JsonObject();
        channels.Add(key, new JsonObject
        {
            ["address"] = address,
            ["x-relayer-transport"] = transport,
            ["messages"] = messages
        });
        channel = new Channel(key, messages);
        endpoints.Add((transport, address), channel);
        return channel;
    }

    private static void AddMessage(Channel channel, string key)
    {
        if (!channel.Messages.ContainsKey(key))
            channel.Messages.Add(key, Reference("#/components/messages/" + key));
    }

    private static JsonObject CreateOperation(string action, Channel channel, IEnumerable<string> keys)
    {
        var references = new JsonArray();
        foreach (var key in keys)
            references.Add((JsonNode)Reference("#/channels/" + channel.Key + "/messages/" + key));
        return new JsonObject
        {
            ["action"] = action,
            ["channel"] = Reference("#/channels/" + channel.Key),
            ["messages"] = references
        };
    }

    private static string GetMessageKey(MessageTypeRegistry registry, Dictionary<MessageType, string> messages, Type type) =>
        registry.TryGetMessageType(type, out var identity) && messages.TryGetValue(identity, out var key) ? key : throw new InvalidOperationException(
            $"Register a stable message identity for '{type}' before exporting AsyncAPI.");

    private static JsonObject Reference(string path) => new() { ["$ref"] = path };
    private static string Number(int value) => value.ToString(CultureInfo.InvariantCulture);
    private sealed record Channel(string Key, JsonObject Messages);
}
