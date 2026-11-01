using System.Text.Json;

namespace Relayer;

/// <summary>The declared topology, available without connecting to a broker.</summary>
public interface ITopology
{
    /// <summary>Named transports, routes and consumers in declaration order, including native options.</summary>
    IReadOnlyList<TransportDefinition> Transports { get; }

    /// <summary>Exports declarations as UTF-8 JSON, using stable message identities. Leaves the stream open.</summary>
    void ExportJson(Stream destination);
}

internal sealed class Topology(RelayerConfiguration configuration, MessageTypeRegistry messages, HandlerRegistry handlers) : ITopology
{
    public IReadOnlyList<TransportDefinition> Transports => configuration.Transports;

    public void ExportJson(Stream destination)
    {
        ArgumentNullException.ThrowIfNull(destination);
        using var writer = new Utf8JsonWriter(destination, new JsonWriterOptions { Indented = true });
        writer.WriteStartObject();
        writer.WriteNumber("schemaVersion", 1);
        writer.WriteStartArray("transports");
        foreach (var transport in Transports)
        {
            writer.WriteStartObject();
            writer.WriteString("name", transport.Name);
            writer.WriteString("mode", (transport.TopologyMode ?? configuration.TopologyMode).ToString());
            writer.WriteStartArray("routes");
            foreach (var route in transport.Routes)
            {
                writer.WriteStartObject();
                writer.WriteString("destination", route.Destination);
                writer.WriteBoolean("hasOrderingKey", route.HasOrderingKey);
                writer.WritePropertyName("message");
                WriteMessage(writer, route.MessageType);
                writer.WriteEndObject();
            }
            writer.WriteEndArray();
            writer.WriteStartArray("consumers");
            foreach (var consumer in transport.Consumers)
            {
                writer.WriteStartObject();
                writer.WriteString("source", consumer.Source);
                writer.WriteNumber("concurrency", consumer.Concurrency);
                writer.WriteNumber("batchSize", consumer.BatchSize);
                writer.WriteStartArray("messages");
                var seen = new HashSet<Type>();
                foreach (var descriptor in handlers.Descriptors)
                    if (consumer.Handlers.Contains(descriptor.HandlerType) && seen.Add(descriptor.MessageType))
                        WriteMessage(writer, descriptor.MessageType);
                writer.WriteEndArray();
                writer.WriteEndObject();
            }
            writer.WriteEndArray();
            writer.WriteEndObject();
        }
        writer.WriteEndArray();
        writer.WriteEndObject();
    }

    private void WriteMessage(Utf8JsonWriter writer, Type type)
    {
        if (!messages.TryGetMessageType(type, out var identity))
            throw new InvalidOperationException($"Register a stable message identity for '{type}' before exporting topology.");
        writer.WriteStartObject();
        writer.WriteString("name", identity.Name);
        writer.WriteNumber("version", identity.Version);
        writer.WriteEndObject();
    }
}
