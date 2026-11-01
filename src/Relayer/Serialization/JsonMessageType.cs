using System.Text.Json.Serialization.Metadata;

namespace Relayer.Serialization;

internal readonly record struct JsonMessageType(object Key, JsonTypeInfo TypeInfo);

internal static class JsonMessageType<TMessage>
{
    // Immutable identity only. Metadata, options and pools belong to each serializer.
    internal static readonly object Key = new();
}
