using System.Buffers;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;

namespace Relayer.Serialization;

internal sealed class JsonMessageSerializer : IMessageSerializer
{
    private readonly Entry[] _messageTypes;

    public JsonMessageSerializer(JsonTypeInfo[] messageTypes)
    {
        _messageTypes = new Entry[messageTypes.Length];
        var pools = new Dictionary<JsonSerializerOptions, WriterPool>(ReferenceEqualityComparer.Instance);
        for (var i = 0; i < messageTypes.Length; i++)
        {
            var typeInfo = messageTypes[i];
            if (!pools.TryGetValue(typeInfo.Options, out var pool))
            {
                pool = new WriterPool(typeInfo.Options);
                pools.Add(typeInfo.Options, pool);
            }
            _messageTypes[i] = new Entry(typeInfo, pool);
        }
    }

    public string ContentType => "application/json";

    public void Serialize<TMessage>(TMessage message, IBufferWriter<byte> destination)
    {
        ArgumentNullException.ThrowIfNull(destination);
        var typeInfo = GetTypeInfo<TMessage>(out var pool);
        var writer = pool.Rent(destination);
        try
        {
            JsonSerializer.Serialize(writer, message, typeInfo);
            writer.Flush();
        }
        finally
        {
            pool.Return(writer);
        }
    }

    public TMessage Deserialize<TMessage>(ReadOnlySequence<byte> source)
    {
        var typeInfo = GetTypeInfo<TMessage>(out _);
        var options = typeInfo.Options;
        var reader = new Utf8JsonReader(source, new JsonReaderOptions
        {
            AllowTrailingCommas = options.AllowTrailingCommas,
            CommentHandling = options.ReadCommentHandling,
            MaxDepth = options.MaxDepth
        });
        var message = JsonSerializer.Deserialize(ref reader, typeInfo);
        if (reader.Read())
        {
            throw new JsonException("A message body must contain exactly one JSON value.");
        }
        return message is null ? throw new JsonException("A message body cannot be null.") : message;
    }

    private JsonTypeInfo<TMessage> GetTypeInfo<TMessage>(out WriterPool pool)
    {
        // Typed metadata is resolved once at startup. No reflection, Type lookup or boxing here.
        foreach (var entry in _messageTypes)
        {
            if (entry.TypeInfo is JsonTypeInfo<TMessage> typed)
            {
                pool = entry.Pool;
                return typed;
            }
        }
        throw new InvalidOperationException("No JSON metadata is registered for this message. Register its [Message] type and call AddMessageTypesFromThisAssembly or UseJsonSerializer with a source-generated context.");
    }

    private readonly record struct Entry(JsonTypeInfo TypeInfo, WriterPool Pool);

    private sealed class WriterPool(JsonSerializerOptions options)
    {
        private readonly Utf8JsonWriter?[] _writers = new Utf8JsonWriter?[Math.Max(1, Environment.ProcessorCount)];
        private readonly ArrayBufferWriter<byte> _idleDestination = new(1);
        private readonly JsonWriterOptions _options = new()
        {
            Encoder = options.Encoder,
            Indented = options.WriteIndented,
            IndentCharacter = options.IndentCharacter,
            IndentSize = options.IndentSize,
            NewLine = options.NewLine,
            MaxDepth = options.MaxDepth
        };

        public Utf8JsonWriter Rent(IBufferWriter<byte> destination)
        {
            for (var i = 0; i < _writers.Length; i++)
            {
                var writer = Interlocked.Exchange(ref _writers[i], null);
                if (writer is null)
                {
                    continue;
                }
                writer.Reset(destination);
                return writer;
            }
            return new Utf8JsonWriter(destination, _options);
        }

        public void Return(Utf8JsonWriter writer)
        {
            // Reset() alone retains the caller's destination. Detach it even on failure.
            // The idle destination is never written to.
            writer.Reset(_idleDestination);
            for (var i = 0; i < _writers.Length; i++)
            {
                if (Interlocked.CompareExchange(ref _writers[i], writer, null) is null)
                {
                    return;
                }
            }
            writer.Dispose();
        }
    }
}
