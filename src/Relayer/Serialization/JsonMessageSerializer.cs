using System.Buffers;
using System.Collections.Frozen;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;

namespace Relayer.Serialization;

internal sealed class JsonMessageSerializer : IMessageSerializer
{
    private readonly FrozenDictionary<object, Entry> _messageTypes;

    public JsonMessageSerializer(JsonMessageType[] messageTypes)
    {
        var entries = new Dictionary<object, Entry>(messageTypes.Length, ReferenceEqualityComparer.Instance);
        var pools = new Dictionary<JsonSerializerOptions, WriterPool>(ReferenceEqualityComparer.Instance);
        for (var i = 0; i < messageTypes.Length; i++)
        {
            var typeInfo = messageTypes[i].TypeInfo;
            if (!pools.TryGetValue(typeInfo.Options, out var pool))
            {
                pool = new WriterPool(typeInfo.Options);
                pools.Add(typeInfo.Options, pool);
            }
            entries.Add(messageTypes[i].Key, new Entry(typeInfo, pool));
        }
        _messageTypes = entries.ToFrozenDictionary(ReferenceEqualityComparer.Instance);
    }

    public string ContentType => "application/json";

    public void Serialize<TMessage>(TMessage message, IBufferWriter<byte> destination)
    {
        ArgumentNullException.ThrowIfNull(destination);
        var entry = GetEntry<TMessage>();
        var typeInfo = (JsonTypeInfo<TMessage>)entry.TypeInfo;
        var pool = entry.Pool;
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
        var entry = GetEntry<TMessage>();
        var typeInfo = (JsonTypeInfo<TMessage>)entry.TypeInfo;
        var reader = new Utf8JsonReader(source, entry.ReaderOptions);
        var message = JsonSerializer.Deserialize(ref reader, typeInfo);
        if (reader.Read())
        {
            throw new JsonException("A message body must contain exactly one JSON value.");
        }
        return message is null ? throw new JsonException("A message body cannot be null.") : message;
    }

    private Entry GetEntry<TMessage>()
    {
        // The generic key is initialized at registration. No reflection, Type lookup or boxing here.
        if (_messageTypes.TryGetValue(JsonMessageType<TMessage>.Key, out var entry))
        {
            return entry;
        }
        throw new InvalidOperationException("No JSON metadata is registered for this message. Register its [Message] type and call AddMessageTypesFromThisAssembly or UseJsonSerializer with a source-generated context.");
    }

    private sealed class Entry(JsonTypeInfo typeInfo, WriterPool pool)
    {
        public JsonTypeInfo TypeInfo { get; } = typeInfo;
        public WriterPool Pool { get; } = pool;
        public JsonReaderOptions ReaderOptions { get; } = new()
        {
            AllowTrailingCommas = typeInfo.Options.AllowTrailingCommas,
            CommentHandling = typeInfo.Options.ReadCommentHandling,
            MaxDepth = typeInfo.Options.MaxDepth
        };
    }

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
