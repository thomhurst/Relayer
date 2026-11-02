using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Frozen;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;
using Dekaf.SchemaRegistry;
using Dekaf.Serialization;
using Relayer.Serialization;

namespace Relayer.Kafka;

/// <summary>Uses Dekaf's AOT-safe JSON Schema Registry serializers with explicit contracts prepared at host startup.</summary>
/// <remarks>The registry client is caller-owned. Only each declared writer schema is accepted on receive.
/// Register older wire versions as separate message types and use Relayer upcasters. Registry I/O never runs during serialization.</remarks>
public sealed class KafkaJsonSchemaSerializer(ISchemaRegistryClient client) : IMessageSerializer, IMessageSerializerInitializer, IAsyncDisposable
{
    private readonly ISchemaRegistryClient _client = client ?? throw new ArgumentNullException(nameof(client));
    private readonly Dictionary<object, Entry> _entries = new(ReferenceEqualityComparer.Instance);
    private FrozenDictionary<object, Entry>? _prepared;
    private bool _initializing;
    private bool _disposed;

    /// <inheritdoc />
    public string ContentType => "application/vnd.relayer.kafka.json-schema";

    /// <summary>Declares one message contract, its native topic/subject configuration and source-generated JSON metadata.</summary>
    /// <remarks>Topic is used by Dekaf for subject selection, not routing. Use the same topic in Send and Consume declarations.
    /// Native schema rules, latest-version selection and header identities require asynchronous per-record preparation and are unsupported.</remarks>
    public KafkaJsonSchemaSerializer Add<TMessage>(string topic, string schema, JsonTypeInfo<TMessage> typeInfo,
        JsonSchemaSerializerConfig? config = null)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (_initializing) throw new InvalidOperationException("Schema declarations are frozen after initialization starts.");
        ArgumentException.ThrowIfNullOrWhiteSpace(topic);
        ArgumentException.ThrowIfNullOrWhiteSpace(schema);
        ArgumentNullException.ThrowIfNull(typeInfo);
        config ??= new();
        if (config.UseLatestVersion || config.RuleExecutor is not null || config.SchemaIdStrategy != SchemaIdSerializerStrategy.Prefix)
            throw new NotSupportedException("Use a fixed JSON writer schema with prefix schema IDs, no latest-version selection and no registry rules. Per-record registry I/O is unsupported.");
        if (_entries.ContainsKey(Key<TMessage>.Value))
            throw new InvalidOperationException($"A schema for '{typeof(TMessage)}' is already declared.");
        _entries.Add(Key<TMessage>.Value, new Entry<TMessage>(_client, topic, schema, typeInfo, config));
        return this;
    }

    /// <inheritdoc />
    public async ValueTask InitializeAsync(CancellationToken cancellationToken)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (_prepared is not null) return;
        if (_initializing) throw new InvalidOperationException("Schema initialization is already running or previously failed. Recreate the serializer to retry.");
        cancellationToken.ThrowIfCancellationRequested();
        _initializing = true;
        foreach (var entry in _entries.Values)
            await entry.PrepareAsync(cancellationToken).ConfigureAwait(false);
        _prepared = _entries.ToFrozenDictionary(ReferenceEqualityComparer.Instance);
    }

    /// <inheritdoc />
    public void Serialize<TMessage>(TMessage message, IBufferWriter<byte> destination)
    {
        if (message is null) throw new ArgumentNullException(nameof(message));
        ArgumentNullException.ThrowIfNull(destination);
        Get<TMessage>().Serialize(message, destination);
    }

    /// <inheritdoc />
    public TMessage Deserialize<TMessage>(ReadOnlySequence<byte> source) => Get<TMessage>().Deserialize(source);

    private Entry<TMessage> Get<TMessage>()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        var entries = _prepared ?? throw new InvalidOperationException("Initialize the schema serializer before sending or consuming. Host startup does this automatically.");
        return entries.TryGetValue(Key<TMessage>.Value, out var entry) ? (Entry<TMessage>)entry
            : throw new InvalidOperationException("No schema is declared for this message on this transport. Call KafkaJsonSchemaSerializer.Add with source-generated metadata.");
    }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        if (_disposed) return;
        _disposed = true;
        foreach (var entry in _entries.Values) await entry.DisposeAsync().ConfigureAwait(false);
    }

    private static class Key<TMessage>
    {
        internal static readonly object Value = new();
    }

    private abstract class Entry : IAsyncDisposable
    {
        public abstract ValueTask PrepareAsync(CancellationToken cancellationToken);
        public abstract ValueTask DisposeAsync();
    }

    private sealed class Entry<TMessage>(ISchemaRegistryClient client, string topic, string schema,
        JsonTypeInfo<TMessage> typeInfo, JsonSchemaSerializerConfig config) : Entry
    {
        private readonly JsonSchemaRegistrySerializer<TMessage> _writer = new(client, schema, typeInfo, config);
        private readonly SerializationContext _context = new() { Topic = topic, Component = SerializationComponent.Value };
        private readonly JsonReaderOptions _readerOptions = new()
        {
            AllowTrailingCommas = typeInfo.Options.AllowTrailingCommas,
            CommentHandling = typeInfo.Options.ReadCommentHandling,
            MaxDepth = typeInfo.Options.MaxDepth,
        };
        private int _schemaId;

        public override async ValueTask PrepareAsync(CancellationToken cancellationToken)
        {
            var resolved = await _writer.PrepareAsync(topic, default!, cancellationToken: cancellationToken).ConfigureAwait(false);
            _schemaId = resolved.SchemaId;
        }

        public void Serialize(TMessage message, IBufferWriter<byte> destination) => _writer.Serialize(message, ref destination, _context);

        public TMessage Deserialize(ReadOnlySequence<byte> source)
        {
            // Use STJ's span entry point for the native Kafka record, retaining strict trailing-value checks.
            if (source.IsSingleSegment)
            {
                var span = source.FirstSpan;
                if (span.Length < 5 || span[0] != 0 || BinaryPrimitives.ReadInt32BigEndian(span[1..]) != _schemaId)
                    throw new JsonException("The record does not use the declared JSON writer schema on this transport.");
                var value = JsonSerializer.Deserialize(span[5..], typeInfo);
                return value is null ? throw new JsonException("A message body cannot be null.") : value;
            }
            var prefix = new SequenceReader<byte>(source);
            if (!prefix.TryRead(out var magic) || magic != 0 || !prefix.TryReadBigEndian(out int schemaId) || schemaId != _schemaId)
                throw new JsonException("The record does not use the declared JSON writer schema on this transport.");
            // Dekaf's synchronous deserializer fetches unknown/evicted schemas synchronously. The explicit
            // startup contract already resolved this ID; read JSON directly to keep registry I/O off this path.
            var reader = new Utf8JsonReader(source.Slice(prefix.Position), _readerOptions);
            var message = JsonSerializer.Deserialize(ref reader, typeInfo);
            if (reader.Read()) throw new JsonException("A message body must contain exactly one JSON value.");
            return message is null ? throw new JsonException("A message body cannot be null.") : message;
        }

        public override ValueTask DisposeAsync() => _writer.DisposeAsync();
    }
}
