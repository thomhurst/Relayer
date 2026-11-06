using System.Buffers;
using System.Collections.Frozen;
using System.Diagnostics.CodeAnalysis;
using Dekaf.SchemaRegistry;
using Dekaf.SchemaRegistry.Avro;
using Dekaf.SchemaRegistry.Avro.Poco;
using Dekaf.Serialization;

namespace Relayer.Serialization.Avro;

/// <summary>Uses Dekaf's generated Avro codecs and prepares registry contracts before message processing starts.</summary>
/// <remarks>The registry client is caller-owned. Declare contracts before initialization; thereafter concurrent reads and writes are supported.
/// Only the prepared writer schema ID is accepted. Use explicit wire types and Relayer upcasters for older versions.
/// Trimmed JIT schema preparation remains unsupported; registration emits a trimming diagnostic, including in Native AOT builds.</remarks>
public sealed class AvroMessageSerializer(ISchemaRegistryClient client) : IMessageSerializer, IMessageSerializerInitializer, IAsyncDisposable
{
    private readonly ISchemaRegistryClient _client = client ?? throw new ArgumentNullException(nameof(client));
    private readonly Dictionary<object, Entry> _entries = new(ReferenceEqualityComparer.Instance);
    private FrozenDictionary<object, Entry>? _prepared;
    private bool _initializing;
    private bool _disposed;

    /// <inheritdoc />
    public string ContentType => "avro/binary";

    /// <summary>Declares a generated codec and its native topic/subject configuration.</summary>
    /// <remarks>Topic selects the registry subject, not Relayer routing. Native configuration is passed unchanged.
    /// Dynamic schemas, rules and header identities require per-record preparation and are unsupported.</remarks>
    [RequiresUnreferencedCode("Dekaf 1.26.0 Avro schema preparation still reaches Apache.Avro/Newtonsoft.Json reflection in trimmed JIT applications. Trimming is unsupported; see https://github.com/thomhurst/Dekaf/issues/4060.")]
    public AvroMessageSerializer Add<TMessage, TCodec>(string topic, AvroSerializerConfig? config = null)
        where TCodec : struct, IAvroPocoCodec<TMessage>
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (_initializing) throw new InvalidOperationException("Schema declarations are frozen after initialization starts.");
        ArgumentException.ThrowIfNullOrWhiteSpace(topic);
        config ??= new();
        if (config.UseLatestVersion || config.RuleExecutor is not null || config.ValidationRulesExecution != ValidationRulesExecution.Disabled
            || config.SchemaIdStrategy != SchemaIdSerializerStrategy.Prefix || config.AsyncSubjectNameStrategy is not null
            || config.SubjectNameStrategy == SubjectNameStrategy.AssociatedName)
            throw new NotSupportedException("Use a fixed Avro writer schema with prefix schema IDs and a synchronous subject strategy, without latest-version selection or registry rules. Per-record registry preparation is unsupported.");
        if (_entries.ContainsKey(Key<TMessage>.Value))
            throw new InvalidOperationException("An Avro codec is already declared for this message.");
        _entries.Add(Key<TMessage>.Value, new Entry<TMessage, TCodec>(_client, topic, config));
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
        ArgumentNullException.ThrowIfNull(message);
        ArgumentNullException.ThrowIfNull(destination);
        Get<TMessage>().Write(message, destination);
    }

    /// <inheritdoc />
    public TMessage Deserialize<TMessage>(ReadOnlySequence<byte> source) => Get<TMessage>().Read(source);

    private Entry<TMessage> Get<TMessage>()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        var entries = _prepared ?? throw new InvalidOperationException("Initialize the Avro serializer before sending or consuming. Host startup does this automatically.");
        return entries.TryGetValue(Key<TMessage>.Value, out var entry) ? (Entry<TMessage>)entry
            : throw new InvalidOperationException("No Avro codec is declared for this message. Call AvroMessageSerializer.Add with its generated AvroCodec.");
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

    private abstract class Entry<TMessage> : Entry
    {
        public abstract void Write(TMessage message, IBufferWriter<byte> destination);
        public abstract TMessage Read(ReadOnlySequence<byte> source);
    }

    private sealed class Entry<TMessage, TCodec>(ISchemaRegistryClient client, string topic, AvroSerializerConfig config) : Entry<TMessage>
        where TCodec : struct, IAvroPocoCodec<TMessage>
    {
        private readonly AvroPocoSchemaRegistrySerializer<TMessage, TCodec> _writer = new(client, config);
        private readonly AvroPocoSchemaRegistryDeserializer<TMessage, TCodec> _reader = new(client,
            new() { SchemaIdStrategy = SchemaIdDeserializerStrategy.Prefix });
        private readonly SerializationContext _context = new() { Topic = topic, Component = SerializationComponent.Value };
        private int _schemaId;

        public override async ValueTask PrepareAsync(CancellationToken cancellationToken)
        {
            var resolved = await _writer.PrepareAsync(topic, cancellationToken: cancellationToken).ConfigureAwait(false);
            _schemaId = resolved.SchemaId;
            await _reader.WarmupAsync(_schemaId, cancellationToken).ConfigureAwait(false);
        }

        public override void Write(TMessage message, IBufferWriter<byte> destination) => _writer.Serialize(message, ref destination, _context);

        public override TMessage Read(ReadOnlySequence<byte> source)
        {
            var prefix = new SequenceReader<byte>(source);
            if (!prefix.TryRead(out var magic) || magic != 0 || !prefix.TryReadBigEndian(out int schemaId) || schemaId != _schemaId)
                throw new InvalidDataException("The record does not use the prepared Avro writer schema for this message.");
            if (source.IsSingleSegment) return Read(source.First);
            // Dekaf accepts contiguous memory. Keep its codec and use a pooled copy only for segmented input.
            var length = checked((int)source.Length);
            var buffer = ArrayPool<byte>.Shared.Rent(length);
            try
            {
                source.CopyTo(buffer);
                return Read(buffer.AsMemory(0, length));
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(buffer, clearArray: true);
            }
        }

        private TMessage Read(ReadOnlyMemory<byte> source)
        {
            // Cache-only native API cannot fall back to registry I/O, even if its prepared state is missing.
            if (!((IAsyncDeserializerPreparer<TMessage>)_reader).TryDeserialize(source, _context, out var message))
                throw new InvalidOperationException("The prepared Avro reader state is unavailable. Recreate and initialize the serializer.");
            return message is null ? throw new InvalidDataException("A message body cannot be null.") : message;
        }

        public override async ValueTask DisposeAsync()
        {
            await _writer.DisposeAsync().ConfigureAwait(false);
            await _reader.DisposeAsync().ConfigureAwait(false);
        }
    }
}
