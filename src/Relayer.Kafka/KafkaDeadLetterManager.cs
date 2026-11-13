using System.Globalization;
using Dekaf;
using Dekaf.Admin;
using Dekaf.Consumer;
using Dekaf.Serialization;

namespace Relayer.Kafka;

/// <summary>An owned native Kafka record snapshot, including null values, keys and ordered headers.</summary>
public sealed class KafkaDeadLetterFeature
{
    internal KafkaDeadLetterFeature(ConsumeResult<byte[], byte[]> record) => Record = record;
    /// <summary>The native record. A null Value denotes a tombstone; empty bytes are distinct.</summary>
    public ConsumeResult<byte[], byte[]> Record { get; }
}

internal sealed class KafkaDeadLetterManager : IDeadLetterManager
{
    private readonly string _name;
    private readonly KafkaTransportOptions _options;
    private readonly Dictionary<string, string> _sources = new(StringComparer.Ordinal);
    private readonly IReadOnlyList<DeadLetterSource> _listed;

    internal KafkaDeadLetterManager(string name, KafkaTransportOptions options, IReadOnlyList<IConsumerDefinition> consumers)
    {
        _name = name;
        _options = options;
        foreach (var declaration in consumers)
        {
            var consumer = (ConsumerDefinition<KafkaConsumerOptions>)declaration;
            var topic = consumer.Failure.DeadLetterEndpoint ?? consumer.Source + ".dead-letter";
            if (_sources.TryGetValue(topic, out var destination) && destination != consumer.Source)
                throw new InvalidOperationException($"Kafka dead-letter topic '{topic}' has multiple redrive destinations. Configure a distinct dead-letter topic for each source.");
            _sources[topic] = consumer.Source;
        }
        _listed = Array.AsReadOnly(_sources.Select(item => new DeadLetterSource(item.Key, item.Value)).ToArray());
    }

    public IReadOnlyList<DeadLetterSource> List() => _listed;

    public async ValueTask<DeadLetterPage> PeekAsync(string source, int maxMessages = 20, string? cursor = null,
        CancellationToken cancellationToken = default)
    {
        Validate(source, maxMessages);
        var (partition, offset) = ParseCursor(source, cursor);
        await using var admin = CreateAdmin();
        var topics = await admin.DescribeTopicsAsync([source], cancellationToken).ConfigureAwait(false);
        var count = topics[source].Partitions.Count;
        if (partition >= count) return new([], null);
        await using var reader = CreateReader();
        reader.Assign(new TopicPartition(source, partition));
        await reader.InitializeAsync(cancellationToken).ConfigureAwait(false);
        reader.Seek(new(source, partition, offset));
        var messages = new List<DeadLetterMessage>(maxMessages);
        var ended = false;
        while (messages.Count < maxMessages)
        {
            var record = await reader.ConsumeOneAsync(Timeout.InfiniteTimeSpan, cancellationToken).ConfigureAwait(false);
            if (record is null) continue;
            if (record.Value.IsPartitionEof) { ended = true; break; }
            messages.Add(Snapshot(record.Value));
            offset = checked(record.Value.Offset + 1);
        }
        if (ended) { partition++; offset = 0; }
        var next = partition >= count ? null : Cursor(source, partition, offset);
        return new(messages.AsReadOnly(), next);
    }

    public async ValueTask<int> RedriveAsync(string source, Func<DeadLetterMessage, bool>? filter = null,
        int maxMessages = 20, CancellationToken cancellationToken = default)
    {
        var destination = Validate(source, maxMessages);
        var ownership = new KafkaOwnership();
        var native = ReaderBuilder().WithGroupId("relayer.deadletters." + _name.Length.ToString(CultureInfo.InvariantCulture) + "." + _name + "." + source)
            .AddRebalanceListener(ownership).SubscribeTo(source);
        await using var reader = native.Build();
        var producerBuilder = new ProducerBuilder<WireString?, (ReadOnlyMemory<byte> Body, Headers Headers)?>()
            .WithBootstrapServers(_options.BootstrapServers.ToArray())
            .WithKeySerializer(new KafkaKeySerializer()).WithValueSerializer(new KafkaValueSerializer());
        _options.ConfigureProducer?.Invoke(producerBuilder);
        // Management progress must never acknowledge an unacknowledged send, even if ordinary sends use acks=0.
        producerBuilder.WithAcks(Dekaf.Producer.Acks.All);
        await using var producer = producerBuilder.Build();
        await producer.InitializeAsync(cancellationToken).ConfigureAwait(false);
        await reader.InitializeAsync(cancellationToken).ConfigureAwait(false);
        var changed = 0;
        for (var scanned = 0; scanned < maxMessages; scanned++)
        {
            var record = await reader.ConsumeOneAsync(Timeout.InfiniteTimeSpan, cancellationToken).ConfigureAwait(false);
            if (record is null) continue;
            var value = record.Value;
            var partition = new TopicPartition(source, value.Partition);
            if (value.IsPartitionEof)
            {
                reader.Pause([partition]);
                if (reader.Paused.Count == reader.Assignment.Count) break;
                scanned--;
                continue;
            }
            var version = ownership.Version;
            var snapshot = Snapshot(value);
            if (filter is not null && !filter(snapshot))
            {
                // Kafka commits partition prefixes. Never commit beyond an unmatched original.
                reader.Pause([partition]);
                if (reader.Paused.Count == reader.Assignment.Count) break;
                continue;
            }
            var owned = snapshot.Features.GetRequired<KafkaDeadLetterFeature>().Record;
            var headers = new Headers();
            foreach (var header in owned.Headers) headers.Add(header);
            var key = owned.IsKeyNull ? (WireString?)null : new WireString(owned.Key!);
            (ReadOnlyMemory<byte> Body, Headers Headers)? body = owned.Value is null ? null : (owned.Value, headers);
            // Only tombstones bypass the value serializer and need explicit native headers.
            if (body is null)
                await producer.ForTopic(destination).ProduceAsync(key, null, headers, cancellationToken).ConfigureAwait(false);
            else
                await producer.ProduceAsync(destination, key, body, cancellationToken).ConfigureAwait(false);
            if (version != ownership.Version || !reader.Assignment.Contains(partition))
                throw new InvalidOperationException("Kafka dead-letter assignment changed after redrive. The acknowledged copy may be delivered again; retry the operation.");
            await reader.CommitAsync([new(source, value.Partition, checked(value.Offset + 1))], cancellationToken).ConfigureAwait(false);
            changed++;
        }
        return changed;
    }

    public async ValueTask<int> PurgeAsync(string source, int maxMessages = 20, CancellationToken cancellationToken = default)
    {
        Validate(source, maxMessages);
        await using var admin = CreateAdmin();
        var topics = await admin.DescribeTopicsAsync([source], cancellationToken).ConfigureAwait(false);
        var deleted = 0;
        for (var partition = 0; partition < topics[source].Partitions.Count && deleted < maxMessages; partition++)
        {
            await using var reader = CreateReader();
            reader.Assign(new TopicPartition(source, partition));
            await reader.InitializeAsync(cancellationToken).ConfigureAwait(false);
            reader.Seek(new(source, partition, 0));
            var scanned = 0;
            long next = 0;
            while (deleted + scanned < maxMessages)
            {
                var record = await reader.ConsumeOneAsync(Timeout.InfiniteTimeSpan, cancellationToken).ConfigureAwait(false);
                if (record is null) continue;
                if (record.Value.IsPartitionEof) break;
                next = checked(record.Value.Offset + 1);
                scanned++;
            }
            if (scanned == 0) continue;
            // Native deletion advances the log start offset for every reader, including redrive groups.
            await admin.DeleteRecordsAsync(new Dictionary<TopicPartition, long> { [new(source, partition)] = next }, cancellationToken).ConfigureAwait(false);
            deleted += scanned;
        }
        return deleted;
    }

    private IAdminClient CreateAdmin()
    {
        var builder = new AdminClientBuilder().WithBootstrapServers(_options.BootstrapServers.ToArray());
        _options.ConfigureAdmin?.Invoke(builder);
        return builder.Build();
    }

    private ConsumerBuilder<byte[], byte[]> ReaderBuilder()
    {
        var builder = new ConsumerBuilder<byte[], byte[]>().WithBootstrapServers(_options.BootstrapServers.ToArray());
        _options.ConfigureConsumer?.Invoke(builder);
        return builder.WithKeyDeserializer(Serializers.ByteArray).WithValueDeserializer(Serializers.ByteArray)
            .WithOffsetCommitMode(OffsetCommitMode.Manual).WithAutoOffsetStore(false)
            .WithAutoOffsetReset(AutoOffsetReset.Earliest).WithPartitionEof();
    }

    private IKafkaConsumer<byte[], byte[]> CreateReader() => ReaderBuilder().Build();

    private string Validate(string source, int maxMessages)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(maxMessages, 1);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(maxMessages, 100);
        ArgumentNullException.ThrowIfNull(source);
        return _sources.TryGetValue(source, out var destination) ? destination
            : throw new ArgumentException($"Unknown Kafka dead-letter source '{source}'. Use List() to find configured sources.", nameof(source));
    }

    private static string Cursor(string source, int partition, long offset) =>
        source + ":" + partition.ToString(CultureInfo.InvariantCulture) + ":" + offset.ToString(CultureInfo.InvariantCulture);

    private static (int Partition, long Offset) ParseCursor(string source, string? cursor)
    {
        if (cursor is null) return (0, 0);
        var parts = cursor.Split(':');
        if (parts.Length != 3 || parts[0] != source ||
            !int.TryParse(parts[1], NumberStyles.None, CultureInfo.InvariantCulture, out var partition) ||
            !long.TryParse(parts[2], NumberStyles.None, CultureInfo.InvariantCulture, out var offset))
            throw new ArgumentException("Kafka peek cursor must belong to this dead-letter source.", nameof(cursor));
        return (partition, offset);
    }

    private static DeadLetterMessage Snapshot(ConsumeResult<byte[], byte[]> record)
    {
        var headers = new Headers();
        foreach (var header in record.Headers) headers.Add(header);
        var key = record.Key?.ToArray();
        var value = record.Value?.ToArray();
        var owned = new ConsumeResult<byte[], byte[]>(record.Topic, record.Partition, record.Offset,
            key!, record.IsKeyNull, value!, value is null, headers, record.TimestampMs, record.TimestampType,
            record.LeaderEpoch, Serializers.ByteArray, Serializers.ByteArray);
        var snapshot = new DeadLetterMessage { Body = value ?? [], MessageId = record.Topic + ":" + record.Partition.ToString(CultureInfo.InvariantCulture) + ":" + record.Offset.ToString(CultureInfo.InvariantCulture) };
        snapshot.Features.Set(new KafkaDeadLetterFeature(owned));
        return snapshot;
    }
}
