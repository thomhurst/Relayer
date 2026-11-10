using System.Buffers.Binary;
using System.Runtime.CompilerServices;
using System.Text;
using MongoDB.Bson;
using MongoDB.Driver;

namespace Relayer.Outbox.MongoDB;

/// <summary>Scoped bounded reads and durable fenced mutations. Does not start dispatch workers.</summary>
public sealed class MongoDBOutboxStore : IOutboxStore
{
    private readonly IMongoCollection<BsonDocument> _collection;
    private readonly List<HeaderSlot> _headers = [];

    /// <summary>Uses the application-owned database and its native configuration unchanged.</summary>
    public MongoDBOutboxStore(IMongoDatabase database, MongoDBOutboxOptions? options = null)
    {
        ArgumentNullException.ThrowIfNull(database);
        options ??= new();
        options.Validate();
        MongoDBSerializerRoots.Preserve();
        _collection = database.GetCollection<BsonDocument>(options.CollectionPrefix + "outbox");
        if (_collection.Settings.WriteConcern?.IsAcknowledged == false)
            throw new ArgumentException("MongoDB outbox mutations require acknowledged writes. Configure an acknowledged native WriteConcern.", nameof(database));
    }

    /// <inheritdoc />
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    public async ValueTask<int> ReadPendingAsync(OutboxReadRequest request, Memory<OutboxRecord> destination,
        CancellationToken cancellationToken)
    {
        if (request.Lane < 0) throw new ArgumentOutOfRangeException(nameof(request), "Lane must not be negative.");
        cancellationToken.ThrowIfCancellationRequested();
        if (destination.IsEmpty) return 0;
        var filter = Eligible(request.Now);
        filter.Add("lane", request.Lane is { } lane ? new BsonInt32(lane) : BsonNull.Value);
        if (request.AfterId is { } after) filter.Add("_id", new BsonDocument("$gt", Id(after)));
        using var cursor = await _collection.FindAsync(filter, new FindOptions<BsonDocument>
        {
            Sort = new BsonDocument("_id", 1), Limit = destination.Length, BatchSize = destination.Length
        }, cancellationToken).ConfigureAwait(false);
        var count = 0;
        while (await cursor.MoveNextAsync(cancellationToken).ConfigureAwait(false))
        {
            foreach (var row in cursor.Current)
            {
                if (_headers.Count == count) _headers.Add(new());
                var headers = _headers[count].Read(row["headers"].AsByteArray);
                var message = new OutgoingMessage(row["destination"].AsString, row["body"].AsByteArray, headers,
                    row["ordering_key"].IsBsonNull ? null : new WireString(row["ordering_key"].AsByteArray),
                    row["deliver_at_utc_ticks"].IsBsonNull ? null : new DateTimeOffset(row["deliver_at_utc_ticks"].AsInt64, TimeSpan.Zero));
                destination.Span[count++] = new(row["_id"].AsBsonBinaryData.ToGuid(GuidRepresentation.Standard),
                    row["transport"].AsString, row["message_type"].AsString, message,
                    new DateTimeOffset(row["created_at_utc_ticks"].AsInt64, TimeSpan.Zero),
                    row["lane"].IsBsonNull ? null : row["lane"].AsInt32, row["attempts"].AsInt32,
                    row["last_error"].IsBsonNull ? null : row["last_error"].AsString,
                    row["tenant_id"].IsBsonNull ? null : row["tenant_id"].AsString);
            }
        }
        return count;
    }

    /// <inheritdoc />
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    public async ValueTask<bool> TryClaimAsync(OutboxClaim claim, DateTimeOffset now, DateTimeOffset expiresAt,
        CancellationToken cancellationToken)
    {
        Validate(claim, now, expiresAt);
        var filter = Eligible(now).Add("_id", Id(claim.MessageId));
        var result = await _collection.UpdateOneAsync(filter, new BsonDocument("$set", new BsonDocument
        {
            { "claim_token", Id(claim.Token) }, { "claim_expires_at_utc_ticks", expiresAt.UtcTicks }
        }), cancellationToken: cancellationToken).ConfigureAwait(false);
        return result.MatchedCount == 1;
    }

    /// <inheritdoc />
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    public async ValueTask<bool> RenewClaimAsync(OutboxClaim claim, DateTimeOffset now, DateTimeOffset expiresAt,
        CancellationToken cancellationToken)
    {
        Validate(claim, now, expiresAt);
        var result = await _collection.UpdateOneAsync(Fenced(claim, now),
            new BsonDocument("$max", new BsonDocument("claim_expires_at_utc_ticks", expiresAt.UtcTicks)),
            cancellationToken: cancellationToken).ConfigureAwait(false);
        return result.MatchedCount == 1;
    }

    /// <inheritdoc />
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    public async ValueTask<bool> CompleteAsync(OutboxClaim claim, DateTimeOffset now, CancellationToken cancellationToken)
    {
        Validate(claim);
        var result = await _collection.DeleteOneAsync(Fenced(claim, now), cancellationToken).ConfigureAwait(false);
        return result.DeletedCount == 1;
    }

    /// <inheritdoc />
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    public async ValueTask<bool> RecordFailureAsync(OutboxClaim claim, string error, DateTimeOffset now,
        CancellationToken cancellationToken)
    {
        Validate(claim);
        ArgumentNullException.ThrowIfNull(error);
        var result = await _collection.UpdateOneAsync(Fenced(claim, now), new BsonDocument
        {
            { "$inc", new BsonDocument("attempts", 1) },
            { "$set", new BsonDocument { { "last_error", error }, { "claim_token", BsonNull.Value }, { "claim_expires_at_utc_ticks", BsonNull.Value } } }
        }, cancellationToken: cancellationToken).ConfigureAwait(false);
        return result.MatchedCount == 1;
    }

    /// <inheritdoc />
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    public async ValueTask<long?> GetPendingCountAsync(DateTimeOffset now, CancellationToken cancellationToken) =>
        await _collection.CountDocumentsAsync(Eligible(now), cancellationToken: cancellationToken).ConfigureAwait(false);

    private static BsonDocument Eligible(DateTimeOffset now) => new()
    {
        { "status", "pending" },
        { "$and", new BsonArray { Due("deliver_at_utc_ticks", now), Due("claim_expires_at_utc_ticks", now) } }
    };

    private static BsonDocument Due(string field, DateTimeOffset now) => new("$or", new BsonArray
    {
        new BsonDocument(field, BsonNull.Value), new BsonDocument(field, new BsonDocument("$lte", now.UtcTicks))
    });

    private static BsonDocument Fenced(OutboxClaim claim, DateTimeOffset now) => new()
    {
        { "_id", Id(claim.MessageId) }, { "claim_token", Id(claim.Token) },
        { "claim_expires_at_utc_ticks", new BsonDocument("$gt", now.UtcTicks) }
    };

    private static BsonBinaryData Id(Guid id) => new(id, GuidRepresentation.Standard);

    private static void Validate(OutboxClaim claim, DateTimeOffset? now = null, DateTimeOffset? expiresAt = null)
    {
        if (claim.MessageId == Guid.Empty || claim.Token == Guid.Empty)
            throw new ArgumentException("Outbox message ID and claim token must not be empty.", nameof(claim));
        if (expiresAt <= now) throw new ArgumentOutOfRangeException(nameof(expiresAt), "Claim expiry must be after now.");
    }

    private sealed class HeaderSlot
    {
        private KeyValuePair<string, WireString>[] _entries = [];
        private byte[][] _names = [];

        internal ReadOnlyMemory<KeyValuePair<string, WireString>> Read(ReadOnlyMemory<byte> bytes)
        {
            var offset = 0;
            var count = Length(bytes.Span, ref offset);
            if (count > (bytes.Length - offset) / 8) throw new FormatException("Invalid MongoDB outbox header count.");
            if (_entries.Length < count) { Array.Resize(ref _entries, count); Array.Resize(ref _names, count); }
            for (var i = 0; i < count; i++)
            {
                var nameLength = Length(bytes.Span, ref offset);
                if (nameLength > bytes.Length - offset) throw new FormatException("Invalid MongoDB outbox header name.");
                var nameBytes = bytes.Span.Slice(offset, nameLength);
                var name = _entries[i].Key;
                if (_names[i] is null || !nameBytes.SequenceEqual(_names[i]))
                {
                    _names[i] = nameBytes.ToArray();
                    name = Encoding.UTF8.GetString(nameBytes);
                }
                offset += nameLength;
                var valueLength = Length(bytes.Span, ref offset);
                if (valueLength > bytes.Length - offset) throw new FormatException("Invalid MongoDB outbox header value.");
                _entries[i] = new(name, new WireString(bytes.Slice(offset, valueLength)));
                offset += valueLength;
            }
            if (offset != bytes.Length) throw new FormatException("Trailing MongoDB outbox header bytes.");
            // Clear previous values while retaining names for reuse with stable header sets.
            for (var i = count; i < _entries.Length; i++) _entries[i] = new(_entries[i].Key, default);
            return _entries.AsMemory(0, count);
        }

        private static int Length(ReadOnlySpan<byte> bytes, ref int offset)
        {
            if (bytes.Length - offset < 4) throw new FormatException("Truncated MongoDB outbox headers.");
            var value = BinaryPrimitives.ReadInt32LittleEndian(bytes[offset..]);
            offset += 4;
            if (value < 0) throw new FormatException("Negative MongoDB outbox header length.");
            return value;
        }
    }
}
