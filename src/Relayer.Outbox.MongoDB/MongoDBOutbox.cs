using System.Runtime.CompilerServices;
using MongoDB.Bson;
using MongoDB.Driver;

namespace Relayer.Outbox.MongoDB;

/// <summary>Scoped outgoing writes into an application-owned native MongoDB transaction.</summary>
/// <remarks>Await every send sequentially. The caller starts, commits, aborts and disposes the native session.</remarks>
public sealed class MongoDBOutbox : IOutboxEnlistment
{
    private readonly IMongoDatabase _database;
    private readonly IMongoCollection<RawBsonDocument> _collection;
    private readonly int _lanes;
    private readonly MongoDBOutboxBuffer _buffer = new();
    private Binding? _binding;
    private int _writing;

    /// <summary>Uses the application's database and native client configuration unchanged.</summary>
    public MongoDBOutbox(IMongoDatabase database, MongoDBOutboxOptions? options = null)
    {
        ArgumentNullException.ThrowIfNull(database);
        options ??= new();
        options.Validate();
        MongoDBSerializerRoots.Preserve();
        _database = database;
        _collection = database.GetCollection<RawBsonDocument>(options.CollectionPrefix + "outbox");
        _lanes = options.Lanes;
    }

    /// <inheritdoc />
    public bool IsActive => _binding is { IsActive: true };

    /// <summary>Associates this scope's sender with one already-active native transaction.</summary>
    /// <remarks>
    /// Dispose the returned association when the transaction ends, including inside each WithTransactionAsync retry.
    /// Relayer does not commit, retry, abort, dispose or change configuration on the caller's session.
    /// </remarks>
    public IDisposable EnlistTransaction(IClientSessionHandle session)
    {
        ArgumentNullException.ThrowIfNull(session);
        if (_binding is not null) throw new InvalidOperationException("Dispose the current MongoDB outbox association before enlisting another transaction.");
        if (!ReferenceEquals(session.Client, _database.Client)) throw new ArgumentException("The session must belong to the outbox database's MongoClient.", nameof(session));
        if (!session.IsInTransaction) throw new InvalidOperationException("Start a native MongoDB transaction before enlisting it in Relayer.");
        return _binding = new(this, session);
    }

    /// <inheritdoc />
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    public async ValueTask EnlistAsync(OutboxRecord record, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        if (_binding is not { IsActive: true } binding)
            throw new InvalidOperationException("Enlist an active native MongoDB transaction before sending through the outbox.");
        if (Interlocked.CompareExchange(ref _writing, 1, 0) != 0)
            throw new InvalidOperationException("MongoDB transactions require sequential operations. Await each outbox send before starting another.");
        try
        {
            using var document = _buffer.Write(record, _lanes);
            // The native driver has consumed the borrowed bytes and stored them in the transaction before returning.
            await _collection.InsertOneAsync(binding.Session, document, cancellationToken: cancellationToken).ConfigureAwait(false);
        }
        finally { Volatile.Write(ref _writing, 0); }
    }

    private sealed class Binding : IDisposable
    {
        private readonly MongoDBOutbox _owner;
        private readonly long _transactionNumber;
        internal IClientSessionHandle Session { get; }
        internal bool IsActive => Session.IsInTransaction && Session.WrappedCoreSession.CurrentTransaction.TransactionNumber == _transactionNumber;

        internal Binding(MongoDBOutbox owner, IClientSessionHandle session)
        {
            _owner = owner;
            Session = session;
            _transactionNumber = session.WrappedCoreSession.CurrentTransaction.TransactionNumber;
        }

        public void Dispose()
        {
            if (Volatile.Read(ref _owner._writing) != 0)
                throw new InvalidOperationException("Await the active MongoDB outbox send before disposing its transaction association.");
            if (ReferenceEquals(_owner._binding, this)) _owner._binding = null;
        }
    }
}
