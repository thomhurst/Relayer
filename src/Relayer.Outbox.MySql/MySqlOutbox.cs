using System.Data;
using System.Data.Common;
using System.Runtime.CompilerServices;
using Microsoft.Extensions.Options;
using MySqlConnector;

namespace Relayer.Outbox.MySql;

/// <summary>Scoped MySQL transaction enlistment. Dispatch workers are configured separately.</summary>
/// <remarks>Use one unit at a time in a scope. Commit through the returned transaction, and use its Connection for Dapper commands.</remarks>
public sealed class MySqlOutbox : IOutboxEnlistment, IDisposable, IAsyncDisposable
{
    private readonly List<BufferedOutboxMessage> _rows = [];
    private readonly MySqlOutboxBatch _batch;
    private readonly int _lanes;
    private readonly MySqlOutboxOptions _options;
    private readonly RelayerInstrumentation? _instrumentation;
    private readonly MySqlDirectDispatcher? _dispatcher;
    private MySqlOutboxTransaction? _transaction;
    private int _count;
    private bool _disposed;
    private readonly MySqlInboxPools? _inboxPools;
    private MySqlInboxPool? _inboxPool;

    /// <summary>Creates an enlistment using this scope's validated options.</summary>
    public MySqlOutbox(IOptionsSnapshot<MySqlOutboxOptions> options, RelayerInstrumentation? instrumentation = null)
        : this(options, instrumentation, null) { }

    internal MySqlOutbox(IOptionsSnapshot<MySqlOutboxOptions> options, RelayerInstrumentation? instrumentation,
        MySqlDirectDispatcher? dispatcher, MySqlInboxPools? inboxPools = null)
    {
        ArgumentNullException.ThrowIfNull(options);
        var value = options.Value;
        ValidateOptions(value);
        if (value.DispatchStoreName is not null && dispatcher is null)
            throw new InvalidOperationException("Resolve a MySQL outbox with DispatchStoreName through its registered service provider.");
        _inboxPools = inboxPools;
        _dispatcher = dispatcher;
        dispatcher?.ValidateStore(value.TablePrefix);
        _options = value;
        _lanes = value.Lanes;
        _instrumentation = instrumentation;
        _batch = new(MySqlOutboxBatch.CreateInsertSql(MySqlOutboxSchema.Table(value)));
    }

    /// <summary>The active transaction for ADO.NET/Dapper business commands, or null outside a unit.</summary>
    /// <remarks>During consumer handling Relayer owns completion. Use Connection to create commands; do not dispose or complete the transaction.</remarks>
    public DbTransaction? CurrentTransaction => IsActive ? _transaction : null;

    /// <inheritdoc />
    public bool IsActive => _transaction is { IsActive: true };

    /// <summary>Begins a transaction on an already open, caller-owned connection.</summary>
    /// <remarks>The caller must not complete the native transaction or close the connection behind this wrapper.</remarks>
    public async ValueTask<DbTransaction> BeginTransactionAsync(MySqlConnection connection,
        IsolationLevel isolationLevel = IsolationLevel.Unspecified, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(connection);
        EnsureAvailable();
        RejectAmbientTransaction();
        _dispatcher?.ValidateConnection(connection);
        var native = await connection.BeginTransactionAsync(isolationLevel, cancellationToken).ConfigureAwait(false);
        return _transaction = new(this, native, null);
    }

    /// <summary>Opens a connection from an application-owned data source. The transaction owns that connection only.</summary>
    public async ValueTask<DbTransaction> BeginTransactionAsync(MySqlDataSource dataSource,
        IsolationLevel isolationLevel = IsolationLevel.Unspecified, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(dataSource);
        EnsureAvailable();
        RejectAmbientTransaction();
        var connection = await dataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            _dispatcher?.ValidateConnection(connection);
            var native = await connection.BeginTransactionAsync(isolationLevel, cancellationToken).ConfigureAwait(false);
            return _transaction = new(this, native, connection);
        }
        catch
        {
            await connection.DisposeAsync().ConfigureAwait(false);
            throw;
        }
    }

    private static void RejectAmbientTransaction()
    {
        if (System.Transactions.Transaction.Current is not null)
            throw new InvalidOperationException("MySQL outbox transactions require explicit ownership. Call outside an ambient TransactionScope.");
    }

    private void EnsureAvailable()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (_transaction is not null)
            throw new InvalidOperationException("Complete or dispose the current MySQL outbox transaction before beginning another.");
    }

    /// <inheritdoc />
    public ValueTask EnlistAsync(OutboxRecord record, CancellationToken cancellationToken)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        cancellationToken.ThrowIfCancellationRequested();
        if (!IsActive) throw new InvalidOperationException("Begin a MySQL outbox transaction before sending via the outbox.");
        BufferedOutboxMessage.Validate(record);
        if (_count == _rows.Count) _rows.Add(new());
        _rows[_count].Copy(record, _lanes);
        _count++;
        return ValueTask.CompletedTask;
    }

    [RuntimeAsyncMethodGeneration(false)]
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    internal async ValueTask<bool> TryInsertInboxAsync(MySqlOutboxTransaction transaction, byte[] consumer,
        Guid id, CancellationToken cancellationToken)
    {
        var pool = _inboxPool ??= _inboxPools?.Get(_options.TablePrefix) ?? new MySqlInboxPool(_options);
        var scratch = pool.Rent();
        var command = scratch.Command;
        try
        {
            var native = transaction.Unwrap(transaction.Connection);
            command.Connection = native.Connection;
            command.Transaction = native;
            command.Parameters[0].Value = consumer;
            id.TryWriteBytes(scratch.Id, bigEndian: true, out _);
            return await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false) == 1;
        }
        finally
        {
            command.Connection = null;
            command.Transaction = null;
            command.Parameters[0].Value = DBNull.Value;
            pool.Return(scratch);
        }
    }

    internal MySqlOutboxBatch? Prepare(MySqlTransaction transaction)
    {
        if (_count == 0) return null;
        _batch.Prepare(transaction);
        for (var i = 0; i < _count; i++) _batch.Add(_rows[i]);
        return _batch;
    }

    internal void Committed()
    {
        _instrumentation?.AddOutboxPending(_count);
        _dispatcher?.Committed(_rows, _count);
    }

    internal void Ended(MySqlOutboxTransaction transaction)
    {
        if (!ReferenceEquals(_transaction, transaction)) return;
        for (var i = 0; i < _count; i++) _rows[i].Clear();
        _count = 0;
        _transaction = null;
    }

    internal static void ValidateOptions(MySqlOutboxOptions options)
    {
        _ = MySqlOutboxSchema.Table(options);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(options.MaxDispatchAttempts);
        if (options.DispatchStoreName is not null && string.IsNullOrWhiteSpace(options.DispatchStoreName))
            throw new ArgumentException("DispatchStoreName must be nonempty when supplied.", nameof(options));
        if (options.Lanes <= 0) throw new ArgumentOutOfRangeException(nameof(options), "Lanes must be positive.");
    }

    /// <inheritdoc />
    public void Dispose()
    {
        if (_disposed) return;
        try { _transaction?.Dispose(); }
        finally { _disposed = true; _batch.Dispose(); if (_inboxPools is null) _inboxPool?.Dispose(); }
    }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        if (_disposed) return;
        try
        {
            if (_transaction is not null) await _transaction.DisposeAsync().ConfigureAwait(false);
        }
        finally { _disposed = true; _batch.Dispose(); if (_inboxPools is null) _inboxPool?.Dispose(); }
    }
}
