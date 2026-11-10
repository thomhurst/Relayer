using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Options;
using System.Data.Common;
using System.Data;
using System.Diagnostics.CodeAnalysis;

namespace Relayer.Outbox.SqlServer;

/// <summary>Scoped SQL Server transaction enlistment. Dispatch workers are configured separately.</summary>
/// <remarks>Use one unit at a time in a scope. Commit through the returned transaction, and use its Connection for Dapper commands.</remarks>
public sealed class SqlServerOutbox : IOutboxEnlistment, IDisposable, IAsyncDisposable
{
    private readonly List<SqlServerBufferedMessage> _rows = [];
    private readonly SqlServerOutboxBatch _batch = new();
    private readonly string _table;
    private readonly int _lanes;
    private readonly RelayerInstrumentation? _instrumentation;
    private readonly SqlServerDirectDispatcher? _dispatcher;
    private SqlServerOutboxTransaction? _transaction;
    private int _count;
    private bool _disposed;

    /// <summary>Creates a scoped SQL Server enlistment using validated options.</summary>
    [RequiresUnreferencedCode("SqlClient 7.1.1 runtime trimming is unsupported; see https://github.com/dotnet/SqlClient/issues/1947.")]
    [RequiresDynamicCode("SqlClient 7.1.1 runtime Native AOT is unsupported; see https://github.com/dotnet/SqlClient/issues/1947.")]
    public SqlServerOutbox(IOptionsSnapshot<SqlServerOutboxOptions> options, RelayerInstrumentation? instrumentation = null)
        : this(options, instrumentation, null) { }

    [RequiresUnreferencedCode("SqlClient 7.1.1 runtime trimming is unsupported; see https://github.com/dotnet/SqlClient/issues/1947.")]
    [RequiresDynamicCode("SqlClient 7.1.1 runtime Native AOT is unsupported; see https://github.com/dotnet/SqlClient/issues/1947.")]
    internal SqlServerOutbox(IOptionsSnapshot<SqlServerOutboxOptions> options, RelayerInstrumentation? instrumentation,
        SqlServerDirectDispatcher? dispatcher)
    {
        ArgumentNullException.ThrowIfNull(options);
        ValidateOptions(options.Value);
        if (options.Value.DispatchStoreName is not null && dispatcher is null)
            throw new InvalidOperationException("Resolve a SQL Server outbox with DispatchStoreName through its registered service provider.");
        _dispatcher = dispatcher;
        dispatcher?.ValidateStore(options.Value.Schema, options.Value.TablePrefix);
        _lanes = options.Value.Lanes;
        _instrumentation = instrumentation;
        _table = SqlServerOutboxSchema.Table(options.Value);
    }

    /// <summary>The active borrowed transaction. Create business commands through its Connection; consumer middleware owns completion.</summary>
    public DbTransaction? CurrentTransaction => _transaction is { IsActive: true } ? _transaction : null;

    internal string Table => _table;

    /// <inheritdoc />
    public bool IsActive => _transaction is { IsActive: true };

    /// <summary>Begins a transaction on an already open, caller-owned connection.</summary>
    /// <remarks>The caller must not complete the native transaction or close the connection behind this wrapper.</remarks>
    public async ValueTask<DbTransaction> BeginTransactionAsync(SqlConnection connection,
        IsolationLevel isolationLevel = IsolationLevel.Unspecified, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(connection);
        EnsureAvailable();
        RejectAmbientTransaction();
        _dispatcher?.ValidateConnection(connection);
        var native = (SqlTransaction)await connection.BeginTransactionAsync(isolationLevel, cancellationToken).ConfigureAwait(false);
        return _transaction = new(this, native, null);
    }

    /// <summary>Opens and owns a connection using the native connection string unchanged.</summary>
    public async ValueTask<DbTransaction> BeginTransactionAsync(string connectionString,
        IsolationLevel isolationLevel = IsolationLevel.Unspecified, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(connectionString);
        EnsureAvailable();
        RejectAmbientTransaction();
        var connection = new SqlConnection(connectionString);
        try
        {
            await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
            _dispatcher?.ValidateConnection(connection);
            var native = (SqlTransaction)await connection.BeginTransactionAsync(isolationLevel, cancellationToken).ConfigureAwait(false);
            return _transaction = new(this, native, connection);
        }
        catch
        {
            await connection.DisposeAsync().ConfigureAwait(false);
            throw;
        }
    }

    private void EnsureAvailable()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (_transaction is not null)
            throw new InvalidOperationException("Complete or dispose the current SQL Server outbox transaction before beginning another.");
    }

    /// <inheritdoc />
    public ValueTask EnlistAsync(OutboxRecord record, CancellationToken cancellationToken)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        cancellationToken.ThrowIfCancellationRequested();
        if (!IsActive) throw new InvalidOperationException("Begin a SQL Server outbox transaction before sending via the outbox.");
        SqlServerBufferedMessage.Validate(record);
        if (_count == _rows.Count) _rows.Add(new());
        _rows[_count].Copy(record, _lanes);
        _count++;
        return ValueTask.CompletedTask;
    }

    internal SqlServerOutboxBatch? Prepare(SqlTransaction transaction)
    {
        if (_count == 0) return null;
        _batch.Prepare(transaction, _table, _rows, _count);
        return _batch;
    }

    internal void Committed()
    {
        _instrumentation?.AddOutboxPending(_count);
        _dispatcher?.Committed(_rows, _count);
    }

    internal void Ended(SqlServerOutboxTransaction transaction)
    {
        if (!ReferenceEquals(_transaction, transaction)) return;
        for (var i = 0; i < _count; i++) _rows[i].Clear();
        _count = 0;
        _transaction = null;
    }

    private static void RejectAmbientTransaction()
    {
        if (System.Transactions.Transaction.Current is not null)
            throw new InvalidOperationException("SQL Server outbox enlistment requires an explicit local transaction; ambient TransactionScope is unsupported.");
    }

    internal static void ValidateOptions(SqlServerOutboxOptions options)
    {
        _ = SqlServerOutboxSchema.CreateScript(options);
        if (options.DispatchStoreName is not null) ArgumentException.ThrowIfNullOrWhiteSpace(options.DispatchStoreName);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(options.MaxDispatchAttempts);
        if (options.Lanes <= 0) throw new ArgumentOutOfRangeException(nameof(options), "Lanes must be positive.");
    }

    /// <inheritdoc />
    public void Dispose()
    {
        if (_disposed) return;
        try { _transaction?.Dispose(); }
        finally { _disposed = true; _batch.Dispose(); }
    }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        if (_disposed) return;
        try
        {
            if (_transaction is not null) await _transaction.DisposeAsync().ConfigureAwait(false);
        }
        finally { _disposed = true; _batch.Dispose(); }
    }
}
