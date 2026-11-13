using Relayer.Transports;
using System.Data;
using System.Data.Common;
using Microsoft.Extensions.Options;
using Npgsql;

namespace Relayer.Outbox.PostgreSql;

/// <summary>Scoped PostgreSQL transaction enlistment. Dispatch workers are configured separately.</summary>
/// <remarks>Use one unit at a time in a scope. Commit through the returned transaction, and use its Connection for Dapper commands.</remarks>
public sealed class PostgreSqlOutbox : IOutboxEnlistment, IDisposable, IAsyncDisposable
{
    private readonly List<BufferedOutboxMessage> _rows = [];
    private readonly PostgreSqlOutboxBatch _batch = new();
    private readonly string _insertSql;
    private readonly int _lanes;
    private readonly RelayerInstrumentation? _instrumentation;
    private readonly PostgreSqlDirectDispatcher? _dispatcher;
    private PostgreSqlOutboxTransaction? _transaction;
    private readonly OutboxTenantBinding? _tenant;
    private bool _failed;
    private int _count;
    private bool _disposed;

    /// <summary>Creates an enlistment using this scope's validated options.</summary>
    public PostgreSqlOutbox(IOptionsSnapshot<PostgreSqlOutboxOptions> options, RelayerInstrumentation? instrumentation = null)
        : this(options, [], instrumentation) { }

    internal PostgreSqlOutbox(IOptionsSnapshot<PostgreSqlOutboxOptions> options,
        IEnumerable<PostgreSqlSchedulingRegistration> workers, RelayerInstrumentation? instrumentation = null, PostgreSqlDirectDispatcher? dispatcher = null)
    {
        ArgumentNullException.ThrowIfNull(options);
        var value = options.Value;
        ValidateOptions(value);
        _tenant = value.TenantBinding;
        _tenant?.Validate();
        _lanes = value.Lanes;
        SupportsScheduling = PostgreSqlSchedulingRegistration.Supports(workers, value.Schema, value.TablePrefix);
        SupportsOrderedScheduling = PostgreSqlSchedulingRegistration.Supports(workers, value.Schema, value.TablePrefix, value.Lanes);
        _instrumentation = instrumentation;
        _dispatcher = dispatcher;
        dispatcher?.ValidateStore(value.Schema, value.TablePrefix);
        _insertSql = PostgreSqlOutboxBatch.CreateInsertSql(
            PostgreSqlOutboxSchema.QuoteIdentifier(value.Schema ?? "public") + "." +
            PostgreSqlOutboxSchema.QuoteIdentifier(value.TablePrefix + "outbox"));
    }

    /// <inheritdoc />
    public bool SupportsScheduling { get; }

    /// <inheritdoc />
    public bool SupportsOrderedScheduling { get; }

    /// <inheritdoc />
    public bool IsActive => _transaction is { IsActive: true };

    /// <summary>Begins a transaction on an already open, caller-owned connection.</summary>
    /// <remarks>The caller must not complete the native transaction or close the connection behind this wrapper.</remarks>
    public async ValueTask<DbTransaction> BeginTransactionAsync(NpgsqlConnection connection,
        IsolationLevel isolationLevel = IsolationLevel.Unspecified, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(connection);
        EnsureAvailable();
        var native = await connection.BeginTransactionAsync(isolationLevel, cancellationToken).ConfigureAwait(false);
        return _transaction = new(this, native, null);
    }

    /// <summary>Opens a connection from an application-owned data source. The transaction owns that connection only.</summary>
    public async ValueTask<DbTransaction> BeginTransactionAsync(NpgsqlDataSource dataSource,
        IsolationLevel isolationLevel = IsolationLevel.Unspecified, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(dataSource);
        EnsureAvailable();
        var connection = await dataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            var native = await connection.BeginTransactionAsync(isolationLevel, cancellationToken).ConfigureAwait(false);
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
        _tenant?.Validate();
        if (_transaction is not null)
            throw new InvalidOperationException("Complete or dispose the current PostgreSQL outbox transaction before beginning another.");
    }

    /// <inheritdoc />
    public ValueTask EnlistAsync(OutboxRecord record, CancellationToken cancellationToken)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        cancellationToken.ThrowIfCancellationRequested();
        if (!IsActive) throw new InvalidOperationException("Begin a PostgreSQL outbox transaction before sending via the outbox.");
        if (_failed) throw new InvalidOperationException("Roll back the failed outbox transaction before sending again.");
        if (_tenant is not null)
        {
            try { record = _tenant.Bind(record); }
            catch { _failed = true; throw; }
        }
        BufferedOutboxMessage.Validate(record);
        if (_count == _rows.Count) _rows.Add(new());
        _rows[_count].Copy(record, _lanes);
        _count++;
        return ValueTask.CompletedTask;
    }

    internal PostgreSqlOutboxBatch? Prepare(NpgsqlTransaction transaction)
    {
        if (_failed) throw new InvalidOperationException("This outbox unit failed. Roll back and retry the complete unit of work.");
        _tenant?.Validate();
        if (_count == 0) return null;
        _batch.Prepare(transaction, _insertSql);
        for (var i = 0; i < _count; i++) _batch.Add(_rows[i]);
        return _batch;
    }

    internal void Committed()
    {
        _instrumentation?.AddOutboxPending(_count);
        _dispatcher?.Committed(_rows, _count);
    }

    internal void Ended(PostgreSqlOutboxTransaction transaction)
    {
        if (!ReferenceEquals(_transaction, transaction)) return;
        for (var i = 0; i < _count; i++) _rows[i].Clear();
        _count = 0;
        _failed = false;
        _transaction = null;
    }

    internal static void ValidateOptions(PostgreSqlOutboxOptions options)
    {
        _ = PostgreSqlOutboxSchema.CreateScript(options);
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
