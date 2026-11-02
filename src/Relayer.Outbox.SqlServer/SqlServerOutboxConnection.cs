using System.Data;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;
using Microsoft.Data.SqlClient;

namespace Relayer.Outbox.SqlServer;

// SqlCommand accepts only SqlTransaction. This view keeps ordinary DbConnection/Dapper calls on the native transaction.
internal sealed class SqlServerOutboxConnection(SqlServerOutboxTransaction transaction, SqlConnection native) : DbConnection
{
    [AllowNull]
    public override string ConnectionString
    {
        get => native.ConnectionString;
        set => throw LifecycleError();
    }
    public override string Database => native.Database;
    public override string DataSource => native.DataSource;
    public override string ServerVersion => native.ServerVersion;
    public override int ConnectionTimeout => native.ConnectionTimeout;
    public override ConnectionState State => transaction.IsActive ? native.State : ConnectionState.Closed;
    public override void ChangeDatabase(string databaseName) => throw LifecycleError();
    public override void Close() => throw LifecycleError();
    public override void Open() => _ = transaction.Unwrap(this);
    public override Task OpenAsync(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        Open();
        return Task.CompletedTask;
    }
    protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel) => throw LifecycleError();
    protected override DbCommand CreateDbCommand() => new Command(this, transaction, native.CreateCommand());
    // This is a borrowed view. Only the transaction owns connection disposal.
    protected override void Dispose(bool disposing) { }
    public override ValueTask DisposeAsync() => ValueTask.CompletedTask;
    private static InvalidOperationException LifecycleError() => new("Manage the SQL Server connection through the outbox transaction. Use BeginTransactionAsync, Commit, Rollback and transaction disposal.");

    private sealed class Command : DbCommand
    {
        private readonly SqlServerOutboxConnection _connection;
        private readonly SqlServerOutboxTransaction _transaction;
        private readonly SqlCommand _native;

        internal Command(SqlServerOutboxConnection connection, SqlServerOutboxTransaction transaction, SqlCommand native)
        {
            _connection = connection;
            _transaction = transaction;
            _native = native;
            _native.Transaction = transaction.Unwrap(connection);
        }

        [AllowNull]
        public override string CommandText { get => _native.CommandText; set => _native.CommandText = value!; }
        public override int CommandTimeout { get => _native.CommandTimeout; set => _native.CommandTimeout = value; }
        public override CommandType CommandType { get => _native.CommandType; set => _native.CommandType = value; }
        public override bool DesignTimeVisible { get => _native.DesignTimeVisible; set => _native.DesignTimeVisible = value; }
        public override UpdateRowSource UpdatedRowSource { get => _native.UpdatedRowSource; set => _native.UpdatedRowSource = value; }
        protected override DbConnection? DbConnection
        {
            // Preserve native provider detection for Dapper.
            get => _native.Connection;
            set
            {
                if (!ReferenceEquals(value, _connection) && !ReferenceEquals(value, _native.Connection)) throw LifecycleError();
            }
        }
        protected override DbTransaction? DbTransaction
        {
            get => _transaction;
            set
            {
                if (value is not null && !ReferenceEquals(value, _transaction))
                    throw new InvalidOperationException("A command must use its own SQL Server outbox transaction.");
                _native.Transaction = _transaction.Unwrap(_connection);
            }
        }
        protected override DbParameterCollection DbParameterCollection => _native.Parameters;
        protected override DbParameter CreateDbParameter() => _native.CreateParameter();
        public override void Cancel() => _native.Cancel();
        public override void Prepare() { Validate(); _native.Prepare(); }
        public override Task PrepareAsync(CancellationToken cancellationToken = default) { Validate(); return _native.PrepareAsync(cancellationToken); }
        public override int ExecuteNonQuery() { Validate(); return _native.ExecuteNonQuery(); }
        public override object? ExecuteScalar() { Validate(); return _native.ExecuteScalar(); }
        public override Task<int> ExecuteNonQueryAsync(CancellationToken cancellationToken) { Validate(); return _native.ExecuteNonQueryAsync(cancellationToken); }
        public override Task<object?> ExecuteScalarAsync(CancellationToken cancellationToken) { Validate(); return _native.ExecuteScalarAsync(cancellationToken); }
        protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior)
        {
            Validate(behavior);
            return _native.ExecuteReader(behavior);
        }
        protected override async Task<DbDataReader> ExecuteDbDataReaderAsync(CommandBehavior behavior, CancellationToken cancellationToken)
        {
            Validate(behavior);
            return await _native.ExecuteReaderAsync(behavior, cancellationToken).ConfigureAwait(false);
        }
        private void Validate(CommandBehavior behavior = CommandBehavior.Default)
        {
            if ((behavior & CommandBehavior.CloseConnection) != 0)
                throw new NotSupportedException("CommandBehavior.CloseConnection cannot close an active outbox transaction. Dispose the transaction instead.");
            _ = _transaction.Unwrap(_connection);
        }
        protected override void Dispose(bool disposing)
        {
            if (disposing) _native.Dispose();
            base.Dispose(disposing);
        }
        public override ValueTask DisposeAsync() => _native.DisposeAsync();
    }
}
