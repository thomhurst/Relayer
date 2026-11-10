using System.Data;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using Microsoft.Data.SqlClient;

namespace Relayer.Outbox.SqlServer;

internal sealed class SqlServerOrderedLane : IDisposable
{
    private readonly SqlServerOutboxStore _reader;
    private readonly string _connectionString;
    private readonly string _read;
    private readonly string _ready;
    private readonly SqlCommand _claim;
    private readonly SqlCommand _complete;
    private readonly SqlCommand _fail;
    private readonly byte[] _id = new byte[16];
    private readonly byte[] _token = new byte[16];

    [RequiresUnreferencedCode("SqlClient 7.1.1 runtime trimming is unsupported; see https://github.com/dotnet/SqlClient/issues/1947.")]
    [RequiresDynamicCode("SqlClient 7.1.1 runtime Native AOT is unsupported; see https://github.com/dotnet/SqlClient/issues/1947.")]
    internal SqlServerOrderedLane(string connectionString, SqlServerOutboxOptions options, TimeSpan duration, int maxAttempts)
    {
        _connectionString = connectionString;
        _reader = new(connectionString, options);
        var table = SqlServerOutboxSchema.Table(options);
        var pending = $"FROM {table} AS pending WHERE lane = @lane AND status = 'pending' AND NOT EXISTS (SELECT 1 FROM {table} AS parked WHERE parked.lane = @lane AND parked.status = 'parked' AND parked.ordering_key COLLATE Latin1_General_100_BIN2 = pending.ordering_key COLLATE Latin1_General_100_BIN2 AND DATALENGTH(parked.ordering_key) = DATALENGTH(pending.ordering_key) AND parked.seq < pending.seq)";
        _read = $"SELECT TOP (@limit) id, transport, message_type, destination, body, headers, ordering_key, deliver_at, created_at, lane, attempts, last_error, tenant_id, seq {pending} ORDER BY seq";
        _ready = $"SELECT CAST(CASE WHEN EXISTS (SELECT 1 FROM (SELECT TOP (1) deliver_at, claim_expires_at {pending} ORDER BY seq) AS head WHERE (deliver_at IS NULL OR deliver_at <= SYSUTCDATETIME()) AND (claim_expires_at IS NULL OR claim_expires_at <= SYSUTCDATETIME())) THEN 1 ELSE 0 END AS bit)";
        var fenced = "id = @id AND claim_token = @token AND claim_expires_at > SYSUTCDATETIME()";
        _claim = Create($"UPDATE {table} SET claim_token = @token, claim_expires_at = DATEADD(millisecond, @duration, SYSUTCDATETIME()) WHERE id = @id AND status = 'pending' AND (deliver_at IS NULL OR deliver_at <= SYSUTCDATETIME()) AND (claim_expires_at IS NULL OR claim_expires_at <= SYSUTCDATETIME()); SELECT CAST(@@ROWCOUNT AS bit)");
        _claim.Parameters.Add("@duration", SqlDbType.Int).Value = checked((int)duration.TotalMilliseconds);
        _complete = Create($"DELETE FROM {table} WHERE {fenced}; SELECT CAST(@@ROWCOUNT AS bit)");
        _fail = Create($"UPDATE {table} SET attempts = attempts + 1, last_error = @error, status = CASE WHEN attempts + 1 >= @maxAttempts THEN 'parked' ELSE 'pending' END, claim_token = NULL, claim_expires_at = NULL WHERE {fenced}; SELECT CAST(@@ROWCOUNT AS bit)");
        _fail.Parameters.Add("@error", SqlDbType.NVarChar, -1);
        _fail.Parameters.Add("@maxAttempts", SqlDbType.Int).Value = maxAttempts;
    }

    internal async ValueTask FindReadyAsync(bool[] assigned, bool[] ready, CancellationToken cancellationToken)
    {
        Array.Clear(ready);
        using var connection = new SqlConnection(_connectionString);
        using var command = new SqlCommand(_ready, connection);
        var laneParameter = command.Parameters.Add("@lane", SqlDbType.Int);
        object boxedLane = 0;
        laneParameter.Value = boxedLane;
        await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
        for (var lane = 0; lane < assigned.Length; lane++)
        {
            if (!assigned[lane]) continue;
            Unsafe.Unbox<int>(boxedLane) = lane;
            ready[lane] = await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false) is true;
        }
    }

    [RuntimeAsyncMethodGeneration(false)]
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    internal async ValueTask<int> ReadAsync(SqlConnection connection, SqlTransaction transaction, int lane, Memory<OutboxRecord> rows, CancellationToken cancellationToken)
    {
        using var command = new SqlCommand(_read, connection, transaction);
        command.Parameters.Add("@lane", SqlDbType.Int).Value = lane;
        command.Parameters.Add("@limit", SqlDbType.Int).Value = rows.Length;
        return await _reader.ReadRowsAsync(command, rows, cancellationToken, includeOrderedSequence: true).ConfigureAwait(false);
    }

    internal ValueTask<bool> ClaimAsync(SqlConnection connection, SqlTransaction transaction, OutboxClaim claim, CancellationToken token) => MutateAsync(connection, transaction, _claim, claim, null, token);
    internal ValueTask<bool> CompleteAsync(SqlConnection connection, SqlTransaction transaction, OutboxClaim claim, CancellationToken token) => MutateAsync(connection, transaction, _complete, claim, null, token);
    internal ValueTask<bool> FailAsync(SqlConnection connection, SqlTransaction transaction, OutboxClaim claim, string error, CancellationToken token) => MutateAsync(connection, transaction, _fail, claim, error, token);

    [RuntimeAsyncMethodGeneration(false)]
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    private async ValueTask<bool> MutateAsync(SqlConnection connection, SqlTransaction transaction, SqlCommand command, OutboxClaim claim, string? error, CancellationToken cancellationToken)
    {
        claim.MessageId.TryWriteBytes(_id, bigEndian: true, out _);
        claim.Token.TryWriteBytes(_token, bigEndian: true, out _);
        command.Connection = connection;
        command.Transaction = transaction;
        if (error is not null) command.Parameters["@error"].Value = error;
        try { return await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false) is true; }
        finally
        {
            command.Transaction = null;
            command.Connection = null;
            if (error is not null) command.Parameters["@error"].Value = DBNull.Value;
        }
    }

    private SqlCommand Create(string sql)
    {
        var command = new SqlCommand(sql);
        command.Parameters.Add("@id", SqlDbType.Binary, 16).Value = _id;
        command.Parameters.Add("@token", SqlDbType.Binary, 16).Value = _token;
        return command;
    }

    public void Dispose() { _claim.Dispose(); _complete.Dispose(); _fail.Dispose(); }
}
