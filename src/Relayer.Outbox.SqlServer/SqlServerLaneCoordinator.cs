using System.Data;
using System.Runtime.CompilerServices;
using Microsoft.Data.SqlClient;

namespace Relayer.Outbox.SqlServer;

// Membership is scoped to the explicitly deployed lease table. The server clock is authoritative.
internal sealed class SqlServerLaneCoordinator
{
    private readonly string _connectionString;
    private readonly int _duration;
    private readonly string _member = "member:" + Guid.NewGuid().ToString("N");
    private readonly string _heartbeat;
    private readonly string _members;
    private readonly string _acquire;
    private readonly string _release;
    private readonly string[] _names;
    private readonly string[] _tokens;
    private readonly List<string> _live = [];
    internal bool[] Assigned { get; }

    internal SqlServerLaneCoordinator(string connectionString, SqlServerOrderedDispatchOptions options)
    {
        _connectionString = connectionString;
        _duration = checked((int)options.LeaseDuration.TotalMilliseconds);
        var table = SqlServerOutboxSchema.OrderedLeaseTable(options.Validate());
        _heartbeat = $"UPDATE {table} SET expires_at = DATEADD(millisecond, @duration, SYSUTCDATETIME()) WHERE name = @member; IF @@ROWCOUNT = 0 INSERT INTO {table} (name, owner, expires_at) VALUES (@member, @member, DATEADD(millisecond, @duration, SYSUTCDATETIME()));";
        _members = $"DELETE FROM {table} WITH (READPAST, READCOMMITTEDLOCK) WHERE name LIKE 'member:%' AND expires_at <= SYSUTCDATETIME(); SELECT name FROM {table} WHERE name LIKE 'member:%' AND expires_at > SYSUTCDATETIME() ORDER BY name COLLATE Latin1_General_100_BIN2;";
        // HOLDLOCK also protects an absent key. A rival waits for the native send transaction to end;
        // expiry never revokes a row lock from a still-running native send.
        _acquire = $"""
            IF NOT EXISTS (SELECT 1 FROM {table} WITH (UPDLOCK, HOLDLOCK) WHERE name = @lane)
                INSERT INTO {table} (name, owner, expires_at) VALUES (@lane, @token, DATEADD(millisecond, @duration, SYSUTCDATETIME()));
            ELSE
                UPDATE {table} SET owner = @token, expires_at = DATEADD(millisecond, @duration, SYSUTCDATETIME())
                WHERE name = @lane AND (owner = @token OR expires_at <= SYSUTCDATETIME());
            SELECT CAST(CASE WHEN EXISTS (SELECT 1 FROM {table} WHERE name = @lane AND owner = @token AND expires_at > SYSUTCDATETIME()) THEN 1 ELSE 0 END AS bit);
            """;
        _release = $"DELETE FROM {table} WHERE (name = @member AND owner = @member) OR owner LIKE @prefix";
        _names = new string[options.Lanes];
        _tokens = new string[options.Lanes];
        Assigned = new bool[options.Lanes];
        for (var i = 0; i < _names.Length; i++)
        {
            _names[i] = "lane:" + i.ToString(System.Globalization.CultureInfo.InvariantCulture);
            _tokens[i] = _member + "/" + Guid.NewGuid().ToString("N");
        }
    }

    internal async ValueTask RebalanceAsync(CancellationToken cancellationToken)
    {
        using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
        using (var heartbeat = new SqlCommand(_heartbeat, connection))
        {
            heartbeat.Parameters.Add("@member", SqlDbType.VarChar, 100).Value = _member;
            heartbeat.Parameters.Add("@duration", SqlDbType.Int).Value = _duration;
            await heartbeat.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }
        _live.Clear();
        using (var command = new SqlCommand(_members, connection))
        await using (var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false))
            while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false)) _live.Add(reader.GetString(0));
        var rank = _live.IndexOf(_member);
        for (var lane = 0; lane < Assigned.Length; lane++)
            Assigned[lane] = rank >= 0 && lane % _live.Count == rank;
    }

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    internal async ValueTask<bool> AcquireAsync(SqlConnection connection, SqlTransaction transaction, int lane, CancellationToken cancellationToken)
    {
        using var command = new SqlCommand(_acquire, connection, transaction);
        command.Parameters.Add("@lane", SqlDbType.VarChar, 100).Value = _names[lane];
        command.Parameters.Add("@token", SqlDbType.VarChar, 100).Value = _tokens[lane];
        command.Parameters.Add("@duration", SqlDbType.Int).Value = _duration;
        return await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false) is true;
    }

    internal async ValueTask ReleaseAsync(CancellationToken cancellationToken)
    {
        using var connection = new SqlConnection(_connectionString);
        using var command = new SqlCommand(_release, connection);
        command.Parameters.Add("@member", SqlDbType.VarChar, 100).Value = _member;
        command.Parameters.Add("@prefix", SqlDbType.VarChar, 100).Value = _member + "/%";
        await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }
}
