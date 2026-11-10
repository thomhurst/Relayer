using System.Runtime.CompilerServices;
using MySqlConnector;

namespace Relayer.Outbox.MySql;

// Database time is authoritative. The send transaction retains the lease row lock through completion.
internal sealed class MySqlLaneCoordinator : IDisposable
{
    private readonly MySqlDataSource _source;
    private readonly string _member = "member:" + Guid.NewGuid().ToString("N");
    private readonly string[] _names;
    private readonly string[] _tokens;
    private readonly List<string> _live = [];
    private readonly MySqlCommand _heartbeat;
    private readonly MySqlCommand _members;
    private readonly MySqlCommand _acquire;
    private readonly MySqlCommand _release;
    internal bool[] Assigned { get; }

    internal MySqlLaneCoordinator(MySqlDataSource source, MySqlOrderedDispatchOptions options)
    {
        _source = source;
        var table = MySqlOutboxSchema.LeaseTable(options.Validate());
        _heartbeat = new($"INSERT INTO {table} (name, owner, expires_at) VALUES (@member, @member, TIMESTAMPADD(MICROSECOND, @duration, UTC_TIMESTAMP(6))) ON DUPLICATE KEY UPDATE expires_at = TIMESTAMPADD(MICROSECOND, @duration, UTC_TIMESTAMP(6))");
        _heartbeat.Parameters.AddWithValue("member", _member);
        _heartbeat.Parameters.AddWithValue("duration", options.LeaseDuration.Ticks / 10);
        _members = new($"DELETE FROM {table} WHERE name LIKE 'member:%' AND expires_at <= UTC_TIMESTAMP(6); SELECT name FROM {table} WHERE name LIKE 'member:%' AND expires_at > UTC_TIMESTAMP(6) ORDER BY name");
        // INSERT IGNORE and UPDATE take native row locks, including when another owner is still live.
        // The final SELECT verifies ownership, independently of CLIENT_FOUND_ROWS and no-op renewals.
        _acquire = new($"INSERT IGNORE INTO {table} (name, owner, expires_at) VALUES (@lane, @token, TIMESTAMPADD(MICROSECOND, @duration, UTC_TIMESTAMP(6))); UPDATE {table} SET owner = @token, expires_at = TIMESTAMPADD(MICROSECOND, @duration, UTC_TIMESTAMP(6)) WHERE name = @lane AND (owner = @token OR expires_at <= UTC_TIMESTAMP(6)); SELECT owner = @token AND expires_at > UTC_TIMESTAMP(6) FROM {table} WHERE name = @lane");
        _acquire.Parameters.AddWithValue("lane", "");
        _acquire.Parameters.AddWithValue("token", "");
        _acquire.Parameters.AddWithValue("duration", options.LeaseDuration.Ticks / 10);
        _release = new($"DELETE FROM {table} WHERE (name = @member AND owner = @member) OR owner LIKE @prefix");
        _release.Parameters.AddWithValue("member", _member);
        _release.Parameters.AddWithValue("prefix", _member + "/%");
        _names = new string[options.Lanes];
        _tokens = new string[options.Lanes];
        Assigned = new bool[options.Lanes];
        for (var lane = 0; lane < options.Lanes; lane++)
        {
            _names[lane] = "lane:" + lane.ToString(System.Globalization.CultureInfo.InvariantCulture);
            _tokens[lane] = _member + "/" + Guid.NewGuid().ToString("N");
        }
    }

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    internal async ValueTask RebalanceAsync(CancellationToken cancellationToken)
    {
        await using var connection = await _source.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        _heartbeat.Connection = connection;
        _members.Connection = connection;
        try
        {
            await _heartbeat.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            _live.Clear();
            await using var reader = await _members.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
            while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false)) _live.Add(reader.GetString(0));
            var rank = _live.IndexOf(_member);
            for (var lane = 0; lane < Assigned.Length; lane++)
                Assigned[lane] = rank >= 0 && lane % _live.Count == rank;
        }
        finally { _heartbeat.Connection = null; _members.Connection = null; }
    }

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    internal async ValueTask<bool> AcquireAsync(MySqlConnection connection, MySqlTransaction transaction,
        int lane, CancellationToken cancellationToken)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(lane);
        ArgumentOutOfRangeException.ThrowIfGreaterThanOrEqual(lane, Assigned.Length);
        _acquire.Connection = connection;
        _acquire.Transaction = transaction;
        _acquire.Parameters["lane"].Value = _names[lane];
        _acquire.Parameters["token"].Value = _tokens[lane];
        try { return Convert.ToInt32(await _acquire.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false), System.Globalization.CultureInfo.InvariantCulture) == 1; }
        finally { _acquire.Transaction = null; _acquire.Connection = null; }
    }

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    internal async ValueTask ReleaseAsync(CancellationToken cancellationToken)
    {
        await using var connection = await _source.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        _release.Connection = connection;
        try { await _release.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false); }
        finally { _release.Connection = null; }
    }

    public void Dispose()
    {
        _heartbeat.Dispose(); _members.Dispose(); _acquire.Dispose(); _release.Dispose();
    }
}
