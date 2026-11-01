using System.Runtime.CompilerServices;
using Npgsql;

namespace Relayer.Outbox.PostgreSql;

// Membership uses the existing lease table. The database clock is authoritative on every instance.
internal sealed class PostgreSqlLaneCoordinator
{
    private readonly NpgsqlDataSource _source;
    private readonly TimeSpan _duration;
    private readonly string _member = "member:" + Guid.NewGuid().ToString("N");
    private readonly string _heartbeat;
    private readonly string _members;
    private readonly string _acquire;
    private readonly string _release;
    private readonly string[] _names;
    private readonly string[] _tokens;
    private readonly List<string> _live = [];
    internal bool[] Assigned { get; }

    internal PostgreSqlLaneCoordinator(NpgsqlDataSource source, PostgreSqlOrderedDispatchOptions options)
    {
        _source = source;
        _duration = options.LeaseDuration;
        var table = PostgreSqlOutboxSchema.QuoteIdentifier(options.Schema ?? "public") + "." +
            PostgreSqlOutboxSchema.QuoteIdentifier(options.TablePrefix + "leases");
        _heartbeat = $"INSERT INTO {table} (name, owner, expires_at) VALUES (@member, @member, clock_timestamp() + @duration) ON CONFLICT (name) DO UPDATE SET expires_at = EXCLUDED.expires_at";
        _members = $"WITH expired AS (DELETE FROM {table} WHERE name IN (SELECT name FROM {table} WHERE name LIKE 'member:%' AND expires_at <= clock_timestamp() FOR UPDATE SKIP LOCKED)) SELECT name FROM {table} WHERE name LIKE 'member:%' AND expires_at > clock_timestamp() ORDER BY name COLLATE \"C\"";
        // A send transaction holds this row lock until all borrowed native sends finish. A rival never steals a live owner.
        _acquire = $"INSERT INTO {table} (name, owner, expires_at) VALUES (@lane, @token, clock_timestamp() + @duration) ON CONFLICT (name) DO UPDATE SET owner = EXCLUDED.owner, expires_at = clock_timestamp() + @duration WHERE {table}.owner = @token OR {table}.expires_at <= clock_timestamp() RETURNING expires_at > clock_timestamp()";
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
        await HeartbeatAsync(cancellationToken).ConfigureAwait(false);
        _live.Clear();
        await using (var command = _source.CreateCommand(_members))
        await using (var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false))
            while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false)) _live.Add(reader.GetString(0));
        var rank = _live.IndexOf(_member);
        for (var lane = 0; lane < Assigned.Length; lane++)
            Assigned[lane] = rank >= 0 && lane % _live.Count == rank;
        // Stop renewing lanes outside this member's fair share. They transfer after their current expiry.
        // No forced revocation can interrupt an in-flight native send.
    }

    internal async ValueTask HeartbeatAsync(CancellationToken cancellationToken)
    {
        await using var command = _source.CreateCommand(_heartbeat);
        command.Parameters.AddWithValue("member", _member);
        command.Parameters.AddWithValue("duration", _duration);
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    internal async ValueTask<bool> AcquireAsync(NpgsqlConnection connection, NpgsqlTransaction transaction,
        int lane, CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand(_acquire, connection, transaction);
        command.Parameters.AddWithValue("lane", _names[lane]);
        command.Parameters.AddWithValue("token", _tokens[lane]);
        command.Parameters.AddWithValue("duration", _duration);
        return await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false) is true;
    }

    internal async ValueTask ReleaseAsync(CancellationToken cancellationToken)
    {
        await using var command = _source.CreateCommand(_release);
        command.Parameters.AddWithValue("member", _member);
        command.Parameters.AddWithValue("prefix", _member + "/%");
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }
}
