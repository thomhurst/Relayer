using Relayer.Transports;
using System.Globalization;
using MySqlConnector;

namespace Relayer.Outbox.MySql;

// MySqlParameter has only an object-valued API. Native text-protocol support for
// ReadOnlyMemory<char> lets us reuse boxed memory views instead of boxing each number/date.
// UUIDs are explicit network-order bytes, independent of the application's GuidFormat.
internal sealed class MySqlStoreParameters
{
    private readonly char[] _now = new char[26];
    private readonly char[] _expires = new char[26];
    private readonly char[] _lane = new char[10];
    private readonly byte[] _id = new byte[16];
    private readonly byte[] _token = new byte[16];
    private readonly byte[] _after = new byte[16];
    private readonly object _laneView;
    private int _limit;
    private readonly MySqlParameter _limitParameter;
    private readonly MySqlParameter _laneParameter;
    private readonly MySqlParameter _afterParameter;
    internal MySqlCommand Command { get; }

    internal MySqlStoreParameters(string sql)
    {
        Command = new() { CommandText = sql };
        Add("now", MySqlDbType.VarChar, (ReadOnlyMemory<char>)_now);
        Add("expires", MySqlDbType.VarChar, (ReadOnlyMemory<char>)_expires);
        _laneView = (ReadOnlyMemory<char>)_lane;
        _laneParameter = Add("lane", MySqlDbType.VarChar, DBNull.Value);
        _limitParameter = Add("limit", MySqlDbType.Int32, 0);
        Add("id", MySqlDbType.Binary, _id);
        Add("token", MySqlDbType.Binary, _token);
        _afterParameter = Add("after", MySqlDbType.Binary, _after);
        Add("error", MySqlDbType.LongText, DBNull.Value);
    }

    private MySqlParameter Add(string name, MySqlDbType type, object value)
    {
        var parameter = new MySqlParameter(name, type) { Value = value };
        Command.Parameters.Add(parameter);
        return parameter;
    }

    internal void Now(DateTimeOffset value) => Timestamp(value, _now);
    internal void Expires(DateTimeOffset value) => Timestamp(value, _expires);
    internal void Claim(OutboxClaim value)
    {
        value.MessageId.TryWriteBytes(_id, bigEndian: true, out _);
        value.Token.TryWriteBytes(_token, bigEndian: true, out _);
    }

    internal void Read(int? lane, Guid? after, int limit)
    {
        if (lane.HasValue)
        {
            lane.Value.TryFormat(_lane, out _, "D10", CultureInfo.InvariantCulture);
            _laneParameter.Value = _laneView;
        }
        else _laneParameter.Value = DBNull.Value;
        _afterParameter.Value = after.HasValue ? _after : DBNull.Value;
        if (after.HasValue) after.Value.TryWriteBytes(_after, bigEndian: true, out _);
        if (_limit != limit) { _limitParameter.Value = limit; _limit = limit; }
    }

    private static void Timestamp(DateTimeOffset value, Span<char> destination) =>
        value.UtcDateTime.TryFormat(destination, out _, "yyyy-MM-dd HH:mm:ss.ffffff", CultureInfo.InvariantCulture);
}
