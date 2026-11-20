namespace Relayer.Transports;

// One store's last observed dead and parked row counts. Maintenance refreshes them; the
// observable gauges read the cached values and never query the database.
internal sealed class OutboxDepth : IDisposable
{
    private readonly RelayerInstrumentation _owner;
    private long _dead = -1, _parked = -1;

    internal OutboxDepth(RelayerInstrumentation owner, string system, string store)
    {
        _owner = owner;
        Tags = [new("db.system.name", system), new("relayer.outbox.store", store)];
    }

    internal KeyValuePair<string, object?>[] Tags
    {
        get;
    }

    internal long Dead => Volatile.Read(ref _dead);

    internal long Parked => Volatile.Read(ref _parked);

    internal bool Enabled => _owner.OutboxDepthEnabled;

    internal void Set(long dead, long parked)
    {
        Volatile.Write(ref _dead, dead);
        Volatile.Write(ref _parked, parked);
    }

    public void Dispose()
    {
        _owner.Untrack(this);
    }
}
