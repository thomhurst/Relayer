namespace Relayer;

/// <summary>The tenant for one application or single-message consumer scope.</summary>
/// <remarks>Registered by AddRelayerTenancy. Do not retain this pooled service beyond its DI scope.
/// Incoming values are untrusted routing metadata; authorize them before selecting tenant resources.</remarks>
public sealed class TenantContext : IDisposable
{
    /// <summary>The native header used for tenant propagation on every transport.</summary>
    public const string HeaderName = "relayer.tenant_id";

    private readonly TenantContextPool _owner;
    private bool _active;
    private WireString _id;

    internal TenantContext(TenantContextPool owner) => _owner = owner;

    /// <summary>The current tenant, or an empty value for an unassigned scope. Native UTF-8 is not decoded.</summary>
    /// <remarks>Values must remain valid until the scope and its sends finish. Set this only after authorizing the tenant.</remarks>
    public WireString Id
    {
        get { ObjectDisposedException.ThrowIf(!_active, this); return _id; }
        set { ObjectDisposedException.ThrowIf(!_active, this); _id = value; }
    }

    // A binding must not become valid again when this pooled object is rented by another scope.
    internal long LeaseVersion { get; private set; }

    internal void Activate()
    {
        LeaseVersion = unchecked(LeaseVersion + 1);
        _active = true;
    }

    /// <summary>Returns this service to its provider-owned pool. DI owns disposal.</summary>
    public void Dispose()
    {
        if (!_active) return;
        _id = default;
        _active = false;
        _owner.Return(this);
    }
}

internal sealed class TenantContextPool
{
    private readonly SlotPool<TenantContext> _items;

    internal TenantContextPool(int capacity)
    {
        _items = new(capacity, capacity);
        for (var i = 0; i < capacity; i++) _items.TryReturn(new(this), i);
    }

    internal TenantContext Rent()
    {
        var context = _items.TryRent() ?? new(this);
        context.Activate();
        return context;
    }

    internal void Return(TenantContext context)
    {
        _items.TryReturn(context);
    }
}
