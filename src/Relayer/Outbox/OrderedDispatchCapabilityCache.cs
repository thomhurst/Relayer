using System.Runtime.CompilerServices;
using Relayer.Transports;

namespace Relayer;

// Owned exclusively by the ordered dispatch worker's sequential lane loop.
internal sealed class OrderedDispatchCapabilityCache
{
    internal const int Capacity = 256;
    private readonly Dictionary<(ITransport Transport, string Destination), TransportCapabilities> _entries = new(Capacity);
    private readonly (ITransport Transport, string Destination)[] _insertionOrder = new (ITransport, string)[Capacity];
    private int _next;

    internal int Count => _entries.Count;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal TransportCapabilities Get(ITransport transport, string destination)
    {
        var key = (transport, destination);
        if (_entries.TryGetValue(key, out var capabilities)) return capabilities;
        return Add(transport, destination, key);
    }

    private TransportCapabilities Add(ITransport transport, string destination, (ITransport Transport, string Destination) key)
    {
        var capabilities = transport.GetCapabilities(destination);
        if (!capabilities.OrderedByKey)
            throw new InvalidOperationException($"Ordered outbox destination '{destination}' on '{transport.Name}' does not support native ordering by key.");
        // Resolve and validate before eviction so failed endpoints cannot displace valid entries.
        // FIFO keeps hits read-only, matching the unordered direct dispatch retention policy.
        if (_entries.Count == Capacity) _entries.Remove(_insertionOrder[_next]);
        _entries.Add(key, capabilities);
        _insertionOrder[_next] = key;
        if (++_next == Capacity) _next = 0;
        return capabilities;
    }
}
