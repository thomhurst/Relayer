using System.Runtime.CompilerServices;
using Relayer.Transports;

namespace Relayer.Outbox;

// Owned exclusively by the direct dispatch worker's single reader.
internal sealed class DirectDispatchCapabilityCache
{
    internal const int Capacity = 256;
    private readonly Dictionary<(string Transport, string Destination), TransportCapabilities> _entries = new(Capacity);
    private readonly (string Transport, string Destination)[] _insertionOrder = new (string, string)[Capacity];
    private int _next;

    internal int Count => _entries.Count;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal TransportCapabilities Get(ITransport transport, string destination)
    {
        var key = (transport.Name, destination);
        if (_entries.TryGetValue(key, out var capabilities)) return capabilities;
        return Add(transport, destination, key);
    }

    private TransportCapabilities Add(ITransport transport, string destination, (string Transport, string Destination) key)
    {
        var capabilities = transport.GetCapabilities(destination);
        if (capabilities.MaxBatchSize <= 0)
            throw new InvalidOperationException("Outbox dispatch requires a positive native MaxBatchSize.");
        // Validate before evicting: a failing endpoint must not displace a successful lookup.
        // FIFO keeps hits read-only and avoids per-hit recency maintenance or allocations.
        if (_entries.Count == Capacity) _entries.Remove(_insertionOrder[_next]);
        _entries.Add(key, capabilities);
        _insertionOrder[_next] = key;
        if (++_next == Capacity) _next = 0;
        return capabilities;
    }
}
