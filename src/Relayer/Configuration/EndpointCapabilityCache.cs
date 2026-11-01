using Relayer.Transports;

namespace Relayer;

/// <summary>Shares native capability declarations between startup validation and pipeline composition.</summary>
internal sealed class EndpointCapabilityCache
{
    private readonly Dictionary<(ITransport Transport, string Endpoint), TransportCapabilities> _endpoints = [];

    internal TransportCapabilities GetCapabilities(ITransport transport, string endpoint)
    {
        // Resolution can also begin through ISender before the host starts. This lock is startup-only.
        lock (_endpoints)
        {
            var key = (transport, endpoint);
            if (_endpoints.TryGetValue(key, out var capabilities)) return capabilities;
            capabilities = transport.GetCapabilities(endpoint);
            _endpoints.Add(key, capabilities);
            return capabilities;
        }
    }
}
