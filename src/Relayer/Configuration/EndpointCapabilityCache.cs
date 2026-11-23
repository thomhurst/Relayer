using Relayer.Transports;

namespace Relayer;

/// <summary>Shares native capability declarations between startup validation and pipeline composition.</summary>
internal sealed class EndpointCapabilityCache
{
    private readonly Dictionary<ITransport, Dictionary<string, TransportCapabilities>> _transports = [];

    internal TransportCapabilities GetCapabilities(ITransport transport, string endpoint)
    {
        // Resolution can also begin through ISender before the host starts. This lock is startup-only.
        lock (_transports)
        {
            if (!_transports.TryGetValue(transport, out var endpoints))
            {
                endpoints = new(StringComparer.Ordinal);
                _transports.Add(transport, endpoints);
            }

            if (endpoints.TryGetValue(endpoint, out var capabilities))
            {
                return capabilities;
            }

            capabilities = transport.GetCapabilities(endpoint);
            endpoints.Add(endpoint, capabilities);
            return capabilities;
        }
    }

    internal void Invalidate(ITransport transport)
    {
        lock (_transports)
        {
            _transports.Remove(transport);
        }
    }
}
