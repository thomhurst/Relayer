using Relayer.Transports;

namespace Relayer;

/// <summary>Resolves endpoint declarations only during startup, before native receive services start.</summary>
internal sealed class EndpointCapabilityValidator
{
    private readonly Dictionary<string, ITransport?> _transports = new(StringComparer.Ordinal);
    private readonly Dictionary<(string Transport, string Endpoint), TransportCapabilities?> _endpoints = [];
    private readonly List<string> _failures;
    private readonly EndpointCapabilityCache _cache;
    private readonly bool _hasRuntimeTransports;

    internal EndpointCapabilityValidator(IEnumerable<ITransport> transports, List<string> failures, EndpointCapabilityCache cache)
    {
        _failures = failures;
        _cache = cache;
        foreach (var transport in transports)
        {
            _hasRuntimeTransports = true;
            if (string.IsNullOrWhiteSpace(transport.Name))
            {
                failures.Add("An ITransport adapter name must not be blank.");
                continue;
            }
            if (!_transports.TryAdd(transport.Name, transport))
            {
                failures.Add($"ITransport adapter '{transport.Name}' is registered more than once. Register exactly one adapter per transport name.");
                _transports[transport.Name] = null;
            }
        }
    }

    internal void Validate(TransportDefinition definition)
    {
        // Configuration-only applications can inspect declarations without installing native adapters.
        if (!_hasRuntimeTransports || string.IsNullOrWhiteSpace(definition.Name)) return;
        if (!_transports.TryGetValue(definition.Name, out var transport))
        {
            _failures.Add($"Transport '{definition.Name}': register a named ITransport adapter before starting runtime transports.");
            return;
        }
        if (transport is null) return; // Duplicate names have already been reported; neither adapter is authoritative.

        foreach (var route in definition.Routes)
        {
            var capabilities = GetCapabilities(transport, route.Destination);
            if (route.HasOrderingKey && capabilities is { OrderedByKey: false })
            {
                _failures.Add($"Route for '{route.MessageType}' to '{route.Destination}' on transport '{definition.Name}': " +
                    "OrderedByKey is unavailable. Use an endpoint with native ordering support or remove the ordering key; ordering has no fallback.");
            }
        }
        foreach (var consumer in definition.Consumers) GetCapabilities(transport, consumer.Source);
    }

    private TransportCapabilities? GetCapabilities(ITransport transport, string endpoint)
    {
        if (string.IsNullOrWhiteSpace(endpoint)) return null; // Declaration validation reports blank endpoints.
        var key = (transport.Name, endpoint);
        if (_endpoints.TryGetValue(key, out var cached)) return cached;

        var location = $"Endpoint '{endpoint}' on transport '{transport.Name}'";
        TransportCapabilities? capabilities = null;
        try
        {
            capabilities = _cache.GetCapabilities(transport, endpoint);
            if (capabilities is null)
                _failures.Add($"{location}: GetCapabilities must return a TransportCapabilities declaration.");
            else if (!ValidateValues(capabilities, location))
                capabilities = null;
        }
        catch (Exception error) when (error is ArgumentException or InvalidOperationException or NotSupportedException)
        {
            _failures.Add($"{location}: cannot resolve TransportCapabilities. {error.Message}");
        }

        _endpoints.Add(key, capabilities);
        return capabilities;
    }

    private bool ValidateValues(TransportCapabilities capabilities, string location)
    {
        var previousCount = _failures.Count;
        if (capabilities.MaxDeliveryDelay < TimeSpan.Zero)
            _failures.Add($"{location}: MaxDeliveryDelay must be null or non-negative.");
        if (capabilities.MaxRedeliveryDelay < TimeSpan.Zero)
            _failures.Add($"{location}: MaxRedeliveryDelay must be null or non-negative (zero permits immediate redelivery only).");
        if (capabilities.MaxBatchSize < 1)
            _failures.Add($"{location}: MaxBatchSize must be at least 1.");
        if (capabilities.MaxMessageBytes <= 0)
            _failures.Add($"{location}: MaxMessageBytes must be null or positive.");
        if (!Enum.IsDefined(capabilities.ConsumptionModel))
            _failures.Add($"{location}: ConsumptionModel must be a defined value.");
        return _failures.Count == previousCount;
    }
}
