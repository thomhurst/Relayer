using Relayer.Transports;

namespace Relayer.AzureServiceBus;

/// <summary>Describes the AzureServiceBus transport.</summary>
public static class AzureServiceBusTransport
{
    private static readonly TransportCapabilities Defaults = new()
    {
        MaxRedeliveryDelay = TimeSpan.Zero,
        DeadLetter = true,
    };

    /// <summary>Returns conservative endpoint capabilities for the transport stub (docs/decisions.md D24).</summary>
    /// <remarks>
    /// Abandon supports immediate redelivery and native dead-lettering retains error metadata.
    /// The native adapter must resolve scheduling limits, sessions, batching, transactions, and message size
    /// from the endpoint and namespace before advertising them. The stub does not infer sessions from a name.
    /// </remarks>
    public static TransportCapabilities GetCapabilities(string endpoint)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(endpoint);
        return Defaults;
    }
}
