using Azure.Messaging.ServiceBus;

namespace Relayer.AzureServiceBus;

/// <summary>Options for the AzureServiceBus transport. Native client configuration is passed through unchanged (docs/decisions.md D1).</summary>
public sealed class AzureServiceBusTransportOptions
{
    /// <summary>Native client options, passed straight to <see cref="ServiceBusClient"/>.</summary>
    public ServiceBusClientOptions ClientOptions { get; set; } = new();
}
