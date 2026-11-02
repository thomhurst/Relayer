using Azure.Messaging.ServiceBus;
using Azure.Messaging.ServiceBus.Administration;

namespace Relayer.AzureServiceBus;

/// <summary>Options for the AzureServiceBus transport. Native client configuration is passed through unchanged (docs/decisions.md D1).</summary>
public sealed class AzureServiceBusTransportOptions
{
    /// <summary>Connection string for a client owned and disposed by Relayer. Do not also set Client.</summary>
    public string? ConnectionString { get; set; }

    /// <summary>A caller-owned client, including clients constructed with TokenCredential. Relayer never disposes it.</summary>
    public ServiceBusClient? Client { get; set; }

    /// <summary>Native client options, passed straight to <see cref="ServiceBusClient"/>.</summary>
    public ServiceBusClientOptions ClientOptions { get; set; } = new();

    /// <summary>Caller-owned administration client, used only in Provision mode. Required for caller-owned messaging clients.</summary>
    public ServiceBusAdministrationClient? AdministrationClient { get; set; }

    /// <summary>Native queue declarations. Names must match send destinations.</summary>
    public IList<CreateQueueOptions> Queues { get; } = [];

    /// <summary>Native topic declarations. Names must match send destinations.</summary>
    public IList<CreateTopicOptions> Topics { get; } = [];

    /// <summary>Native subscriptions to provision independently of receive composition.</summary>
    public IList<CreateSubscriptionOptions> Subscriptions { get; } = [];
}
