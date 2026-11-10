using Azure.Messaging.ServiceBus;
using Azure.Messaging.ServiceBus.Administration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Relayer.Transports;

namespace Relayer.AzureServiceBus;

/// <summary>Registers named Service Bus transports with native topology declarations and processors.</summary>
public static class ServiceBusBuilderExtensions
{
    /// <summary>Declares native queue/topic sending and queue or subscription processors.</summary>
    public static IRelayerBuilder ServiceBus(this IRelayerBuilder builder, string name,
        Action<TransportBuilder<AzureServiceBusTransportOptions, ServiceBusRouteOptions, ServiceBusConsumerOptions>> configure)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(configure);
        builder.AddTransport<AzureServiceBusTransportOptions, ServiceBusRouteOptions, ServiceBusConsumerOptions>(name, transport =>
        {
            transport.SendDefaults = new() { HeaderNames = new("cloudEvents:") };
            configure(transport);
        });
        builder.Services.AddKeyedSingleton<ServiceBusSendTransport>(name, (provider, _) =>
        {
            var configuration = provider.GetRequiredService<RelayerConfiguration>();
            var definition = configuration.Transports.Single(item => item.Name == name);
            var options = provider.GetRequiredService<IOptionsMonitor<AzureServiceBusTransportOptions>>().Get(name);
            if ((options.Client is null) == string.IsNullOrWhiteSpace(options.ConnectionString))
                throw new InvalidOperationException($"Service Bus transport '{name}': configure exactly one of Client or ConnectionString.");
            ArgumentNullException.ThrowIfNull(options.ClientOptions);
            ServiceBusSendTransport.ValidateDeclarations(name, options, definition.Routes.Select(route => route.Destination));
            foreach (var consumer in definition.Consumers)
                ServiceBusConsumer.ValidateDeclaration(name, options, (ConsumerDefinition<ServiceBusConsumerOptions>)consumer);
            var mode = definition.TopologyMode ?? configuration.TopologyMode;
            var admin = options.AdministrationClient;
            if (mode == TopologyMode.Provision && admin is null)
                admin = options.ConnectionString is { } connection ? new ServiceBusAdministrationClient(connection)
                    : throw new InvalidOperationException($"Service Bus transport '{name}': Provision requires AdministrationClient when using a caller-owned Client.");
            // Validate never constructs or calls an administration client.
            var client = options.Client ?? new ServiceBusClient(options.ConnectionString!, options.ClientOptions);
            var logging = provider.GetService<ILoggerFactory>() ?? NullLoggerFactory.Instance;
            return new ServiceBusSendTransport(name, client, options.Client is null, options,
                definition.Routes.Select(route => route.Destination), mode, admin,
                provider.GetRequiredService<MessageTracing>(), logging, definition.Consumers);
        });
        builder.Services.AddSingleton<ITransport>(provider => provider.GetRequiredKeyedService<ServiceBusSendTransport>(name));
        builder.Services.AddKeyedSingleton<IDeadLetterManager>(name, (provider, _) =>
        {
            var transport = provider.GetRequiredKeyedService<ServiceBusSendTransport>(name);
            var definition = provider.GetRequiredService<RelayerConfiguration>().Transports.Single(item => item.Name == name);
            return new ServiceBusDeadLetterManager(transport.Client, definition.Consumers);
        });
        builder.Services.AddSingleton<IHostedService>(provider => new ServiceBusHostedService(provider, name));
        return builder;
    }

    /// <summary>Customises the native outgoing Service Bus message immediately before sending.</summary>
    public static SendOptions ServiceBus(this SendOptions options, Action<ServiceBusMessage> configure) =>
        options.ConfigureNative(configure);
}

/// <summary>Service Bus send route options. Declare native entities on the named transport options.</summary>
public sealed class ServiceBusRouteOptions;

/// <summary>Native processor configuration and the common durable failure policy.</summary>
public sealed class ServiceBusConsumerOptions
{
    /// <summary>The subscription when Source names a topic; null means Source names a queue.</summary>
    public string? Subscription { get; set; }

    /// <summary>Native non-session options, passed unchanged. Auto-complete and ReceiveAndDelete are unsupported.</summary>
    public ServiceBusProcessorOptions ProcessorOptions { get; set; } = new() { AutoCompleteMessages = false };

    /// <summary>Selects a native session processor when non-null. Disable AutoCompleteMessages and use PeekLock.</summary>
    public ServiceBusSessionProcessorOptions? SessionProcessorOptions { get; set; }

    /// <summary>In-process retries, immediate native redelivery and native dead-letter handling.</summary>
    public FailureOptions Failure { get; set; } = new();

    /// <summary>Optional failure-rate circuit. Stops native processing while open and admits one recovery probe.</summary>
    public ConsumerCircuitOptions? CircuitBreaker { get; set; }
}
