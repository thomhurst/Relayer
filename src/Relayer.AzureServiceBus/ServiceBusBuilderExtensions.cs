using Azure.Messaging.ServiceBus;
using Azure.Messaging.ServiceBus.Administration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Relayer.Transports;

namespace Relayer.AzureServiceBus;

/// <summary>Registers named Service Bus send transports with native topology declarations.</summary>
public static class ServiceBusBuilderExtensions
{
    /// <summary>Declares native queue/topic sending. Complete receive composition is not implemented yet.</summary>
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
        builder.Services.AddSingleton<ITransport>(provider =>
        {
            var configuration = provider.GetRequiredService<RelayerConfiguration>();
            var definition = configuration.Transports.Single(item => item.Name == name);
            if (definition.Consumers.Count != 0)
                throw new NotSupportedException($"Service Bus transport '{name}': consumer execution is not implemented yet. Use the native receive primitives or wait for complete receiver composition.");
            var options = provider.GetRequiredService<IOptionsMonitor<AzureServiceBusTransportOptions>>().Get(name);
            if ((options.Client is null) == string.IsNullOrWhiteSpace(options.ConnectionString))
                throw new InvalidOperationException($"Service Bus transport '{name}': configure exactly one of Client or ConnectionString.");
            ArgumentNullException.ThrowIfNull(options.ClientOptions);
            ServiceBusSendTransport.ValidateDeclarations(name, options, definition.Routes.Select(route => route.Destination));
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
                provider.GetRequiredService<MessageTracing>(), logging);
        });
        builder.Services.AddSingleton<IHostedService>(provider => new ServiceBusSendHostedService(provider, name));
        return builder;
    }

    /// <summary>Customises the native outgoing Service Bus message immediately before sending.</summary>
    public static SendOptions ServiceBus(this SendOptions options, Action<ServiceBusMessage> configure) =>
        options.ConfigureNative(configure);
}

/// <summary>Service Bus send route options. Declare native entities on the named transport options.</summary>
public sealed class ServiceBusRouteOptions;

/// <summary>Reserved for complete native receiver composition.</summary>
public sealed class ServiceBusConsumerOptions;

internal sealed class ServiceBusSendHostedService(IServiceProvider provider, string name) : IHostedService
{
    private ITransport Transport => provider.GetServices<ITransport>().Single(transport => transport.Name == name);
    public Task StartAsync(CancellationToken cancellationToken) => Transport.StartAsync(cancellationToken);
    public Task StopAsync(CancellationToken cancellationToken) => Transport.StopAsync(cancellationToken);
}
