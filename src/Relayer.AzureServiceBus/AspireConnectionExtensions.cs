using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace Relayer.AzureServiceBus;

/// <summary>Reads native transport connections supplied by Aspire's WithReference.</summary>
public static class AspireConnectionExtensions
{
    /// <summary>Sets a Relayer-owned Service Bus client's native connection string from ConnectionStrings:connectionName.</summary>
    /// <remarks>Native ClientOptions are unchanged. Do not also configure a caller-owned Client. Values are captured at registration.</remarks>
    public static TransportBuilder<AzureServiceBusTransportOptions, ServiceBusRouteOptions, ServiceBusConsumerOptions> UseAspireConnection(
        this TransportBuilder<AzureServiceBusTransportOptions, ServiceBusRouteOptions, ServiceBusConsumerOptions> builder,
        IConfiguration configuration, string connectionName)
    {
        ArgumentNullException.ThrowIfNull(builder);
        var connection = AspireConnectionConfiguration.ReadConnection(configuration, connectionName);
        builder.Options.Configure(options => options.ConnectionString = connection);
        return builder;
    }
}
