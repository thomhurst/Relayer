using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Relayer.AzureServiceBus;
using Relayer.Kafka;

namespace Relayer.Aspire;

/// <summary>Reads native transport connections supplied by Aspire's WithReference.</summary>
public static class AspireConnectionExtensions
{
    /// <summary>Sets Kafka bootstrap servers from ConnectionStrings:connectionName. Native Dekaf callbacks remain unchanged.</summary>
    /// <remarks>Call once per transport, before other Options configuration. Values are captured at registration; restart to change the connection.</remarks>
    public static TransportBuilder<KafkaTransportOptions, KafkaRouteOptions, KafkaConsumerOptions> UseAspireConnection(
        this TransportBuilder<KafkaTransportOptions, KafkaRouteOptions, KafkaConsumerOptions> builder,
        IConfiguration configuration, string connectionName)
    {
        ArgumentNullException.ThrowIfNull(builder);
        var connection = ReadConnection(configuration, connectionName);
        var servers = connection.Split(',', StringSplitOptions.TrimEntries);
        if (Array.Exists(servers, string.IsNullOrWhiteSpace))
            throw new InvalidOperationException($"ConnectionStrings:{connectionName} must contain comma-separated Kafka bootstrap servers without empty entries.");
        builder.Options.Configure(options =>
        {
            options.BootstrapServers.Clear();
            foreach (var server in servers) options.BootstrapServers.Add(server);
        });
        return builder;
    }

    /// <summary>Sets a Relayer-owned Service Bus client's native connection string from ConnectionStrings:connectionName.</summary>
    /// <remarks>Native ClientOptions are unchanged. Do not also configure a caller-owned Client. Values are captured at registration.</remarks>
    public static TransportBuilder<AzureServiceBusTransportOptions, ServiceBusRouteOptions, ServiceBusConsumerOptions> UseAspireConnection(
        this TransportBuilder<AzureServiceBusTransportOptions, ServiceBusRouteOptions, ServiceBusConsumerOptions> builder,
        IConfiguration configuration, string connectionName)
    {
        ArgumentNullException.ThrowIfNull(builder);
        var connection = ReadConnection(configuration, connectionName);
        builder.Options.Configure(options => options.ConnectionString = connection);
        return builder;
    }

    private static string ReadConnection(IConfiguration configuration, string connectionName)
    {
        ArgumentNullException.ThrowIfNull(configuration);
        ArgumentException.ThrowIfNullOrWhiteSpace(connectionName);
        var connection = configuration.GetConnectionString(connectionName);
        if (string.IsNullOrWhiteSpace(connection))
            throw new InvalidOperationException($"Missing ConnectionStrings:{connectionName}. Reference the Aspire resource with WithReference, or configure this connection string explicitly.");
        return connection;
    }
}
