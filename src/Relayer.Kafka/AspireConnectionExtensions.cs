using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace Relayer.Kafka;

/// <summary>Reads native transport connections supplied by Aspire's WithReference.</summary>
public static class AspireConnectionExtensions
{
    /// <summary>Sets Kafka bootstrap servers from ConnectionStrings:connectionName. Native Dekaf callbacks remain unchanged.</summary>
    /// <remarks>Call once per transport, before other Options configuration. Values are captured at registration; restart to change the connection.</remarks>
    public static TransportBuilder<KafkaTransportOptions, KafkaConsumerOptions> UseAspireConnection(
        this TransportBuilder<KafkaTransportOptions, KafkaConsumerOptions> builder,
        IConfiguration configuration, string connectionName)
    {
        ArgumentNullException.ThrowIfNull(builder);
        var connection = AspireConnectionConfiguration.ReadConnection(configuration, connectionName);
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
}
