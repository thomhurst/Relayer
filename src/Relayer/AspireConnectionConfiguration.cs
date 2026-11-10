using Microsoft.Extensions.Configuration;

namespace Relayer;

internal static class AspireConnectionConfiguration
{
    internal static string ReadConnection(IConfiguration configuration, string connectionName)
    {
        ArgumentNullException.ThrowIfNull(configuration);
        ArgumentException.ThrowIfNullOrWhiteSpace(connectionName);
        var connection = configuration.GetConnectionString(connectionName);
        if (string.IsNullOrWhiteSpace(connection))
            throw new InvalidOperationException($"Missing ConnectionStrings:{connectionName}. Reference the Aspire resource with WithReference, or configure this connection string explicitly.");
        return connection;
    }
}
