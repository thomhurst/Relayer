using Relayer.Transports;
using System.Diagnostics.CodeAnalysis;
using Microsoft.Extensions.DependencyInjection;

namespace Relayer.Outbox.SqlServer;

/// <summary>Explicitly named SQL Server durable stores.</summary>
public static partial class SqlServerOutboxServiceCollectionExtensions
{
    /// <summary>Registers a keyed scoped store, without schema deployment, enqueue or workers.</summary>
    /// <remarks>The native connection string, including pooling and authentication settings, passes through unchanged.</remarks>
    [RequiresUnreferencedCode("SqlClient runtime trimming is unsupported; see https://github.com/dotnet/SqlClient/issues/1947.")]
    [RequiresDynamicCode("SqlClient runtime Native AOT is unsupported; see https://github.com/dotnet/SqlClient/issues/1947.")]
    public static IServiceCollection AddRelayerSqlServerOutboxStore(this IServiceCollection services,
        string storeName, string connectionString, SqlServerOutboxOptions? options = null)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentException.ThrowIfNullOrWhiteSpace(storeName);
        ArgumentException.ThrowIfNullOrWhiteSpace(connectionString);
        _ = new Microsoft.Data.SqlClient.SqlConnectionStringBuilder(connectionString);
        options ??= new();
        _ = SqlServerOutboxSchema.CreateScript(options);
        services.AddKeyedScoped<IOutboxStore>(storeName, (_, _) => new SqlServerOutboxStore(connectionString, options));
        return services;
    }
}
