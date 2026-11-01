using Microsoft.Extensions.DependencyInjection;

namespace Relayer.Outbox.PostgreSql;

/// <summary>Registers explicitly named PostgreSQL durable read/claim stores.</summary>
public static class PostgreSqlOutboxServiceCollectionExtensions
{
    /// <summary>Registers a keyed scoped store with an application-owned data source and snapshotted names.</summary>
    /// <remarks>This does not create tables, enlist sends, register health checks or run dispatch workers.</remarks>
    public static IServiceCollection AddRelayerPostgreSqlOutboxStore(this IServiceCollection services,
        string storeName, Npgsql.NpgsqlDataSource dataSource, PostgreSqlOutboxOptions? options = null)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentException.ThrowIfNullOrWhiteSpace(storeName);
        ArgumentNullException.ThrowIfNull(dataSource);
        options ??= new();
        ArgumentNullException.ThrowIfNull(options.TablePrefix);
        var snapshot = new PostgreSqlOutboxOptions { Schema = options.Schema, TablePrefix = options.TablePrefix };
        // Validate identifiers immediately, rather than waiting for the first database operation.
        _ = new PostgreSqlOutboxStore(dataSource, snapshot);
        services.AddKeyedScoped<IOutboxStore>(storeName, (_, _) => new PostgreSqlOutboxStore(dataSource, snapshot));
        return services;
    }
}
