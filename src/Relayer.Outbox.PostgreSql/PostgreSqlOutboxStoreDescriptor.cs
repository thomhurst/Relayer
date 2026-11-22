using Npgsql;

namespace Relayer.Outbox.PostgreSql;

/// <summary>A read-only view of a named PostgreSQL outbox store, keyed by store name, for tests and diagnostics.</summary>
/// <remarks>Resolve with <c>GetRequiredKeyedService&lt;PostgreSqlOutboxStoreDescriptor&gt;(storeName)</c>.</remarks>
public sealed class PostgreSqlOutboxStoreDescriptor
{
    internal PostgreSqlOutboxStoreDescriptor(string name, NpgsqlDataSource dataSource, string? schema, string tablePrefix)
    {
        Name = name;
        DataSource = dataSource;
        Schema = schema;
        TablePrefix = tablePrefix;
    }

    /// <summary>The store name used for registration.</summary>
    public string Name
    {
        get;
    }

    /// <summary>The application-owned data source the store uses.</summary>
    public NpgsqlDataSource DataSource
    {
        get;
    }

    /// <summary>The schema for Relayer tables. <see langword="null"/> uses public.</summary>
    public string? Schema
    {
        get;
    }

    /// <summary>The prefix for table and index names.</summary>
    public string TablePrefix
    {
        get;
    }
}
