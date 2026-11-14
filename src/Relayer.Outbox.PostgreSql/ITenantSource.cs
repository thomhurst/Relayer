using System.Diagnostics.CodeAnalysis;
using Npgsql;

namespace Relayer.Outbox.PostgreSql;

/// <summary>An application-owned, authorized catalog of native PostgreSQL tenant stores.</summary>
[Experimental("RLYEXP004", UrlFormat = "https://relayer.tomlonghurst.dev/docs/reference/diagnostics#rlyexp004")]
public interface ITenantSource
{
    /// <summary>Returns a finite, immutable snapshot. Do not perform database or network I/O here.</summary>
    /// <remarks>Keep entries and their data sources alive until removal has been observed and in-flight work has finished.</remarks>
    IReadOnlyList<PostgreSqlTenantStore> GetTenants();
}

/// <summary>One independently provisioned store. Tenant IDs and physical stores must be unique within a snapshot.</summary>
/// <remarks>Use a native unpooled data source (Pooling=false) so idle pools cannot exceed the catalog's connection budget.
/// The application owns the data source. Use a separate pooled source for business transactions if desired.</remarks>
[Experimental("RLYEXP004", UrlFormat = "https://relayer.tomlonghurst.dev/docs/reference/diagnostics#rlyexp004")]
public sealed record PostgreSqlTenantStore(string TenantId, NpgsqlDataSource DataSource, PostgreSqlOutboxOptions Options);
