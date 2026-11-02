---
title: Native tenant store selection
description: "Select application-owned PostgreSQL stores for manual outbox units and explicit tenant endpoints."
---

An application can select a different native PostgreSQL store for each manually created service scope.
Use the scoped factory already provided by EF Core; Relayer's interceptors use that DbContext's native
connection and transaction. There is no separate Relayer `IOutboxStoreResolver` API.

This supports manually initiated outbox units and explicitly composed endpoints whose tenant is known
at startup. Dynamic selection from an incoming tenant header, automatic durable `tenant_id` population,
tenant enumeration and a shared dispatch connection budget remain planned. This page does not establish
complete multi-tenant consumer or dispatcher support.

## Select before resolving database services

The following selection types belong to your application. Populate the catalog with existing,
application-owned `NpgsqlDataSource` instances and authorized tenant identifiers. Do not create a new
data source for every scope or treat a message header as authentication.

```csharp
using Npgsql;

public sealed class AuthorizedTenant
{
    public string? Id { get; set; }
}

public sealed record TenantStore(NpgsqlDataSource DataSource, string Schema);

public sealed class TenantStoreCatalog(IReadOnlyDictionary<string, TenantStore> stores)
{
    public TenantStore Resolve(string? authorizedTenant)
    {
        if (authorizedTenant is null || !stores.TryGetValue(authorizedTenant, out var store))
            throw new InvalidOperationException("Select an authorized tenant store first.");
        return store;
    }
}
```

Register the catalog instance and a normal, non-pooled scoped DbContext. Both context options and
Relayer's send enlistment must use the same service scope:

```csharp
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Relayer.Outbox.EntityFrameworkCore;

services.AddSingleton(new TenantStoreCatalog(tenantStores));
services.AddScoped<AuthorizedTenant>();
services.AddRelayerEntityFrameworkCoreOutbox();
services.AddDbContext<OrdersDbContext>((provider, options) =>
{
    var tenant = provider.GetRequiredService<AuthorizedTenant>();
    var store = provider.GetRequiredService<TenantStoreCatalog>().Resolve(tenant.Id);
    options.UseNpgsql(store.DataSource).AddRelayerOutbox(provider,
        new EntityFrameworkCoreOutboxOptions { Schema = store.Schema });
});
```

Keep the usual `AddRelayer` message and route registrations. After authenticating and authorizing a
request, select its tenant **before** resolving the DbContext or any dependency that resolves it:

```csharp
await using var scope = serviceScopeFactory.CreateAsyncScope();
var provider = scope.ServiceProvider;
provider.GetRequiredService<AuthorizedTenant>().Id = authorizedTenantId;
var db = provider.GetRequiredService<OrdersDbContext>();
var sender = provider.GetRequiredService<ISender>();

using (db.EnlistRelayerOutbox())
{
    db.Orders.Add(order);
    await sender.SendAsync(new OrderPlaced(order.Id), cancellationToken);
    await db.SaveChangesAsync(cancellationToken);
}
```

Treat the selected tenant as immutable for the lifetime of the scope. Create a fresh scope for another
tenant or retry. Never use a default tenant when authorization or lookup fails. Concurrent requests
use separate scopes; a DbContext or send enlistment cannot be shared concurrently.

## Database and schema layouts

For a database per tenant, give each catalog entry the native data source for that database. For a schema
per tenant, configure the native connection's `SearchPath` for unqualified application tables and pass
the same schema to `AddRelayerOutbox`. For example, during catalog construction:

```csharp
var settings = new NpgsqlConnectionStringBuilder(connectionString)
{
    SearchPath = trustedSchema
};
var dataSource = NpgsqlDataSource.Create(settings.ConnectionString);
var store = new TenantStore(dataSource, trustedSchema);
```

Schema names come from a trusted catalog. PostgreSQL `search_path` has native identifier and trust
rules; arbitrary header values must never become connection settings or SQL. Provision the
[Relayer tables](postgres-schema.md) and business tables before accepting work. With this layout the EF
model uses unqualified table names, for example `ToTable("orders")`. An explicit `ToTable` schema or
`HasDefaultSchema` overrides the native search path. Changing schemas inside one cached EF model
requires EF's own model configuration strategy; Relayer does not implement one.

Both layouts retain PostgreSQL's transaction and unique-key semantics. Identical business IDs and
inbox message IDs can exist in different stores. Rolling back one tenant's unit rolls back that store's
business, inbox and outbox rows, without touching another tenant. These are separate physical stores,
not row-level isolation within a shared outbox/inbox table.

The application owns catalog data sources and disposes them at application shutdown, after their
users stop. EF disposes its contexts and connections; `UseNpgsql(dataSource)` does not transfer ownership
of the supplied data source. Native pool sizes apply per data source. They do not establish the planned
shared connection budget across tenants.

For [ADO.NET or Dapper](postgres-outbox.md), select the authorized native data source first and pass it
to `PostgreSqlOutbox.BeginTransactionAsync`. Configure the scoped outbox's schema/prefix to match the
selected store. The transaction owns only the connection it opens, not the application data source.

## Consumer and dispatch boundary

The [EF Core consumer boundary](ef-core-inbox.md) currently resolves a DbContext in a temporary startup
scope to validate provider and transaction settings. A dynamic scoped factory that requires the current
delivery's tenant therefore cannot compose this boundary yet. Do not bypass that check by falling back
to a default tenant or allowing an untrusted header to choose a store.

An explicitly composed endpoint with one known tenant can use its fixed native store in every scope,
including the startup scope. Such endpoints use the existing consumer boundary, inbox deduplication
and outbox interceptors unchanged. This is a static endpoint arrangement, not automatic routing of
mixed-tenant deliveries. Inbox identity remains local to each selected database/schema.

Dispatch and maintenance registrations also have explicit store identities. Register them separately
and match schema, prefix and lane settings; persistence alone does not dispatch rows. Automatic tenant
enumeration, dynamic store addition/removal, isolated tenant lane/lease coordination and one connection
budget spanning all tenant workers remain planned. Avoid creating permanent workers and unbounded
pools per tenant as a substitute.

The optional low-level `OutboxRecord.TenantId` remains caller-supplied metadata. Store selection neither
infers it from headers nor validates it against an authorized tenant. Applications must maintain that
consistency themselves until automatic tenant binding is implemented.
