---
title: Native tenant store selection
description: "Select application-owned PostgreSQL stores through native scoped factories."
---

An application can select a different native PostgreSQL store for each application or single-message consumer scope.
Use the scoped factory already provided by EF Core; Relayer's interceptors use that DbContext's native
connection and transaction. There is no separate Relayer `IOutboxStoreResolver` API.

This supports manually initiated outbox units and incoming single-message EF Core consumers registered
with `AddRelayerTenancy`. The application authorizes the tenant before resolving its native store.
Automatic durable `tenant_id` population, tenant enumeration and a shared dispatch connection budget
remain planned.

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

With `AddRelayerTenancy`, the [EF Core consumer boundary](ef-core-inbox.md) composes without resolving
a startup DbContext. The incoming header is restored before the scoped native factory runs. Perform
authorization in that factory (or a scoped dependency it resolves), before selecting any database:

```csharp
services.AddRelayerTenancy();
services.AddScoped<AuthorizedTenant>(provider =>
{
    var incoming = provider.GetRequiredService<TenantContext>().Id.ToString();
    // Application-defined authorization. Throw for missing or unauthorized identifiers.
    var authorized = tenantAuthorizer.RequireAuthorized(incoming);
    return new AuthorizedTenant { Id = authorized };
});
```

Use this registration in place of the empty `AuthorizedTenant` registration in the manual example.
The existing native `AddDbContext` factory selects its authorized store. Compose the consumer with
`.EntityFrameworkCore<OrdersDbContext>("orders")` as usual; no fallback tenant is needed.

The selected context is validated before opening a transaction: the Npgsql provider is required,
pooled contexts are rejected, and inbox options must match the context's outbox registration.
Those checks run in each delivery and native retry scope. Native retry strategy selection also uses
the selected context. Each fresh retry and unknown-commit verification scope restores the original
header and resolves the application's authorization again. Keep the tenant-to-store mapping stable
for the delivery and its retries; never switch it while an attempt is in flight.
Retries must retain the same inbox/outbox capabilities.

Providers without `AddRelayerTenancy` retain startup context validation and can use fixed-store
endpoints. `Saga<TContext,TState>` still validates its state model with a startup context and does not
support tenant-required scoped factories. These guarantees apply to the single-message
`EntityFrameworkCore<TContext>` boundary.

Dispatch and maintenance registrations also have explicit store identities. Register them separately
and match schema, prefix and lane settings; persistence alone does not dispatch rows. Automatic tenant
enumeration, dynamic store addition/removal, isolated tenant lane/lease coordination and one connection
budget spanning all tenant workers remain planned. Avoid creating permanent workers and unbounded
pools per tenant as a substitute.

The optional low-level `OutboxRecord.TenantId` remains caller-supplied metadata. Store selection neither
infers it from headers nor validates it against an authorized tenant. Applications must maintain that
consistency themselves until automatic tenant binding is implemented.
