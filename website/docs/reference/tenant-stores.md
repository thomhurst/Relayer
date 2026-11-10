---
title: Native tenant store selection
description: "Select application-owned PostgreSQL stores through native scoped factories."
---

An application can select a different native PostgreSQL store for each application or single-message consumer scope.
Use the scoped factory already provided by EF Core; Relayer's interceptors use that DbContext's native
connection and transaction. There is no separate Relayer `IOutboxStoreResolver` API.

This supports manually initiated outbox units and incoming single-message EF Core consumers registered
with `AddRelayerTenancy`. The application authorizes the tenant before resolving its native store.
A scoped `OutboxTenantBinding` supplied by that factory binds the store's durable tenant metadata to
its application-authorized tenant and the propagated header. A native `ITenantSource` catalog supports bounded dispatch across tenant stores.

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
services.AddRelayerTenancy();
services.AddRelayerEntityFrameworkCoreOutbox();
services.AddDbContext<OrdersDbContext>((provider, options) =>
{
    var tenant = provider.GetRequiredService<AuthorizedTenant>();
    var store = provider.GetRequiredService<TenantStoreCatalog>().Resolve(tenant.Id);
    options.UseNpgsql(store.DataSource).AddRelayerOutbox(provider,
        new EntityFrameworkCoreOutboxOptions
        {
            Schema = store.Schema,
            TenantBinding = new OutboxTenantBinding(provider.GetRequiredService<TenantContext>(), tenant.Id!)
        });
});
```

Keep the usual `AddRelayer` message and route registrations. After authenticating and authorizing a
request, select its tenant **before** resolving the DbContext or any dependency that resolves it:

```csharp
await using var scope = serviceScopeFactory.CreateAsyncScope();
var provider = scope.ServiceProvider;
provider.GetRequiredService<AuthorizedTenant>().Id = authorizedTenantId;
provider.GetRequiredService<TenantContext>().Id = new WireString(authorizedTenantId);
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
of the supplied data source. Native pool sizes apply per data source. The bounded dispatcher below
uses separate unpooled native sources to enforce its aggregate physical connection limit.

For [ADO.NET or Dapper](postgres-outbox.md), select the authorized native data source first and pass it
to `PostgreSqlOutbox.BeginTransactionAsync`. Configure the scoped outbox's schema/prefix to match the
selected store, including `PostgreSqlOutboxOptions.TenantBinding` created with the same scoped
`TenantContext` and authorized tenant ID. Supply these scope-specific options through your scoped
`IOptionsSnapshot<PostgreSqlOutboxOptions>` factory or your scoped `PostgreSqlOutbox` factory; do not
put a binding in singleton options or configuration files. The transaction owns only the connection
it opens, not the application data source.

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

## Dispatch a tenant catalog

Register one scheduler for the application-owned catalog instead of permanent hosted workers for each
tenant. It enumerates a finite snapshot each pass and reuses the PostgreSQL direct queue, unordered
recovery, ordered lane coordination and retention operations. Each store has independent tables,
membership and fenced leases. Stored native headers, including tenant headers, are sent unchanged.

```csharp
using Relayer.Outbox.PostgreSql;

public sealed class DispatchCatalog : ITenantSource
{
    private IReadOnlyList<PostgreSqlTenantStore> _snapshot = [];
    public IReadOnlyList<PostgreSqlTenantStore> GetTenants() => Volatile.Read(ref _snapshot);
    public void Replace(PostgreSqlTenantStore[] entries) => Volatile.Write(ref _snapshot, entries);
}

// This separate native source belongs to the application. Do not change a writer's pooled source.
var dispatchSettings = new NpgsqlConnectionStringBuilder(connectionString) { Pooling = false };
var dispatchSource = NpgsqlDataSource.Create(dispatchSettings.ConnectionString);
var catalog = new DispatchCatalog();
catalog.Replace([
    new PostgreSqlTenantStore("tenant-a", dispatchSource,
        new PostgreSqlOutboxOptions { Schema = "tenant_a", Lanes = 32 }),
    new PostgreSqlTenantStore("tenant-b", dispatchSource,
        new PostgreSqlOutboxOptions { Schema = "tenant_b", Lanes = 32 })
]);
services.AddSingleton<ITenantSource>(catalog);
services.AddRelayerPostgreSqlTenantDispatch(new PostgreSqlTenantDispatchOptions
{
    MaxConnections = 4,
    TurnTimeout = TimeSpan.FromSeconds(15)
});
```

The native data source may serve several isolated schemas or one database. Provision all tables first,
and match every writer's schema, prefix, lane count, lease duration and maximum attempts. IDs and
physical stores must be unique in each snapshot. Authorization and tenant-to-store mapping belong to
the application; catalog membership does not authorize an incoming request. Return an immutable,
thread-safe snapshot without network or database I/O. Keep data sources alive until the dispatcher
stops, then dispose them in application shutdown.

For a selected EF context, set `EntityFrameworkCoreOutboxOptions.DispatchStoreName` to its catalog
`TenantId`. The existing commit handoff then uses the catalog's bounded direct queue. Dispatch begins
on the next catalog turn, without awaiting the broker in the committing scope. Catalog recovery also
handles ADO.NET/Dapper rows, queue overflow, crashes and future-due rows. Dynamic catalog scheduling
capabilities are available on that explicitly selected EF context; the unrelated default scoped
outbox options do not infer a tenant's scheduling capabilities.

`MaxConnections` bounds all **catalog dispatcher** connections together, including direct/recovery
claims, ordered transactions, membership probes, retention and lease release. It does not bound
business transactions, fixed-store workers or other applications. Do not register fixed dispatch,
maintenance or CDC workers for the same catalog stores. Each native operation finishes before the
next operation in a tenant turn begins. Native `Pooling=false` is required: Npgsql's per-source pools
could otherwise retain an idle connection for every visited tenant even with a single admitted turn.
Relayer validates this setting and never rewrites native configuration. Separate pooled writer
sources remain supported.

Additions become eligible on the next pass, and a new EF scope can notify an added store immediately.
Removals finish any already admitted turn, stop accepting direct handoffs, discard queued memory
while retaining durable rows, and release owned leases within the same budget. If a store is
unavailable or shutdown interrupts release, native leases expire. Re-adding the store resumes durable
recovery. Replacing a source or store options retires its previous state; create fresh EF scopes after
the catalog pass has observed the replacement. Old scopes may leave committed rows for recovery.

Each turn has a `TurnTimeout`, so a failed or contended store cannot monopolize admission indefinitely.
Ordered batches stop admitting messages after half the remaining turn budget (or the existing lease-based
admission budget, whichever is shorter), reserving time for the final admitted send and database commit.
This lets completed sends drain a lane when a turn is shorter than a full batch. A send that itself
exceeds the deadline can still roll back the batch and be retried under at-least-once delivery.
Tenant turns resume with the next dispatch phase after a timeout, so a busy direct queue cannot
starve recovery, ordered delivery or cleanup. Ordered passes likewise resume at the next lane;
a failed lane does not stop healthy lanes. Native operations and transports must honor cancellation. Failures are logged and other stores keep
running. `PollInterval` controls catalog refresh and scheduling latency, `RecoveryDelay` controls
unordered recovery age, and cleanup follows `CleanupInterval` and the retention settings. A large
catalog may take several lane lease durations to revisit a store; native ownership is re-established
on each turn before dispatch. Scheduling settings also bind from `IConfiguration`; `infinite` disables
a retention limit. Defaults retain dead rows for 30 days and inbox identities for seven days.

## Durable tenant binding

Create `OutboxTenantBinding` after authorization, in the same scoped factory that selects the native
store. Attach it to `EntityFrameworkCoreOutboxOptions.TenantBinding` or
`PostgreSqlOutboxOptions.TenantBinding`. With `AddRelayerTenancy`, these outbox registrations require
a binding; missing bindings fail when the scoped outbox or DbContext options are resolved.

The binding is an explicit application assertion about the selected connection, database and schema.
Relayer cannot authenticate tenants or infer resource ownership from a connection string. The factory
must choose the authorized store; the binding verifies that the scope and messages agree with that
choice. Never reuse the binding across scopes or mutate the tenant during a unit of work.

Scoped sends already carry the header. For low-level `IOutboxEnlistment.EnlistAsync` calls, supply
exactly one `relayer.tenant_id` header matching the authorized ID. Relayer fills a missing
`OutboxRecord.TenantId` with that ID and accepts an explicitly matching value. Missing, duplicate or
conflicting headers, conflicting durable metadata, and a changed scoped tenant fail before enqueueing.
A rejected enlistment invalidates the unit: roll back and retry in a fresh authorized scope, even if
your code caught the exception. Commit validates the current tenant again, including units with no
outgoing messages. EF also validates before starting transactions and saving business changes.

Comparisons are ordinal and preserve UTF-8 values without per-message decoding. IDs are not case-folded
or normalized. The binding snapshots the authorized ID once per scoped store. It does not change native
transaction, retry, rollback or inbox uniqueness semantics. Without tenancy and a binding, existing
fixed-store callers retain their optional caller-supplied `OutboxRecord.TenantId` behavior.
