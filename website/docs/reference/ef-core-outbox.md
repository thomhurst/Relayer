---
title: EF Core transaction enlistment
description: "Persist outgoing messages atomically with PostgreSQL business changes using EF Core interceptors."
---

`Relayer.Outbox.EntityFrameworkCore` implements transaction enlistment for the Npgsql EF Core provider.
It buffers serialized sends, copies borrowed memory before each send completes, and writes one batched
PostgreSQL `INSERT` immediately before commit. The insert uses the same connection and transaction as
the business data. No `SaveChanges` override or outbox entity mapping is required.

Enlistment alone persists rows. Enable [unordered direct dispatch](direct-dispatch.md) to send due messages
in the background after commit, or register [ordered dispatch and lane leases](ordered-dispatch.md) separately.
Register the [PostgreSQL durable store](postgres-store.md) separately for reads, claims and backlog health queries.
[Leased recovery and cleanup](postgres-maintenance.md) are available separately. Outbox tracing remains planned.
The [EF Core consumer boundary](ef-core-inbox.md) supplies transactional inbox deduplication and
enables it automatically for its selected outbox context.
There are no published NuGet packages yet; use project references from a repository checkout.

## Registration

Create the [PostgreSQL schema](postgres-schema.md) first. Register ordinary scoped DbContexts with
the same scoped service provider used by `ISender`:

```csharp
using Microsoft.EntityFrameworkCore;
using Relayer.Outbox.EntityFrameworkCore;

services.AddRelayerEntityFrameworkCoreOutbox();
services.AddDbContext<OrdersDbContext>((provider, options) =>
    options.UseNpgsql(connectionString).AddRelayerOutbox(provider));
```

Keep your normal `AddRelayer` message and route registrations. Register these interceptors on each
participating DbContext. Context pooling, concurrent use of one service scope and ambient
`TransactionScope` enlistment are not supported. Other EF database providers fail on first use;
[SQL Server schema tooling and ADO.NET enlistment](sqlserver-store.md) and [MySQL schema tooling](mysql-store.md) are implemented separately. EF Core integration for those providers remains planned.

Application-owned scoped factories can select native stores for manually initiated units and explicitly
composed tenant endpoints. See [native tenant store selection](tenant-stores.md) for database/schema
isolation, ownership and the current dynamic-consumer limitation.

The defaults are schema `public`, table prefix `relayer_`, and 32 lanes. Set immutable options per context:

```csharp
options.UseNpgsql(connectionString).AddRelayerOutbox(provider,
    new EntityFrameworkCoreOutboxOptions
    {
        Schema = "messaging",
        TablePrefix = "orders_",
        Lanes = 32
    });
```

Alternatively, `services.AddRelayerEntityFrameworkCoreOutbox(configuration.GetSection("Outbox"))`
binds immutable defaults through the Options pattern. `IOptionsSnapshot<EntityFrameworkCoreOutboxOptions>`
keeps a scope's settings stable; configuration changes apply to newly created scopes. Invalid identifiers,
nonpositive lane counts and malformed numeric values fail validation. A per-context options argument
overrides those defaults. Always match the deployed schema and prefix. Drain the outbox before changing lanes.
`LeaseDuration` and `MaxDispatchAttempts` do not affect enlistment. Configure ordered worker deadlines
through `PostgreSqlOrderedDispatchOptions`; direct dispatch uses `PostgreSqlDirectDispatchOptions.ClaimDuration`.
Poison-row movement remains planned.

## Save without an explicit transaction

Select the context before sending, so Relayer knows which future save owns the messages:

```csharp
using (db.EnlistRelayerOutbox())
{
    db.Orders.Add(order);
    await sender.SendAsync(new OrderPlaced(order.Id), cancellationToken);
    await db.SaveChangesAsync(cancellationToken);
}
```

`SaveChangesAsync` creates an owned transaction when rows are buffered, even for a single business
command or an outbox-only save. It commits business changes and outgoing rows together. Synchronous
`SaveChanges` works too. Disposing the selection before saving discards unsaved messages. A second save
within the selection starts a new unit. Disposing the selection never commits anything.

## Explicit transactions

Starting an EF transaction activates automatic enlistment when it is the scope's only active context:

```csharp
await using var transaction = await db.Database.BeginTransactionAsync(cancellationToken);
await sender.SendAsync(new OrderPlaced(order.Id), cancellationToken);
db.Orders.Add(order);
await db.SaveChangesAsync(cancellationToken);
await transaction.CommitAsync(cancellationToken);
```

Sends made after the final save still join the same batch at commit. Several saves do not cause several
outbox inserts. `SendViaOutboxAsync` requires an active transaction or context selection;
`SendNowAsync` bypasses enlistment. Without either, `SendAsync` sends directly to the transport.

Commit through EF's `IDbContextTransaction`, including the wrapper returned by `UseTransactionAsync`.
Calling `Commit` directly on the underlying `DbTransaction` bypasses EF interceptors and is unsupported.
Use EF's rollback and disposal APIs too. Cancellation or insertion failure prevents commit; disposing or
rolling back a transaction discards its buffered rows. Native callbacks cannot be persisted and fail before buffering.

After a failed save, commit, or manual savepoint rollback, roll back the complete explicit transaction
and retry the whole unit with a fresh DbContext. Relayer rejects commit of an invalidated unit.
For an implicit save, insertion failure rolls back business data even if EF has already accepted tracked
changes; reusing that change tracker is not a recovery strategy. Configure EF execution strategies around
the whole explicit transaction, rather than retrying a final save alone.

### Retrying execution strategies

For message consumers, the [EF Core consumer boundary](ef-core-inbox.md#ef-core-connection-retries)
applies the strategy to the complete handler automatically and verifies unknown commits through the
inbox. The following manual pattern is for units of work outside that consumer boundary.

When Npgsql uses `EnableRetryOnFailure()`, an implicit save with buffered messages fails before opening
a transaction, with a Relayer error explaining the required explicit transaction. Wrap the entire unit
in EF's execution strategy and create a fresh DI scope and DbContext on each attempt:

```csharp
await strategyContext.Database.CreateExecutionStrategy().ExecuteAsync(async () =>
{
    await using var scope = serviceScopeFactory.CreateAsyncScope();
    var db = scope.ServiceProvider.GetRequiredService<OrdersDbContext>();
    var sender = scope.ServiceProvider.GetRequiredService<ISender>();
    await using var transaction = await db.Database.BeginTransactionAsync(cancellationToken);
    db.Orders.Add(new Order { Id = orderId });
    await sender.SendAsync(new OrderPlaced(orderId), cancellationToken);
    await db.SaveChangesAsync(cancellationToken);
    await transaction.CommitAsync(cancellationToken);
});
```

Synchronous `Execute`, `SaveChanges` and transaction methods follow the same pattern. Do not carry
tracked entities or buffered messages from a failed attempt into the next attempt. A connection failure
during commit can leave the outcome unknown; retries still require application-level idempotency.

## Several DbContexts

Each context owns its own messages. Saving another context does not consume that buffer. When two
contexts have active transactions, wrap each send group in `context.EnlistRelayerOutbox()` to select
the owner; ambiguous automatic sends fail. Nested selections restore the previous owner when disposed
in reverse creation order. Dispose selections before their contexts.

Contexts may share a native connection and transaction with EF's `UseTransactionAsync`. Configure them
with the same scoped provider, schema, prefix and lane count. Select each owner while sending and
commit through EF. Relayer combines their rows into one insert for that native transaction.
Disposing a joined context or its borrowed transaction wrapper leaves its buffered messages attached
to the live native transaction. Automatic sends then select the remaining active context. Commit or
rollback through the owner ends the entire shared unit. Complete that transaction before reusing a
detached context for another unit; Relayer rejects reuse that could discard its pending messages.

## Stored representation

`IOutboxEnlistment.EnlistAsync(OutboxRecord, CancellationToken)` receives the original UUIDv7 ID,
stable wire type and creation time directly from the send pipeline. This also works with raw envelopes
and custom native header names; Relayer does not infer identity by scanning headers.
The serialized body, all headers, ordering key and delivery time survive buffer reuse. New sends have
zero attempts and no dispatch error; callers cannot supply a lane. Tenant ID is preserved when supplied
through the low-level record contract; automatic tenant resolution remains planned.

Header codec version 1 uses little-endian Int32 version and count, followed by name/value pairs, each
with an Int32 byte length and UTF-8 bytes. Header order and duplicate names are preserved. Ordered rows
use FNV-1a over the UTF-8 ordering key, modulo the configured lane count; unordered rows have a null lane.
The writer passes reusable column lists to one `unnest` insert, with a fixed parameter count for large batches.

The enlistment pipeline uses no reflection or runtime code generation. EF Core and its provider still
require their own supported compiled-model/query setup for Native AOT; the database transaction tests
run under the JIT and do not establish end-to-end EF Native AOT support.

### Declared scheduling requirements

For `SendRouteOptions.RequiredDeliveryDelay` fallback, register matching [maintenance](postgres-maintenance.md) or [ordered dispatch](ordered-dispatch.md) workers. Keep the global `EntityFrameworkCoreOutboxOptions` used by startup validation consistent with the participating DbContext's schema, prefix and lane count. The selected context is checked again before a scheduled row is buffered. Direct commit dispatch alone cannot deliver future schedules. See [send requirements](../guides/sending.md) for native limits and transaction guarantees.

Default scheduling capabilities are cached per options version. Resolving a sender without a DbContext
does not generate database SQL. Configuration reload updates defaults for newly resolved scopes;
an existing enlistment keeps its defaults. A selected DbContext always uses its own validated store settings.
