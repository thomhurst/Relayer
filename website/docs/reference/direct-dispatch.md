---
title: Unordered outbox direct dispatch
description: "Send committed PostgreSQL outbox messages immediately through named native transports."
---

The PostgreSQL direct dispatcher sends due, unordered messages in the background as soon as an
[EF Core outbox transaction](ef-core-outbox.md) commits. It does not wait for a polling interval.
Committing the transaction does not wait for the broker. A rollback never queues sends.

## Registration

Create the [schema](postgres-schema.md) first. Use the same application-owned `NpgsqlDataSource`,
database, schema and prefix for EF Core and dispatch. Keep your normal `AddRelayer` routes and named
`ITransport` registrations. For EF Core applications, prefer [the combined outbox registration](ef-core-outbox.md#registration),
which includes direct dispatch and recovery. To configure workers individually:

```csharp
using Microsoft.EntityFrameworkCore;
using Relayer.Outbox.EntityFrameworkCore;
using Relayer.Outbox.PostgreSql;

services.AddRelayerPostgreSqlDirectDispatch("orders", dataSource,
    new PostgreSqlOutboxOptions { Schema = "messaging", TablePrefix = "orders_" },
    new PostgreSqlDirectDispatchOptions
    {
        QueueCapacity = 256,
        BatchSize = 100,
        ClaimDuration = TimeSpan.FromSeconds(30)
    });
services.AddRelayerPostgreSqlMaintenance("orders", dataSource,
    new PostgreSqlMaintenanceOptions { Schema = "messaging", TablePrefix = "orders_" });
services.AddRelayerEntityFrameworkCoreOutbox();
services.AddDbContext<OrdersDbContext>((provider, options) =>
    options.UseNpgsql(dataSource).AddRelayerOutbox(provider,
        new EntityFrameworkCoreOutboxOptions
        {
            Schema = "messaging",
            TablePrefix = "orders_",
            DispatchStoreName = "orders"
        }));
```

Direct-dispatch registration also registers the named scoped `IOutboxStore`; do not register that store
again. The host starts and stops the worker. Resolving services without starting their hosted services
does not dispatch messages. Dispose the data source only after the host has stopped.
Startup fails before workers start if direct dispatch has no maintenance worker using the same
`NpgsqlDataSource` instance, schema and table prefix. A differently named maintenance worker can recover
the same tables. Ordered dispatch alone does not recover unordered rows.
If you use `NpgsqlSlimDataSourceBuilder`, call `EnableArrays()` before `Build()`; fenced batch claim and deletion
operations use native UUID array parameters. The regular `NpgsqlDataSource` builder enables these mappings by default.

Set `DispatchStoreName` on each participating context. Names are case-sensitive. A missing dispatcher
or mismatched schema/prefix fails when the DbContext options are built. Omitting the name keeps
enlistment-only behavior. The EF Core options also bind this name from configuration.

For dispatch limits from configuration, use
`services.AddRelayerPostgreSqlDirectDispatch("orders", dataSource, configuration.GetSection("Dispatch"), storeOptions)`.
Keys are `QueueCapacity`, `BatchSize` and `ClaimDuration`. Registration snapshots the values and rejects
malformed or nonpositive values. Read the same named snapshot with
`IOptionsMonitor<PostgreSqlDirectDispatchOptions>.Get("orders")`. Restart the host to change dispatch limits.

## Sending and ownership

Use the ordinary [transactional send API](ef-core-outbox.md#save-without-an-explicit-transaction).
Both synchronous and asynchronous saves and explicit EF transaction commits notify the worker only
after successful commit. The worker takes ownership of reusable serialized buffers before the caller
can reuse or dispose its scope. It never uses the committing DbContext in the background.

For each native group, one conditional database update acquires fresh fenced claims for eligible rows.
Competing instances cannot normally dispatch the same row. The worker excludes rows claimed by another
instance, and skips sending if the group's shared claim deadline has already passed when the database
responds. It preserves the stored message ID, body, native headers and
creation context; it does not serialize the application message again or generate a new envelope.
Once a stored delivery time is due, the native send requests immediate delivery. This also works with
transports such as Kafka that have no native scheduling support.

Messages are grouped by named transport and destination. A group is bounded by `BatchSize` and the
endpoint's native `MaxBatchSize`. Groups with several messages use `ITransport.SendBatchAsync`; a
single message uses `SendAsync`. Native byte limits and partial-delivery behavior still apply. After
success, one SQL statement deletes the group's rows, checking every token and live expiry separately.
The concrete `PostgreSqlOutboxStore.CompleteBatchAsync` exposes this fenced batch deletion to other
PostgreSQL integration authors and returns the number of rows deleted.

Each worker retains at most 256 successful capability lookups, keyed by both named transport and
exact destination. The cache evicts the oldest inserted entry when full; hits do not change eviction
order. An evicted destination is resolved again through `ITransport.GetCapabilities` before its next
group is claimed, so its own native `MaxBatchSize` still applies. Failed lookups are never cached.
This bounds retained destination metadata for applications with changing tenant destinations. Working
sets larger than 256 entries can require more native capability lookups; there is no cache setting.

## Failures and current boundaries

Delivery remains at-least-once. A crash after a broker send, a partial native batch failure or a send
that outlives `ClaimDuration` can deliver a message again. The direct worker does not heartbeat during
a broker call. Choose a claim duration longer than expected native send and database latency.

A failed send records attempts and its error and releases each live claim. Native exception messages
are stored in `last_error`; restrict database access as these messages can contain endpoint details.
A completion failure after a successful send logs a distinct error and leaves claims held until expiry,
without changing attempts or `last_error`. If completion deletes fewer rows than were sent, a warning
reports both counts and the duplicate-delivery risk. Increase `ClaimDuration` if native send or database
latency exceeds the lease. Neither failure rolls back business data that already committed.
If a transport is unregistered, capability lookup throws, or the endpoint reports a nonpositive
`MaxBatchSize`, the worker logs the error and skips that transport/destination group without claiming
its rows or consuming send attempts. Other groups in the same commit still dispatch. The skipped rows
need recovery dispatch after the configuration or destination is corrected.
The worker logs failures and continues with other groups and subsequent commits; it does not retry
the failed group in a busy loop.

`QueueCapacity` bounds queued commit notifications, not transaction size. If the queue is full or the
worker is stopped, an error states how many committed rows remain pending. Shutdown completes the
queue and drains it within the host's shutdown deadline; cancellation leaves unfinished work durable.

[Ordered lane dispatch](ordered-dispatch.md) handles rows with ordering keys separately.
[Leased recovery and cleanup](postgres-maintenance.md) handles due unordered rows left by failure,
overflow, cancellation, process death or a future `DeliverAt`. Register it explicitly alongside this worker.
Retry backoff remains planned. [Poison rows](postgres-store.md#poison-rows-and-key-release) move atomically to dead storage after `storeOptions.MaxDispatchAttempts` failed sends (default 10). This worker handles immediate commit-triggered sends.
