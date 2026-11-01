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
`ITransport` registrations. With a .NET host, register:

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
If you use `NpgsqlSlimDataSourceBuilder`, call `EnableArrays()` before `Build()`; fenced batch deletion
uses native UUID array parameters. The regular `NpgsqlDataSource` builder enables these mappings by default.

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

For each row, a conditional database update acquires a fresh fenced claim. Competing instances cannot
normally dispatch the same row. Before sending a group, the worker renews its claims and excludes any
row whose ownership has already expired. It preserves the stored message ID, body, native headers and
creation context; it does not serialize the application message again or generate a new envelope.
Once a stored delivery time is due, the native send requests immediate delivery. This also works with
transports such as Kafka that have no native scheduling support.

Messages are grouped by named transport and destination. A group is bounded by `BatchSize` and the
endpoint's native `MaxBatchSize`. Groups with several messages use `ITransport.SendBatchAsync`; a
single message uses `SendAsync`. Native byte limits and partial-delivery behavior still apply. After
success, one SQL statement deletes the group's rows, checking every token and live expiry separately.
The concrete `PostgreSqlOutboxStore.CompleteBatchAsync` exposes this fenced batch deletion to other
PostgreSQL integration authors and returns the number of rows deleted.

## Failures and current boundaries

Delivery remains at-least-once. A crash after a broker send, a partial native batch failure or a send
that outlives `ClaimDuration` can deliver a message again. The direct worker does not heartbeat during
a broker call. Choose a claim duration longer than expected native send and database latency.

A failed send records attempts and its error and releases each live claim. A database failure can
leave the claim held until expiry. Neither failure rolls back business data that already committed.
The worker logs failures and continues with subsequent commits; it does not retry the failed commit
in a busy loop.

`QueueCapacity` bounds queued commit notifications, not transaction size. If the queue is full or the
worker is stopped, an error states how many committed rows remain pending. Shutdown completes the
queue and drains it within the host's shutdown deadline; cancellation leaves unfinished work durable.

**Recovery polling, retry scheduling, ordered lane dispatch, poison-row handling and cleanup remain
planned.** Rows skipped because of a future `DeliverAt` or an ordering key remain in the database.
Rows left by failure, overflow, cancellation or process death also need recovery dispatch. This worker
handles immediate commit-triggered sends only; it is not a complete autonomous recovery system.
See [#32](https://github.com/thomhurst/Relayer/issues/32) and
[#33](https://github.com/thomhurst/Relayer/issues/33) for remaining dispatch work.
