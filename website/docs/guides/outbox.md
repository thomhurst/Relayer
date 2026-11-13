---
title: Use the outbox and inbox
description: "Commit business changes and outgoing messages together, deduplicate incoming deliveries and recover pending sends."
---

Use the outbox when a database change and an outgoing message must succeed together.
The transaction stores both; dispatch sends the message after commit. Delivery remains
at-least-once: a crash after broker acceptance but before the row is completed can send a duplicate.
The inbox deduplicates incoming messages by consumer name and original message ID.

This guide uses PostgreSQL and EF Core with Kafka. Reference `Relayer.Kafka`,
`Relayer.Outbox.EntityFrameworkCore` and the Npgsql EF Core provider. Packages are not published
yet; use project references from a repository checkout. The runnable
[Kafka and EF Core sample](samples.md) shows the complete host and business model.

## Configure a store and dispatch

Provision the Kafka topics externally; Kafka currently requires `TopologyMode.None`.
Deploy the [PostgreSQL schema](../reference/postgres-schema.md) before starting the host.
The default schema is `public`, with table prefix `relayer_`; use the same settings for schema
creation and registration. Register an ordinary scoped DbContext and the combined outbox:

```csharp
using Microsoft.EntityFrameworkCore;
using Npgsql;
using Relayer;
using Relayer.Kafka;
using Relayer.Outbox.EntityFrameworkCore;

// The application owns this data source; dispose it after the host stops.
await using var dataSource = NpgsqlDataSource.Create(connectionString);
services.AddDbContext<OrdersDbContext>(options => options.UseNpgsql(dataSource));
services.AddRelayer(relayer =>
{
    relayer.UsePostgreSqlOutbox<OrdersDbContext>(dataSource);
    relayer.AddMessageTypesFromThisAssembly();
    relayer.Kafka("events", kafka =>
    {
        kafka.TopologyMode = TopologyMode.None; // Provision topics externally.
        kafka.Options.Configure(options => options.BootstrapServers.Add("localhost:9092"));
        kafka.Send<OrderPlaced>("orders");
        kafka.Consume("orders", consumer =>
        {
            consumer.Options.Group = "billing";
            consumer.EntityFrameworkCore<OrdersDbContext>("orders.billing");
            consumer.Handler<OrderPlacedHandler>();
        });
    });
});

[Message("orders.placed", Version = 1)]
public sealed record OrderPlaced(string OrderId);
```

`UsePostgreSqlOutbox` wires transaction enlistment, unordered commit dispatch, ordered lane
dispatch, recovery and cleanup for the selected context. It adds EF interceptors but does not
configure the database provider or create tables. Use `AddDbContext`, not context pooling.
Keep the data source alive until all hosted workers stop. See
[EF Core registration](../reference/ef-core-outbox.md#registration) for store names, schema options,
multiple contexts and separate writer/worker deployments.

## Send within the business transaction

Resolve the DbContext and `ISender` from the same application scope. Begin and commit through
EF's transaction wrapper, saving your business changes normally:

```csharp
await using var transaction = await db.Database.BeginTransactionAsync(cancellationToken);
db.Orders.Add(new Order { Id = orderId });
await sender.SendViaOutboxAsync(new OrderPlaced(orderId), cancellationToken);
await db.SaveChangesAsync(cancellationToken);
await transaction.CommitAsync(cancellationToken);
```

The explicit transaction activates enlistment automatically when it is the scope's only active
context. `SendViaOutboxAsync` requires active enlistment and fails if it is missing. `SendAsync`
also enlists while active, but sends directly otherwise. `SendNowAsync` bypasses the outbox.
Native send callbacks cannot be stored; use portable headers, ordering keys and delivery times.

Commit writes business changes and outgoing rows atomically. Rollback or disposal discards the
buffered sends. Awaiting commit establishes database durability, not broker delivery. For implicit
saves, select the context with `db.EnlistRelayerOutbox()` before sending; see
[implicit saves](../reference/ef-core-outbox.md#save-without-an-explicit-transaction).
If an attempt fails, retry the whole business unit, not only its final save or commit;
[EF transaction recovery](../reference/ef-core-outbox.md#explicit-transactions) explains tracked-state
reset, savepoints and unknown commit outcomes.

## Deduplicate a consumer and send follow-up messages

The `consumer.EntityFrameworkCore<OrdersDbContext>("orders.billing")` declaration above opens
the database boundary around each handler attempt. Register it before any other consume middleware.
Keep its consumer name stable across replicas and restarts. With the selected outbox context,
inbox deduplication is automatic; merely registering an outbox does not attach a consumer boundary.

In the handler, use the scoped DbContext and `ISender`, apply business changes, send any follow-up
messages through registered routes, and call `SaveChangesAsync`. Do not start or commit another
transaction: the boundary commits the inbox receipt, saved changes and outgoing messages after the
handler and inner middleware succeed. Native settlement follows that commit and scope disposal.
A failed attempt rolls back the database unit; redelivery of a committed identity skips the handler.

The inbox protects work in this database transaction. External HTTP calls and direct native-client
sends need their own idempotency. Retain inbox receipts for at least your possible redelivery window;
removing a receipt permits that identity to execute again. See the
[inbox reference](../reference/ef-core-inbox.md) for isolation, connection retries and inbox-only use.

## Recover and inspect pending messages

The combined registration starts recovery workers as well as commit-triggered dispatch. After a
process crash, recovery claims eligible unordered rows once previous claims expire; ordered workers
resume through fenced lane leases. Scheduled rows wait until due. No application replay loop is needed.
Retryable broker failures remain pending with persisted backoff; monitor backlog and configure native
exception classification for permanent rejections. An outage does not expire pending messages by age.

Start diagnosis with [message lookup](../reference/postgres-store.md#find-a-missing-message), then
inspect pending, parked or dead rows. Fix the underlying failure before
[redriving poison rows or releasing parked keys](../reference/postgres-store.md#poison-rows-and-key-release).
These durable outbox rows are separate from a broker's receive dead-letter queue.

Use [maintenance settings](../reference/postgres-maintenance.md) for polling, retention and leadership,
[direct dispatch](../reference/direct-dispatch.md) for commit-triggered sends, and
[ordered dispatch](../reference/ordered-dispatch.md) for ordering and lane changes. Monitor
[outbox tracing and metrics](telemetry.md#outbox-tracing-and-metrics).
For ADO.NET/Dapper or other databases, follow the supported
[PostgreSQL](../reference/postgres-outbox.md), [SQL Server](../reference/sqlserver-store.md) or
[MySQL](../reference/mysql-store.md) transaction paths instead of assuming EF provider support.
