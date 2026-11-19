---
title: Use the outbox and inbox
description: "Commit business changes and outgoing messages together, deduplicate incoming deliveries and recover pending sends."
---

Use the outbox to save business changes and outgoing messages in one database transaction.
Relayer sends the messages after the transaction commits.

This guide uses PostgreSQL, EF Core and Kafka. The [Kafka and EF Core sample](samples.md)
includes a complete host, DbContext and handler.

## Configure a store and dispatch

Add `Relayer.Kafka`, `Relayer.Outbox.PostgreSql.EntityFrameworkCore` and the Npgsql EF Core
provider to your project. Register a scoped DbContext, the PostgreSQL outbox and a Kafka send route.

```csharp
using Microsoft.EntityFrameworkCore;
using Npgsql;
using Relayer;
using Relayer.Kafka;
using Relayer.Outbox.EntityFrameworkCore;

await using var dataSource = NpgsqlDataSource.Create(connectionString);
services.AddDbContext<OrdersDbContext>(options => options.UseNpgsql(dataSource));
services.AddRelayer(relayer =>
{
    relayer.UsePostgreSqlOutbox<OrdersDbContext>(dataSource);
    relayer.AddMessageTypesFromThisAssembly();
    relayer.Kafka("events", kafka =>
    {
        kafka.Options.Configure(options => options.BootstrapServers.Add("localhost:9092"));
        kafka.Send<OrderPlaced>("orders");
    });
});

[Message("orders.placed", Version = 1)]
public sealed record OrderPlaced(string OrderId);
```

## Send within the business transaction

Resolve `db` and `sender` (`ISender`) from the same scope. Open an EF Core transaction and use
`SendAsync` to save the message with the order when the transaction commits.

```csharp
await using var transaction = await db.Database.BeginTransactionAsync(cancellationToken);
db.Orders.Add(new Order { Id = orderId });
await sender.SendAsync(new OrderPlaced(orderId), cancellationToken);
await db.SaveChangesAsync(cancellationToken);
await transaction.CommitAsync(cancellationToken);
```

## Deduplicate a consumer and send follow-up messages

Add this consumer to the Kafka registration above. The inbox runs the handler in a database
transaction and skips deliveries it has already committed for `orders.billing`.

```csharp
kafka.Consume("orders", consumer =>
{
    consumer.Options.Group = "billing";
    consumer.EntityFrameworkCore<OrdersDbContext>("orders.billing");
    consumer.Handler<OrderPlacedHandler>();
});
```

## Things to know

### Before starting the host

Create the Kafka topics and deploy the [PostgreSQL schema](../reference/postgres-schema.md).
Kafka checks that the topics exist at startup by default. Use the same schema and table prefix in
the deployment and outbox registration. The defaults are `public` and `relayer_`.

Use `AddDbContext` to register a scoped context and keep the Npgsql data source alive until the host
stops. See [EF Core registration](../reference/ef-core-outbox.md#registration) for context pooling
restrictions, multiple contexts and separate writer and worker deployments.

### Transaction choices and retries

The explicit transaction enlists sends automatically when it is the scope's only active context.
With an outbox registered, `SendAsync` throws if no outbox transaction or pending unit of work is active.
`SendViaOutboxAsync` also requires enlistment and additionally rejects a missing outbox registration.
For an implicit save, call `db.EnlistRelayerOutbox()`
before sending. See [EF Core transactions](../reference/ef-core-outbox.md#explicit-transactions)
for retrying the whole business operation. To bypass the outbox, use `SendWithoutOutboxAsync`.
See [sending messages](sending.md) for how each send method behaves.
When upgrading an application that relied on direct fallback, follow [Upgrade outbox sends](../migration/outbox-sends.md).

### What the inbox protects

Delivery is at least once. If the broker accepts a message and the dispatcher crashes before
recording success, the message can arrive again. The inbox uses the consumer name and original
message ID to recognize duplicates. Keep the name stable and keep receipts for at least as long as
messages can be redelivered.

Register the EF Core boundary before other consume middleware. To write data or send follow-up
messages, use the scoped DbContext and sender and call `SaveChangesAsync`. Let the boundary commit
the changes, outgoing messages and inbox receipt together. External side effects such as HTTP calls
need their own idempotency. See the [inbox reference](../reference/ef-core-inbox.md) for details.

## Recover and inspect pending messages

The outbox workers resume pending sends after a restart. If a message is missing, start with
[message lookup](../reference/postgres-store.md#find-a-missing-message) to see whether it is pending,
parked or dead. Fix the underlying failure before
[redriving the row or releasing its key](../reference/postgres-store.md#poison-rows-and-key-release).

For polling, cleanup and retention, see
[PostgreSQL maintenance](../reference/postgres-maintenance.md).

## Other transaction options

For ADO.NET or Dapper, see the transaction examples for
[PostgreSQL](../reference/postgres-outbox.md), [SQL Server](../reference/sqlserver-store.md) and
[MySQL](../reference/mysql-store.md).
