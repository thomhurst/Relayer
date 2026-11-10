---
title: Migrate from CAP
description: "Move named CAP publications and subscriptions to typed routes with explicit database durability."
---

CAP's [messaging API](https://cap.dotnetcore.xyz/user-guide/en/cap/messaging/) uses named publications and subscriptions. Its [transaction model](https://cap.dotnetcore.xyz/user-guide/en/cap/transactions/) targets eventual consistency rather than a distributed ACID transaction. Preserve that boundary when moving to Relayer: commit business changes and an outgoing row locally, then dispatch to the broker.

## Map publication and subscription

| CAP | Relayer |
| --- | --- |
| `ICapPublisher.PublishAsync("orders.placed", message)` | `ISender.SendAsync(message, cancellationToken)` with `Send<OrderPlaced>(destination)` |
| `[CapSubscribe("orders.placed")]` method | Explicit `Consume(destination, consumer => consumer.Handler<OrderPlacedHandler>())` |
| Publication name and subscriber group | Stable `[Message("orders.placed", Version = 1)]` plus separately configured native destination/group |

The wire identity and broker destination are separate. A CAP publication name is not enough to configure Relayer's route or deserialize old records. Add concrete contracts through `AddMessageTypesFromThisAssembly()` and implement `IHandler<T>.HandleAsync`. Start from the runnable [Kafka EF outbox sample](../guides/samples.md), whose POST endpoint does:

```csharp
await using var transaction = await db.Database.BeginTransactionAsync(cancellationToken);
db.Orders.Add(new Order { Id = id });
await sender.SendAsync(new OrderPlaced(id), cancellationToken);
await db.SaveChangesAsync(cancellationToken);
await transaction.CommitAsync(cancellationToken);
```

This is the sample's real call site: its PostgreSQL context has `AddRelayerOutbox` interceptors, and direct dispatch plus maintenance use the same database. Outside an active transaction or explicit context selection, ordinary `SendAsync` sends directly. Use `SendViaOutboxAsync` when missing enlistment must fail. See [EF transaction enlistment](../reference/ef-core-outbox.md) for setup and failure semantics.

## Check storage and subscriber behavior

The EF integration currently supports PostgreSQL/Npgsql. SQL Server has separate [ADO.NET/Dapper transactional APIs](../reference/sqlserver-store.md); SQL Server EF integration remains planned. MySQL has [store primitives and schema tooling](../reference/mysql-store.md); do not infer full CAP storage-provider parity. Do not reuse CAP tables: Relayer has its own schema, message identities, row ownership and retention.

For consumers that change a database, register the implemented [PostgreSQL EF inbox boundary](../reference/ef-core-inbox.md). Transport settlement follows durable work. Inbox deduplication is scoped to the stored consumer/message identity; business operations must still tolerate duplicates from bridges or publishers that assign a new identity. Match native consumer groups or subscriptions explicitly; no attribute scanning or CAP subscriber group discovery occurs.

Configure [failure handling](../guides/failure-handling.md) rather than assuming CAP retry defaults. CAP callback names do not translate automatically; model an explicit response message or use the implemented [request/response](../guides/request-response.md) API after checking its timeout and routing constraints. CAP dashboard/storage history compatibility is unavailable. Scheduled delivery depends on the selected native transport or configured outbox fallback. Parallel Kafka receive remains planned.

## Drain before switching

Finish CAP's pending published/received work and retries with the original system. If old producers must coexist, use a bridge that receives with CAP and sends the new typed Relayer contract to separate destinations. Reusing JSON bodies alone does not preserve headers, identity or subscription behavior. Test rollback, duplicate receipt, crash recovery and poison messages against the actual broker and database before switching producers. The [testing guide](../guides/testing.md) describes the harness and integration boundary.
