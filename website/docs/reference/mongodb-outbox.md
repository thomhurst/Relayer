---
title: MongoDB persistence foundation
description: "Explicit MongoDB outbox registration, native session transactions and fenced durable storage."
---

:::warning Release blocker

This provider is under development in [#233](https://github.com/thomhurst/Relayer/issues/233).
MongoDB.Driver 3.13.0 emits trimming and Native AOT analysis diagnostics that fail Relayer's strict publish gate.
The provider is not ready for release. See the upstream [trimming work](https://jira.mongodb.org/browse/CSHARP-4840).
Runtime smoke coverage with explicit serializer roots does not replace a clean strict publish.

:::

`Relayer.Outbox.MongoDB` supplies native transaction enlistment and the [durable store contract](outbox-stores.md).
It does not start a dispatcher. MongoDB inbox handling, commit-triggered sends, ordered lane ownership,
crash-recovery workers and retention remain planned in [#234](https://github.com/thomhurst/Relayer/issues/234)
and [#235](https://github.com/thomhurst/Relayer/issues/235). Rows remain durable until an application-owned
dispatcher claims and completes them. Registering this foundation alone does not deliver stored messages.

## Deploy and register explicitly

Use a transaction-capable replica set or sharded cluster. A standalone MongoDB server cannot run these
transactions. Deploy collections outside application transactions with permissions to create collections and indexes:

```csharp
using MongoDB.Driver;
using Relayer.Outbox.MongoDB;

var client = new MongoClient(connectionString); // Application owns this client.
var database = client.GetDatabase("orders");
var options = new MongoDBOutboxOptions { CollectionPrefix = "orders_", Lanes = 32 };

await MongoDBOutboxSchema.CreateAsync(database, options);
services.AddRelayerMongoDBOutbox("orders", database, options);
```

The deployment method checks transaction support and creates `orders_outbox`, its UUID primary index,
a pending `(lane, _id)` index and a delivery-time index. It is idempotent and never runs during registration.
Deploy business collections separately. Native client, database and transaction configuration pass through
unchanged; select the write concern required by your durability policy, such as `WriteConcern.WMajority`.
Read/claim stores reject unacknowledged writes rather than returning an unreliable outcome.

`AddRelayerMongoDBOutbox` registers scoped `MongoDBOutbox`, the same scoped `IOutboxEnlistment` used by
`ISender`, and a keyed scoped `IOutboxStore` named `orders`. Configure only one scoped enlistment integration.
For read/claim operations without send enlistment, use `AddRelayerMongoDBOutboxStore("orders", database, options)`.
The [health adapter](../guides/health-checks.md) can use that keyed store separately.

Both registration methods accept `IConfiguration` instead of an options instance. `CollectionPrefix` and `Lanes`
bind to named `MongoDBOutboxOptions`, available through `IOptionsSnapshot<MongoDBOutboxOptions>.Get("orders")`.
Invalid options fail startup validation. Reloaded configuration applies to new scopes. Keep collection names and
lane counts consistent across writers and readers; drain the outbox before changing the lane count.

## Use the native session transaction

Resolve `outbox` and `sender` from the same async DI scope. Configure message types and routes normally.
Application writes must use the same native session:

```csharp
using var session = await client.StartSessionAsync(cancellationToken: cancellationToken);
session.StartTransaction(new TransactionOptions(writeConcern: WriteConcern.WMajority));
using var association = outbox.EnlistTransaction(session);
try
{
    await orders.InsertOneAsync(session, orderDocument, cancellationToken: cancellationToken);
    await sender.SendAsync(new OrderPlaced(orderId), cancellationToken: cancellationToken);
    await session.CommitTransactionAsync(cancellationToken);
}
catch
{
    if (session.IsInTransaction) await session.AbortTransactionAsync(CancellationToken.None);
    throw;
}
```

`EnlistTransaction` accepts an already-active `IClientSessionHandle` from the database's `MongoClient`.
Relayer writes each outgoing row into that transaction before its send completes. Native commit makes business
writes and outbox rows visible together; native abort discards both. Borrowed message memory can be reused after
the send completes. There is no application-side batch waiting for a Relayer commit wrapper.

Relayer does not commit, abort, retry or dispose the session. Follow MongoDB's native transaction error and
commit retry semantics. With `session.WithTransactionAsync`, create and dispose the association **inside each
callback invocation** so a native retry replays the complete unit. Dispose an association before starting another;
an association never becomes active again if its original transaction ends and the same session starts a new one.

Await every application write and send sequentially. The native driver does not support parallel operations
inside one transaction. Dispose the association before the native session, and keep its DI scope alive through
transaction completion. Direct calls to native clients remain the application's responsibility.

Neither scheduling capability is advertised while workers remain planned. An ordering key is stored losslessly
and mapped to a lane with the shared UTF-8 FNV-1a hash. Storage and row claims alone do not establish ordered delivery.

## Stored envelopes and claims

IDs use standard BSON UUID binary representation, whose byte order preserves UUIDv7 ordering. Bodies and
ordering keys remain binary. Headers use a length-prefixed binary list, preserving duplicate names, arbitrary
binary values and empty values. Original wire identity, creation headers, destination and tenant ID survive reads.
Native callbacks and tombstones cannot be persisted.

Instant fields use signed 64-bit UTC ticks (`created_at_utc_ticks`, `deliver_at_utc_ticks`,
`claim_expires_at_utc_ticks`). This avoids losing sub-millisecond claim boundaries to BSON DateTime precision.
Read cursors, due-time predicates, atomic claims and fenced mutations follow the portable contract. Renewal never
shortens an expiry. Completion deletes only a matching live claim. Failure increments attempts once and releases
the claim. Database errors and cancellation propagate; a failed response can still follow a committed operation.
Pending and parked records do not expire by age.

The write codec reuses its body/header buffer. Native driver and BSON wrapper allocations are separate from
Relayer's encoding allocation budget. Built-in serializer constructor roots support the driver's BSON command
responses without changing process-wide serializer configuration; upstream analysis diagnostics still block release.
