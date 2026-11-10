---
title: EF Core consumer inbox
description: "Deduplicate deliveries and commit handler changes and outgoing messages in one PostgreSQL transaction."
---

`Relayer.Outbox.EntityFrameworkCore` supplies a database boundary for single-message consumers using
the Npgsql EF Core provider. Each handling attempt owns one transaction. An inbox insert claims
`(consumer, message_id)` before the handler runs. The insert, saved business changes and any outgoing
outbox rows commit together after the handler and all inner middleware finish. Native settlement
still runs through the consume pipeline after commit and asynchronous scope disposal.

Create the [PostgreSQL schema](postgres-schema.md) first. Register an ordinary scoped DbContext and
the [EF Core outbox](ef-core-outbox.md), using the same scoped provider as the handler's `ISender`.
Compose the consumer explicitly at startup:

```csharp
using Relayer.Outbox.EntityFrameworkCore;

var pipeline = new ConsumePipelineBuilder(scopeFactory, messageTypes, serializer, failures)
    .Handler(handlers.GetHandler<OrderPlaced, OrderPlacedHandler>())
    .EntityFrameworkCore<OrdersDbContext>("orders.billing")
    .Use(otherMiddleware)
    .Settle(SettleNativeMessageAsync)
    .Build();
```

`EntityFrameworkCore<TContext>` must be the first middleware registration, so the transaction
surrounds all subsequent middleware. The selected context comes from the delivery's DI scope;
no assembly scanning or handler dependency inspection occurs. Its identity is an application-declared
consumer name, distinct from the message type. Keep the name stable across restarts and replicas.
Different consumer names can process the same message independently.

When that context has `AddRelayerOutbox`, the inbox is enabled automatically and uses that context's
schema and table prefix. Outbox registration on another context does not enable it. `SendAsync`
joins the active transaction; the handler calls `SaveChangesAsync` as usual. Several saves stay in
the same transaction. The middleware commits after the handler returns, including when handling
produces no outgoing messages. To send due unordered rows after commit, configure
[unordered direct dispatch](direct-dispatch.md) on the selected outbox context. Without a dispatcher,
committed rows remain pending.

## Inbox without an outbox

For a scoped Npgsql DbContext without `AddRelayerOutbox`, opt in explicitly on that consumer:

```csharp
builder.EntityFrameworkCore<OrdersDbContext>("orders.billing", inbox: true);
```

Without `inbox: true`, this declaration still owns the transaction but does not deduplicate.
An optional `EntityFrameworkCoreOutboxOptions` argument selects the schema and prefix for an
inbox-only context. With an outbox, configure those options on `AddRelayerOutbox` instead; its
scoped configuration is authoritative, including configuration reloads for newly created scopes.

## Transaction isolation

Omitting `isolationLevel` keeps the provider default (normally Read Committed on PostgreSQL).
Pass the native `System.Data.IsolationLevel` when the handler needs a stronger invariant:

```csharp
builder.EntityFrameworkCore<OrdersDbContext>("orders.billing",
    isolationLevel: System.Data.IsolationLevel.Serializable);
```

With an explicit isolation level and no configured retrying EF execution strategy, Relayer uses
EF Core's native execution strategy to retry PostgreSQL serialization failures (`40001`) and
deadlocks (`40P01`) immediately, up to three retries per handling attempt. These conflicts can
originate in the inbox insert, handler, save or commit. Each replay runs the complete handler and
inner middleware in a fresh DI scope, DbContext and transaction. Conflicts within that budget do
not consume the ordinary failure policy's retries or dead-letter budget. Exhaustion raises EF's
`RetryLimitExceededException` into normal consumer failure handling; each ordinary retry gets a
new conflict budget. Other errors go directly to normal failure handling.

At Repeatable Read or Serializable, a duplicate inbox insert waiting on a transaction that commits
after its snapshot can raise `40001` instead of returning zero rows. A fresh attempt sees the
committed inbox row and skips the handler.

If the application configures a retrying EF execution strategy (for example `EnableRetryOnFailure`),
that strategy remains authoritative for classification, retry count and delays, including conflicts.
The default provider strategy classifies both conflict codes as transient. Relayer does not add a
second retry budget around it. With neither an explicit isolation level nor a retrying strategy,
the original transaction path and normal consumer failure policy are unchanged.

## Redelivery and failure

The original message ID must be a non-empty UUIDv7. Invalid identities enter normal consume failure
handling before invoking the handler or writing to the database. PostgreSQL's unique constraint
coordinates concurrent instances: a second insert waits for the first transaction. If the first
commits, the duplicate skips the handler and completes normally. If it rolls back, the waiting
delivery inserts its own inbox row and runs the handler.

Handler errors, failed saves, failed outbox writes and cancellation roll back the transaction before
normal failure handling. A requested `ISettlementFeature.Retry` or `DeadLetter` also rolls back
business changes, inbox and outbox; `Complete` commits. `RequestedDisposition` exposes the pending
request to transaction middleware; custom `ISettlementFeature` implementations must supply this
property. Make settlement requests before the handler and inner middleware
return. Before committing, the boundary requests `Complete` when no outcome is pending, including
for duplicates. The one-request-per-attempt rule then rejects further requests during scope disposal
with `InvalidOperationException`; disposal cannot retry or dead-letter already committed work. A
disposal error prevents native settlement, and redelivery acknowledges the committed duplicate.
This also applies with native EF retries enabled: cleanup errors propagate without retrying the
transaction or entering the consumer handler-failure policy, even when the error is transient.
An acknowledgement failure after commit leaves durable data
intact, and redelivery skips the handler. Commit failures with an unknown outcome remain safe to
redeliver: the committed inbox row determines whether the handler runs again.

Each suppressed duplicate records `relayer.inbox.duplicates` on the registered `Relayer` meter,
with the stable consumer name under `relayer.consumer`. Message IDs are never metric dimensions.

## EF Core connection retries

Contexts configured with `EnableRetryOnFailure` are supported, including contexts shared with HTTP
request handlers. Relayer runs the complete handler and inner middleware through EF Core's native
execution strategy. Each transient-fault retry uses a fresh DI scope, DbContext and transaction;
failed-attempt business writes, inbox rows and outgoing rows roll back together. Configure the native
provider's retry count and delays as usual. Non-transient handler errors still follow
[Relayer's consumer failure policy](../guides/failure-handling.md).

When a transient commit error leaves its outcome unknown, the strategy checks the attempted inbox
key on a fresh connection. A committed key completes processing without replaying the handler;
buffered direct sends are retained and flushed by the outer failure pipeline. Each native attempt
uses the outbox configuration of its fresh scoped DbContext, including reloaded schema and prefix.
Otherwise the strategy retries the complete unit. This verification requires the inbox, either
implicit through `AddRelayerOutbox` or explicit through `inbox: true`. Without it, unknown commits can
replay business work. Keep external side effects idempotent even with an inbox: database rollback
cannot undo an HTTP call.

The provider-default path without configured retries retains its original transaction middleware. Retrying contexts additionally
use native strategy callbacks and a fresh scope per attempt.

## PostgreSQL Serializable isolation

The real-PostgreSQL `SerializableIsolationTests` test uses the consumer's
`isolationLevel: IsolationLevel.Serializable` option and checks the server's actual isolation level before the inbox
insert. With 16 concurrent writers in 32 synchronized batches, distinct UUIDv7 inbox keys and one
outgoing row per transaction, all 512 attempts committed: zero `40001` serialization failures and
zero `40P01` deadlocks, matching the 512-attempt control without inbox/outbox statements. Direct
dispatch, recovery sweeps and retention deletes operate while writer transactions remain open.
A transaction-start counter rejects replay, so conflict retries cannot mask a failed attempt.
See the [outbox measurement](ef-core-outbox.md#postgresql-serializable-isolation) for versions,
method and the other enqueue paths.

This bounded result for unrelated business rows and distinct inbox keys is not a guarantee against
all Serializable conflicts. In particular, a duplicate inbox key committed after a Serializable
snapshot can make `INSERT ... ON CONFLICT DO NOTHING` raise `40001`, rather than return zero rows.
That case requires a fresh whole-transaction attempt which sees the committed inbox row. Application
read/write dependencies can also cause failures. The [isolation option](#transaction-isolation)
replays these conflicts within a bounded budget in fresh scopes; the provider-default path remains
Read Committed and does not opt into conflict-specific replay.

## Boundaries

Use one database boundary per consumer. The middleware owns transaction completion: handlers must
not commit, roll back, dispose or replace its transaction, use a different database connection for
their durable work, or settle the native message themselves. Save changes in the selected context;
Relayer does not call `SaveChanges` on your behalf. External HTTP calls and direct broker sends are
outside the database transaction and need their own idempotency.

Context pooling, ambient transactions and batch consumers are unsupported. Composition rejects
pooled contexts and other providers. Both EF Core transient retries and Relayer consumer retries
run whole attempts in fresh scopes and DbContexts. Register scoped services for the lifetime of a delivery;
do not share a scope concurrently. Configure [inbox retention and cleanup](postgres-maintenance.md)
to bound retained history; deleting a row ends its deduplication protection.
