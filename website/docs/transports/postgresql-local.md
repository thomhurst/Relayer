---
title: Durable local work (PostgreSQL)
description: "Run durable work in the same deployment using PostgreSQL outbox rows and native transactions."
---

:::warning Preview
PostgreSQL local transport is preview and outside the first stable release compatibility promise. Using its API requires explicit compile-time opt-in by suppressing [RLYEXP002](../reference/diagnostics.md#rlyexp002) (for example, add `<NoWarn>$(NoWarn);RLYEXP002</NoWarn>` to your project).
:::


Use `PostgreSqlLocal` for follow-up steps, cleanup and other work that must survive crashes but belongs to the same deployment. Use a broker for independently deployed consumers, subscriptions, replay or native delivery controls. Use in-memory queues when losing queued work on shutdown is acceptable.

The transport lives in `Relayer.Outbox.PostgreSql` and reuses the existing outbox tables, fenced claims, direct dispatch, leased recovery, ordered lanes and dead-letter manager. Deploy the [schema](../reference/postgres-schema.md) before starting the host.

## Register routes and workers

```csharp
using Npgsql;
using Relayer;
using Relayer.Outbox.PostgreSql;

var source = NpgsqlDataSource.Create(connectionString); // application-owned
services.AddRelayer(builder =>
{
    builder.AddMessageTypesFromThisAssembly();
    builder.PostgreSqlLocal("local", source, transport =>
    {
        transport.Send<Cleanup>("cleanup");
        transport.Consume("cleanup", consumer => consumer.Handler<CleanupHandler>());
    });
});
services.AddRelayerPostgreSqlOutbox("local");
services.AddRelayerPostgreSqlDirectDispatch("local", source);
services.AddRelayerPostgreSqlMaintenance("local", source);
services.AddRelayerPostgreSqlDeadLetters("local", source);
```

`AddRelayerPostgreSqlOutbox("local")` selects the direct-dispatch store for commit notifications. `PostgreSqlLocal` defaults to a worker matching its transport name. Pass `dispatchStoreName:` to select another named worker.

Use the same `NpgsqlDataSource` instance, schema and table prefix for the local transport and its workers. Startup checks the selected direct-dispatch worker and also rejects workers on a separate data source when the configured host, port, database, schema and table prefix match the local transport. Host aliases can point to the same server and still compare as different hosts. Before workers start, Relayer checks that direct dispatch, maintenance and ordered dispatch for those tables agree with the local transport on `MaxDispatchAttempts` and `RetainSentMessages`. Registration names can differ.

To enable [sent-message history](../reference/postgres-maintenance.md#sent-message-history), set `RetainSentMessages = true` in the `PostgreSqlOutboxOptions` passed to `PostgreSqlLocal` and in the matching store and worker options.

Every dispatching instance must declare the same local consumers. Instances compete for work; consumers are not broadcast subscriptions.

Relayer registers local receivers after all consumer pipelines for that transport compose and validate successfully. If pipeline composition fails, `StartAsync` surfaces the error. Once the cause is cleared, you can retry startup on the same host.

For keyed routes, use `route.OrderingKey(message => message.CustomerId)` and register `AddRelayerPostgreSqlOrderedDispatch("local", source)`. Set the same `Lanes` on the local transport, outbox writer and ordered dispatcher. Relayer checks lane counts at startup for the ordered dispatcher and an ADO.NET writer configured with a dispatch store name. Relayer checks an unnamed ADO.NET writer when it begins an outbox transaction and an EF Core writer when its DbContext enlists. The ordered dispatcher preserves insertion order and parks poisoned keys until redrive or purge. Other keys continue. Register maintenance even with direct dispatch to recover after crashes, missed notifications, queue pressure and failed attempts.

## Enqueue atomically

Resolve `PostgreSqlOutbox` and `ISender` from the same scope:

```csharp
await using var transaction = await outbox.BeginTransactionAsync(source,
    cancellationToken: cancellationToken);
// Use transaction.Connection and transaction for business commands.
await sender.SendAsync(new Cleanup(customerId), cancellationToken: cancellationToken);
await transaction.CommitAsync(cancellationToken);
```

Rollback also rolls back the enqueue. Successful commit hands due unordered rows to the shared direct dispatcher. [EF Core enlistment](../reference/ef-core-outbox.md) can use the same worker.

## Enqueue standalone work

With the outbox registration above, `SendAsync` requires an active outbox transaction. To enqueue standalone work from a controller or background service, use `SendWithoutOutboxAsync`.

```csharp
await sender.SendWithoutOutboxAsync(new Cleanup(customerId), cancellationToken);
```

Without an outbox registration, `SendAsync` also works for standalone local work. The local transport commits the row in its own PostgreSQL transaction before the send returns. The handler runs separately. Use the outbox transaction above when the row and application writes must commit together. See [Upgrade outbox sends](../migration/outbox-sends.md) for the send API rules.

CloudEvents envelopes with the original UUIDv7 ID and stable type are required. Raw envelopes, native callbacks and tombstones are unsupported.

## Complete work atomically

```csharp
using Npgsql;
using Relayer;
using Relayer.Outbox.PostgreSql;
using Relayer.Transports;

public sealed class CleanupHandler : IHandler<Cleanup>
{
    public async ValueTask HandleAsync(Cleanup message, MessageContext context,
        CancellationToken cancellationToken)
    {
        var delivery = context.Features
            .GetRequired<NativeMessage<PostgreSqlLocalDelivery>>().Message;
        await using var command = new NpgsqlCommand(
            "DELETE FROM expired_sessions WHERE customer_id = @customer",
            delivery.Transaction.Connection, delivery.Transaction);
        command.Parameters.AddWithValue("customer", message.CustomerId);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }
}
```

`PostgreSqlLocalDelivery` exposes the row's claim, attempt number, original message and native `NpgsqlTransaction`. Use this transaction for business writes. Fenced deletion and business work commit together. Lost or expired fences roll back business work. Never complete, dispose or retain the delivery transaction yourself: it may belong to a shared recovery pass or ordered lane batch.

Atomic completion covers only writes in that transaction and database. HTTP calls, files and independent connections remain at-least-once effects.

## Complete work at least once

Atomic completion keeps a pooled connection and an open transaction for the whole handler run, so local concurrency is bounded by the connection pool, and long transactions hold back vacuum. Handlers that do not write through the delivery transaction, such as notifications, webhooks or realtime fan-out, can opt out per consumer:

```csharp
transport.Consume("notifications", consumer =>
{
    consumer.Options.Completion = PostgreSqlLocalCompletion.AtLeastOnce;
    consumer.Handler<SendNotificationHandler>();
});
```

With `AtLeastOnce`, the dispatcher claims the row with a lease in a short transaction and commits it. The handler then runs with no open delivery transaction or held connection, and a second short statement fenced by the claim token deletes the row, or records its retry or poison transition. This works for direct dispatch, recovery passes and ordered lanes. A recovery pass commits and returns its connection before it runs such rows, so a slow handler lengthens that maintenance turn. An ordered lane commits its lease before each such row and renews it afterwards, so rows with the same key still run one at a time in order while the lease and claim are unexpired.

The handler's own writes are not rolled back when it fails, and a crash or a lost claim after the handler finishes delivers the row again, so make the handler idempotent. When the claim expires before completion, for example because the handler ran longer than the direct dispatch `ClaimDuration` or the ordered `LeaseDuration`, the completion fails, the error is logged, and the row stays for redelivery. A database error during completion is handled the same way. Neither counts as a failed attempt, because the handler already succeeded. An ordered lane stops its turn instead of moving past the row, and a recovery pass reports the database error, so change capture does not checkpoint past it. Reading `delivery.Transaction` in this mode throws an `InvalidOperationException`; open your own connection for business writes. `Atomic` stays the default.

## Failures and capabilities

The shared consume pipeline runs generated handlers, deserialization, tracing and the registered `IFailureClassifier`. A `Retry` failure counts one attempt in the row and makes it due again. Maintenance or ordered dispatch picks it up on its next pass. At the attempt limit the row is dead-lettered, or its ordering key is parked. Permanent failures use the same fenced poison transition immediately. `Discard` deletes the row. Both `Retry` and `Discard` roll back writes made through the `Atomic` delivery transaction. Independently committed writes and other side effects can persist, including in `AtLeastOnce` mode. Make those effects idempotent.

Configure `MaxDispatchAttempts` consistently on the store and workers. Inspect, redrive and purge with the existing [dead-letter manager](../reference/postgres-store.md#poison-rows-and-key-release). Each failed attempt logs one Error from the consume pipeline (category `Relayer.Pipeline.ConsumePipeline`, event ID 1, described in [logging](../guides/telemetry.md)); the direct and ordered dispatchers do not log the handler failure again when they record the retry or poison transition.

### Retry schedule per consumer

By default a local consumer retries without delay and uses the store's `MaxDispatchAttempts`. Set a schedule per consumer on `consumer.Options`. The delay and attempt count are stored in the row by the same fenced transition, so they survive restarts and apply on every dispatching instance:

```csharp
builder.PostgreSqlLocal("local", source, transport =>
{
    // A few quick retries, then hours, then dead-letter after 10 attempts.
    transport.Consume("notifications", consumer =>
    {
        consumer.Options.RedeliveryDelays =
            [TimeSpan.FromSeconds(5), TimeSpan.FromSeconds(30), TimeSpan.FromHours(1), TimeSpan.FromHours(6)];
        consumer.Options.MaxAttempts = 10;
        consumer.Handler<SendNotificationHandler>();
    });
    // Safe-to-repeat reconciliation: 1 minute, 10 minutes, then hourly until it succeeds.
    transport.Consume("reconcile", consumer =>
    {
        consumer.Options.RedeliveryDelays =
            [TimeSpan.FromMinutes(1), TimeSpan.FromMinutes(10), TimeSpan.FromHours(1)];
        consumer.Options.MaxAttempts = int.MaxValue;
        consumer.Handler<ReconcileHandler>();
    });
});
```

`RedeliveryDelays[n]` is the delay after the attempt numbered n + 1 fails, and the last delay repeats. PostgreSQL intervals have microsecond precision. Both direct and ordered dispatch round fractional microseconds up to the next whole microsecond when setting the retry deadline.

`MaxAttempts` overrides `MaxDispatchAttempts` for that consumer; `int.MaxValue` never dead-letters a `Retry` failure, but a `DeadLetter` classification still dead-letters at once. Negative delays and a non-positive `MaxAttempts` fail startup. A delayed ordered row blocks later rows with its key until it succeeds. Declare the same schedule on every dispatching instance. `consumer.Failures(...)` is rejected for local consumers, because its in-process retries and copy-based redelivery cannot take part in the fenced row transition. Watch rows that keep retrying with the `relayer.outbox.dispatch_lag` histogram and the `attempts` and `last_error` columns.

Send and completion state pools grow to retain peak concurrent usage, including beyond their initial 16 send and 64 completion slots. After warm-up, higher concurrency reuses those states; native Npgsql and serializer allocations still apply. Retained send resources are disposed with the transport.

`Concurrency` bounds active attempts per consumer in each process. With hosted direct dispatch and `AtLeastOnce`, a concurrency setting above one lets independently committed unordered rows for the same consumer run concurrently. Direct dispatch shares the admission limit with recovery and ordered dispatch. When a consumer is full, hosted direct dispatch holds its deferred unordered rows unclaimed and retries admission when a slot opens. Other destinations can continue. Each direct worker holds up to `QueueCapacity` deferred rows in addition to its commit queue. Overflow and rows still deferred at shutdown stay durable for recovery.

Batch handlers are rejected at startup. Shutdown wakes idle maintenance and ordered dispatch immediately and drains in-flight local work; a shutdown deadline still bounds draining.

Native settlement/redelivery overrides, pause/resume, replay and native scheduled sends are unsupported. Use transactional outbox scheduling with the existing maintenance/ordered workers. Capability requirements are validated at startup. Local routes support ordering by key. Standalone sends commit their own PostgreSQL transaction, so startup rejects routes that require `SendTransactionRequirement.Native`. Use `SendTransactionRequirement.Outbox` and [enlist in the business transaction](#enqueue-atomically) for atomic writes and sends. Retries and poison handling belong to the shared outbox protocol. Consumed-message counts, process duration and critical time use the shared `Relayer` meter with `postgresql` and the declared destination as dimensions. Send, process and completion spans use the application's `Relayer` source and `postgresql` messaging system.

A handler can request `Complete()` through [`ISettlementFeature`](../guides/failure-handling.md#manual-settlement). On a PostgreSQL local endpoint, `Retry(delay)` and `DeadLetter(reason)` throw `NotSupportedException` at the request call, so the handler can catch the exception and finish its work.

Direct follow-up sends made through the handler scope’s `ISender` are buffered until the handler and async scope disposal succeed. A failed attempt discards those sends, including sends made during disposal. Successful direct sends flush before completion and commit independently of the delivery transaction.

To commit business writes and follow-up sends together, begin an outbox transaction on a separate connection. Use it for the business writes and call `SendAsync` from the same scope to enlist the follow-up. The worker still completes the original row in its own transaction, so a failure after the outbox commit can replay the original delivery. Make the application work safe to repeat.

`PostgreSqlLocalDelivery.Transaction` is owned by the worker. The ADO.NET outbox starts a new transaction, so calling `BeginTransactionAsync` on the delivery connection fails. Attaching the delivery transaction with EF Core's `UseTransactionAsync` also cannot enlist follow-up sends for the worker's commit. The worker commits the underlying Npgsql transaction directly, bypassing [EF Core's outbox commit interceptors](../reference/ef-core-outbox.md#explicit-transactions).
