---
title: Unordered outbox direct dispatch
description: "Send committed PostgreSQL outbox messages immediately through named native transports."
---

The PostgreSQL direct dispatcher can send due, unordered messages in the background after a
successful commit from a configured [EF Core](ef-core-outbox.md) or [ADO.NET/Dapper](postgres-outbox.md)
outbox writer. Dispatch can start before the next recovery poll. The transaction commits independently
of the broker. A rollback never queues sends.

## Registration

Create the [schema](postgres-schema.md) first. Keep your normal `AddRelayer` routes and named
`ITransport` registrations. Register the direct dispatcher and maintenance worker with the same
application-owned `NpgsqlDataSource`, schema and table prefix. The example names both workers `orders`.

```csharp
using Microsoft.Extensions.DependencyInjection;
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
```

Direct-dispatch registration also registers the named scoped `IOutboxStore`; do not register that store
again. The host starts and stops the worker. Resolving services without starting their hosted services
does not dispatch messages. Dispose the data source only after the host has stopped.
Startup fails before workers start if direct dispatch has no maintenance worker using the same
`NpgsqlDataSource` instance, schema and table prefix. A differently named maintenance worker can recover
the same tables. Ordered dispatch alone does not recover unordered rows.
If you use `NpgsqlSlimDataSourceBuilder`, call `EnableArrays()` before `Build()`; fenced batch claim and deletion
operations use native UUID array parameters. The regular `NpgsqlDataSource` builder enables these mappings by default.

For dispatch limits from configuration, use
`services.AddRelayerPostgreSqlDirectDispatch("orders", dataSource, configuration.GetSection("Dispatch"), storeOptions)`.
Keys are `QueueCapacity`, `BatchSize` and `ClaimDuration`. Registration snapshots the values and rejects
malformed or nonpositive values. Read the same named snapshot with
`IOptionsMonitor<PostgreSqlDirectDispatchOptions>.Get("orders")`. Restart the host to change dispatch limits.

### EF Core writer

Prefer the [combined outbox registration](ef-core-outbox.md#registration), which includes direct
dispatch and recovery. If you register the workers individually as above, configure each
participating DbContext with the matching schema, table prefix and `DispatchStoreName`.

```csharp
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Relayer.Outbox.EntityFrameworkCore;

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

Names are case-sensitive. A missing dispatcher or mismatched schema or table prefix fails when
the DbContext options are built. Before committing an EF Core transaction with buffered outbox rows,
Relayer checks that its configured PostgreSQL host, port and database match the selected dispatcher.
Separate data sources with the same target are supported. Roll back the transaction and correct the
connection settings if the check fails. Omitting the name keeps enlistment-only behavior. The EF Core
options also bind this name from configuration.

Both synchronous and asynchronous [saves](ef-core-outbox.md#save-without-an-explicit-transaction)
and explicit EF transaction commits notify the worker after a successful commit.

### ADO.NET and Dapper writer

Use the [named writer registration](postgres-outbox.md#named-direct-dispatch) with
`AddRelayerPostgreSqlOutbox("orders", options)`. Use the dispatcher's case-sensitive name and
matching schema and table prefix. The unnamed writer only enqueues rows for separately registered
workers. Both `Commit` and `CommitAsync` on the
[PostgreSQL outbox transaction](postgres-outbox.md#adonet-and-dapper) notify the named worker after
the native transaction commits successfully.

## Sending and ownership

The worker takes ownership of reusable serialized buffers before the caller can reuse or dispose
its scope. It opens its own database connections for background work.

For each native group, one conditional database update acquires fresh fenced claims for eligible rows.
Competing instances cannot normally dispatch the same row. The worker excludes rows claimed by another
instance. PostgreSQL evaluates delivery eligibility, claim expiry, completion, sent-history timestamps
and failure backoff with `clock_timestamp()`, including when recovery reuses this dispatcher.
Before starting a send or retry, the worker checks monotonic elapsed time since it began claiming.
This conservative budget includes the database round trip and avoids comparing database deadlines
with the host wall clock. It can leave a live claim for recovery when the round trip consumes the budget. It preserves the stored message ID, body, native headers and
creation context; it does not serialize the application message again or generate a new envelope.
Once a stored delivery time is due, the native send requests immediate delivery. This also works with
transports such as Kafka that have no native scheduling support.

Before sending, the single reader drains already queued commit notifications into a working batch,
up to `BatchSize` rows and `QueueCapacity` notifications. It never waits to fill a batch. A notification
that does not fit stays queued for the next pass; a larger transaction keeps its existing within-commit
grouping. PostgreSQL, SQL Server and MySQL use this behavior. Business transactions remain separate,
and ordered rows never enter this path. Owned serialized buffers remain live until dispatch finishes,
even when the committing scope is reused or disposed.

Messages are grouped by named transport and destination across the collected commits. A group is bounded by `BatchSize` and the
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

Native send failures use the registered `IFailureClassifier` after native client retries finish.
`Retry` records the error, releases the live claim and persists exponential backoff in `deliver_at`,
from one second to five minutes; retryable sends remain pending beyond `MaxDispatchAttempts`.
`DeadLetter` immediately moves the fenced row to dead storage, while `Discard` completes it without
a dead copy. See [failure classification and batch isolation](postgres-store.md#poison-rows-and-key-release)
for default classification, native exception rules and partial batch handling. `last_error` stores the
exception type and message, followed by the innermost exception's type and message, for example
`System.InvalidOperationException: send failed ---> System.TimeoutException: socket timed out`, truncated
to 4,096 characters. Restrict database access as these messages can contain endpoint details.
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
worker is stopped, the worker attempts to log how many committed rows remain pending. A logging failure
leaves the commit result intact. Shutdown completes the queue and drains it within the host's shutdown
deadline. Cancellation leaves unfinished work durable.

For [PostgreSQL local consumers](../transports/postgresql-local.md), the worker also holds up to
`QueueCapacity` deferred unordered rows across saturated consumers. It retries admission when a
consumer slot opens. Overflow and unfinished deferred rows remain available for recovery.

[Ordered lane dispatch](ordered-dispatch.md) handles rows with ordering keys separately.
[Leased recovery and cleanup](postgres-maintenance.md) handles due unordered rows left by failure,
overflow, cancellation, process death or a future `DeliverAt`. Register it explicitly alongside this worker.
Local-handler retries retain the `storeOptions.MaxDispatchAttempts` poison budget (default 10).
This worker handles immediate commit-triggered sends; maintenance revisits native retries when their
persisted deadline becomes due.

## Clock assumptions

Keep database wall clocks stable and synchronize application clocks. PostgreSQL direct claim fencing
and the local transport's transactional completion use the database clock; these checks do not depend
on the application clock offset. The worker does not renew direct claims during a send. A database
clock jump can still expire a lease early, and at-least-once delivery still permits duplicates.

Creation timestamps remain application-generated. Recovery compares `created_at` with database time
and `RecoveryDelay`, so a writer clock ahead of the database can delay recovery; a clock behind can
make a row eligible sooner. Dispatch-lag telemetry also compares application timestamps. Absolute
`DeliverAt` values reflect the caller's intended UTC instant, evaluated against database time.
The low-level [PostgreSQL store](postgres-store.md) methods that accept `now` and an expiry continue
to honor those explicit instants; application-owned workers must supply a consistent clock.
