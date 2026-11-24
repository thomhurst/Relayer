---
title: PostgreSQL change data capture
description: "Wake unordered outbox dispatch from native logical replication commits, with persistent slot recovery."
---

:::warning Preview
PostgreSQL change capture is preview and outside the first stable release compatibility promise. Using its API requires explicit compile-time opt-in by suppressing [RLYEXP003](../reference/diagnostics.md#rlyexp003) (for example, add `<NoWarn>$(NoWarn);RLYEXP003</NoWarn>` to your project).
:::


`Relayer.Outbox.PostgreSql` can dispatch committed unordered messages from PostgreSQL logical replication,
without waiting for the [maintenance poll](postgres-maintenance.md). It uses Npgsql's native `pgoutput`
reader and the existing fenced recovery/send pipeline. The outbox table layout does not change.
SQL Server CDC is not implemented.

## Prerequisites

Deploy the [outbox schema](postgres-schema.md). Enable PostgreSQL `wal_level = logical`, reserve a
replication slot and WAL sender, and grant the replication login `REPLICATION` and database/table access.
The ordinary data source needs the same read/write privileges as recovery dispatch. Provision an INSERT-only
publication and a **persistent** `pgoutput` slot in the writer's database before starting the host:

```sql
CREATE PUBLICATION orders_cdc FOR TABLE messaging.orders_outbox
    WITH (publish = 'insert');
SELECT * FROM pg_create_logical_replication_slot('orders_cdc', 'pgoutput');
```

Do not use temporary slots, row filters, or publications that include updates, deletes or truncates.
INSERT-only capture avoids feeding dispatch's claim, failure and deletion writes back into dispatch.
Relayer validates the slot and publication at host startup. Provisioning, replacement and reset stay
with database operations. An invalidated slot fails startup validation. If the slot becomes invalid
while the host runs, revalidation faults the worker before reconnecting. An occupied healthy slot
and other errors Npgsql classifies as transient retry after `ReconnectDelay`.
Permanent database, replication or configuration errors fault the CDC worker. With the default
`BackgroundServiceExceptionBehavior.StopHost`, the host logs the error and stops the application.
If you configure `Ignore`, keep maintenance available, repair the cause and restart the host.

## Registration

Register named native transport adapters and the writer as usual. Add CDC using native Npgsql settings:

```csharp
using Npgsql.Replication;
using Npgsql.Replication.PgOutput;
using Relayer.Outbox.PostgreSql;

services.AddRelayerPostgreSqlChangeCapture("orders", dataSource,
    () => new LogicalReplicationConnection(connectionString),
    new PostgreSqlChangeCaptureOptions
    {
        Slot = new PgOutputReplicationSlot("orders_cdc"),
        Replication = new PgOutputReplicationOptions("orders_cdc", PgOutputProtocolVersion.V1),
        Schema = "messaging",
        TablePrefix = "orders_",
        BatchSize = 100,
        ClaimDuration = TimeSpan.FromSeconds(30),
        ReconnectDelay = TimeSpan.FromSeconds(5)
    });

services.AddRelayerPostgreSqlMaintenance("orders", dataSource,
    new PostgreSqlMaintenanceOptions { Schema = "messaging", TablePrefix = "orders_" });
```

The factory must return a new replication connection to the **same database** as the writer and data source.
The worker owns and disposes each replication connection, including on reconnect. The data source remains
application-owned. Replication uses a separate native connection outside the ordinary pool; a
single-connection pool can serve recovery transactions. Stop the host before disposing the data source.

Keep maintenance registered for failed sends, expired claims, future delivery times, retention, and
[rows that become visible after their CDC dispatch pass](#when-rows-become-visible-after-cdc).

Direct commit-triggered dispatch can coexist but is not required for CDC. Fenced claims arbitrate between workers.

Ordered rows remain exclusively with the [ordered lane dispatcher](ordered-dispatch.md). CDC never bypasses
lane ownership or sends ordered rows. Register ordered dispatch for routes using lanes. Native transport
grouping, batch limits, scheduling decisions and telemetry are inherited from recovery dispatch.

To bind store and retry properties from configuration, pass native slot/options separately:

```csharp
services.AddRelayerPostgreSqlChangeCapture("orders", dataSource,
    () => new LogicalReplicationConnection(connectionString),
    new PgOutputReplicationSlot("orders_cdc"),
    new PgOutputReplicationOptions("orders_cdc", PgOutputProtocolVersion.V1),
    configuration.GetSection("ChangeCapture"));
```

Settings are snapshots, available through `IOptionsMonitor<PostgreSqlChangeCaptureOptions>.Get("orders")`.
Do not mutate native replication options after registration. Only protocol V1 without transaction streaming
or two-phase decoding is supported. Batch size is 1–10000 and claim duration must be positive.
`ReconnectDelay` defaults to 5 seconds. Registration accepts delays from 1 to 4,294,967,294 milliseconds.
Restart the host to change settings.

## Recovery and delivery

One slot supports one active reader. Deploy one active CDC worker per slot; other instances can write and
run leased maintenance. PostgreSQL enforces slot exclusivity. CDC uses row fencing instead of waiting for
the maintenance leader's idle lease.

Hosted CDC waits for `IHostApplicationLifetime.ApplicationStarted` before opening its replication
connection or dispatching rows. This gives hosted transports time to finish startup even when services
start concurrently. On the initial connection and after transient connection failures, CDC traverses
pending unordered rows through a fixed ID boundary in bounded transactions. This includes rows committed
before slot creation. The existing slot retains changes during catch-up, closing the gap before live
capture. Each subsequent captured outbox commit triggers another bounded traversal. Rolled-back writer
transactions never dispatch.

The reader advances the applied and flushed positions after all dispatch transactions for a captured
commit have completed. Npgsql sends feedback on its native status interval. Failed sends can be
checkpointed because their rows remain durable for maintenance. Database failures and cancelled
dispatches preserve the earlier checkpoint. Restart can revisit rows, and claims skip rows already
deleted or claimed. Delivery is **at-least-once**. Broker acceptance followed by a crash before
database commit can deliver the same ID and body again. Use the [consumer inbox](ef-core-inbox.md)
for deduplication.

Monitor slot activity, `confirmed_flush_lsn`, retained WAL and disk capacity. A stopped reader retains WAL;
server retention limits can invalidate a slot. Restore capture through your database operations procedure,
keeping durable rows and maintenance available. Do not share this slot with Debezium.

Dispatch runs inline on the replication read loop. A slow or unavailable broker delays reading later
commits and advancing feedback, so retained WAL can grow during a send stall. Npgsql continues its native
status timer, but that does not advance the applied/flushed position past unfinished dispatch. Size WAL
retention for broker outages and monitor disk capacity.

### When rows become visible after CDC

PostgreSQL can send a decoded commit before a new SQL snapshot on the primary can see the inserted rows.
This window can occur without synchronous replication and can last longer while the writer waits for a
synchronous standby. [PostgreSQL bug #19733](https://www.postgresql.org/message-id/flat/19733-5fcd39e85fa221a4%40postgresql.org)

If the dispatch pass misses a row, CDC can acknowledge the commit while the row remains pending in the
outbox. A later captured outbox commit can trigger another pass. Idle checkpoint markers only advance
replication feedback. When there are no more outbox inserts, [maintenance](postgres-maintenance.md#recovery-and-ownership)
picks up the row on a sweep after it becomes visible, is due, and reaches the minimum age set by
`RecoveryDelay` (30 seconds by default, measured from `created_at`). An idle worker waits `SweepInterval`
(5 seconds by default) between scans. Sweep progress and send time also affect delivery latency.

### Checkpoint during periods without outbox inserts

For a database that generates WAL between outbox inserts, enable checkpoint markers in the CDC options:

```csharp
Replication = new PgOutputReplicationOptions("orders_cdc", PgOutputProtocolVersion.V1, messages: true),
IdleCheckpointInterval = TimeSpan.FromSeconds(30),
```

Set both options. `IdleCheckpointInterval` defaults to zero, which disables markers. When using the
configuration overload, set `IdleCheckpointInterval` to a duration such as `00:00:30` and pass
`messages: true` in the native options. The writer data source must be able to execute
`pg_catalog.pg_logical_emit_message`.

At each interval the worker commits a small logical message in the writer's database. The reader
checkpoints its commit in stream order after the dispatch passes triggered by earlier outbox commits
have finished. Allow for the checkpoint interval, Npgsql's feedback interval and any time spent waiting
for dispatch when monitoring lag.

## Debezium event router

The unchanged schema supports the [Debezium event router](https://debezium.io/documentation/reference/stable/transformations/outbox-event-router.html)
with explicit column mappings, rather than its default column names. Map `table.field.event.id=id`,
`table.field.event.key=ordering_key`, `table.fields.additional.placement=message_type:header:type`, `table.field.event.payload=body`,
`table.field.event.timestamp=created_at`, and `route.by.field=destination`. Relayer's binary `headers` field
is an encoded envelope, not a JSON header object. Binary payload encoding and header projection require
connector-specific configuration or transformation.

Use a separate connector slot and choose **one** delivery owner. Debezium delivery alongside Relayer dispatch
can duplicate sends; Relayer deletes successful rows, so configure Debezium's handling of deletes/tombstones.
Debezium does not implement Relayer's ordered lane leases, claim protocol, future-due scheduling or native
transport sends. Validate those contracts separately when choosing it.
