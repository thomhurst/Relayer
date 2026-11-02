---
title: PostgreSQL change data capture
description: "Wake unordered outbox dispatch from native logical replication commits, with persistent slot recovery."
---

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
Relayer validates the slot and publication at host startup; it never creates, drops or resets a slot.
Native errors are logged or propagated. Errors Npgsql classifies as transient retry after `ReconnectDelay`,
including an occupied slot; restore invalidated slots through your database operations procedure.

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

Keep maintenance registered for failed sends, expired claims, future delivery times, and retention: due times
and claim expiry do not produce insert events. CDC replaces polling as the wake-up for new commits;
recovery still polls exceptional or time-dependent work. Direct commit-triggered dispatch can coexist but
is not required for CDC. Fenced claims arbitrate between workers.

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
or two-phase decoding is supported. Batch size is 1–10000; claim/reconnect durations must be positive.
Restart the host to change settings.

## Recovery and delivery

One slot supports one active reader. Deploy one active CDC worker per slot; other instances can write and
run leased maintenance. PostgreSQL enforces slot exclusivity. CDC uses row fencing instead of waiting for
the maintenance leader's idle lease.

At startup and after transient connection failures, CDC traverses pending unordered rows through a fixed
ID boundary in bounded transactions. This includes rows committed before slot creation. The existing slot
retains changes during catch-up, closing the gap before live capture. Each subsequent captured outbox
commit triggers another bounded traversal. Rolled-back writer transactions never dispatch.

The reader advances the applied/flushed position only after the traversal's database transactions commit.
Npgsql sends feedback on its native status interval. Failed sends can be checkpointed because their rows
remain durable for maintenance; database failures and cancelled dispatches are not checkpointed. Restart
can revisit rows, and claims skip rows already deleted or claimed. Delivery is **at-least-once**: broker
acceptance followed by a crash before database commit can deliver the same ID and body again. Use the
[consumer inbox](ef-core-inbox.md) for deduplication.

Monitor slot activity, `confirmed_flush_lsn`, retained WAL and disk capacity. A stopped reader retains WAL;
server retention limits can invalidate a slot. Restore capture through your database operations procedure,
keeping durable rows and maintenance available. Do not share this slot with Debezium.

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
