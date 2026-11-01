---
title: Ordered PostgreSQL dispatch
description: "Dispatch ordered outbox rows with lane leases, heartbeats and fair instance membership."
---

`Relayer.Outbox.PostgreSql` supplies a hosted dispatcher for ordered rows written by
[EF Core outbox enlistment](ef-core-outbox.md) or [ADO.NET/Dapper transactions](postgres-outbox.md).
[Unordered direct dispatch](direct-dispatch.md) runs separately after a business transaction commits.
Recovery polling for unordered rows, poison-row movement, cleanup, inbox deduplication and outbox tracing
remain separate planned work.

## Register

Deploy the [shared schema](postgres-schema.md), register native transports, then add the worker. Existing deployments must apply the current schema script or a new EF schema migration to add the parked-key index:

```csharp
using Relayer.Outbox.PostgreSql;

services.AddRelayerPostgreSqlOrderedDispatch("orders", dataSource, new PostgreSqlOrderedDispatchOptions
{
    Schema = "messaging",
    TablePrefix = "orders_",
    Lanes = 32,
    LeaseDuration = TimeSpan.FromSeconds(30),
    PollInterval = TimeSpan.FromSeconds(1),
    BatchSize = 100
});
```

The application owns `dataSource`. Dispose it after the host stops. Worker names are explicit and
case-sensitive; duplicate names fail immediately. Use the same database, schema, prefix and lane count
on every writer and worker. Drain the outbox before changing the lane count. Distinct stores have separate
membership and leases. Two worker names pointing to the same tables join the same group.

Options are init-only and available through `IOptionsMonitor<PostgreSqlOrderedDispatchOptions>.Get("orders")`.
An overload accepts `IConfiguration`, for example `configuration.GetSection("Outbox:Orders")`, with explicit
Native AOT-safe binding. Settings are snapshotted at registration; restart to apply changes. Lane counts
must be 1–65,536, batch sizes 1–10,000, lease durations one second to one hour, and poll intervals positive
and less than one third of the lease duration. Invalid settings fail registration.

Normal route registration validates native ordering support. Persisted rows referencing missing adapters,
missing ordering keys or endpoints without `OrderedByKey` fail dispatch with actionable errors. Destination
capabilities are cached after first use; changing topology requires restarting workers.

## Ownership and order

The writer hashes each UTF-8 ordering key with FNV-1a modulo the lane count. Worker membership uses
`member:<unique instance ID>` rows in `relayer_leases`. Members heartbeat using the database clock. Sorted
live members divide the lanes by rank, differing by at most one lane. Members stop renewing lanes outside
their share; a new owner waits for expiry. Graceful shutdown releases membership and owned lanes. A crash
leaves leases to expire. Expired membership rows are removed during heartbeats.

A worker scans its assigned lanes and sends one bounded batch per lane per cycle. After any committed progress it immediately starts another cycle, including when the time budget shortened a batch. PollInterval applies only to scans with no progress, so a backlog is not limited to one batch per polling interval. Each lane lease has
an instance-specific owner token. Its database row stays locked in a transaction until native sends and
fenced row outcomes finish. A competing owner cannot take over while that transaction remains connected,
even after lease expiry. The worker sets a deadline of half the lease duration for acquiring, reading,
sending and committing each batch. It stops admitting rows after one quarter of the lease duration, reserving the remaining deadline for the final admitted send and commit. Slow healthy batches therefore commit a confirmed prefix instead of repeatedly replaying it. A single send must still fit the configured hard deadline. It awaits cancellation completion before freeing borrowed payloads.

Reads follow the partial `(lane, id)` pending index, in PostgreSQL UUID order. Scheduling and existing claims
never cause the worker to skip a head row. A future or live-claimed head temporarily stalls its lane.
Later rows with a parked head's ordering key stay parked while other keys can continue, using the parked-key
index. This worker does not itself move poison rows or expose redrive management yet.

Messages are sent sequentially through the registered native `ITransport`, preserving original bodies,
headers, identities and ordering keys. Due delivery timestamps are consumed by the database rather than
scheduled again in the broker. Successful rows are deleted with their claim tokens; failures persist attempts
and the dispatch error and stop the batch. Preparation failures, including missing adapters, missing ordering keys
and unsupported native ordering, use the same durable failure path. Completed rows before the failure commit,
so a bad row does not repeatedly replay a successful prefix. Database or completion uncertainty rolls back the batch, retaining
rows for retry. Up to `BatchSize` accepted sends can replay when their batch does not commit. Unordered rows are untouched.

## Failure boundaries

Ordering means ascending IDs among rows visible to a lane scan, using the broker's own key-ordering semantics.
UUIDv7 IDs do not establish commit order between concurrent business transactions. Applications needing a
business order across concurrent writers must serialize those writes.

Delivery remains at-least-once. A crash after the broker accepts a message but before database commit replays
that identity. Cancellation and database fencing cannot retract a native request already accepted by the
broker. If a database connection dies while a native request remains in flight, its duplicate can arrive after
a successor's sends. The protocol prevents overlapping connected database owners, but does not promise
exactly-once delivery or fence native broker requests. Consumers needing duplicate suppression must use
idempotent processing; the built-in inbox remains planned. A transport must observe cancellation for prompt
shutdown. A stalled connected transaction deliberately delays takeover until it ends.
