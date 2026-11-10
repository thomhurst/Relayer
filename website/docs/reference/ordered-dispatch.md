---
title: Ordered PostgreSQL dispatch
description: "Dispatch ordered outbox rows with lane leases, heartbeats and fair instance membership."
---

`Relayer.Outbox.PostgreSql` supplies a hosted dispatcher for ordered rows written by
[EF Core outbox enlistment](ef-core-outbox.md) or [ADO.NET/Dapper transactions](postgres-outbox.md).
[Unordered direct dispatch](direct-dispatch.md) runs separately after a business transaction commits.
[Maintenance](postgres-maintenance.md) supplies unordered recovery and retention cleanup;
[consumer inbox deduplication](ef-core-inbox.md) and outbox tracing are also implemented.
Poison-row movement remains planned.

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
When using `NpgsqlSlimDataSourceBuilder`, call `EnableArrays()` before `Build()` so the assigned-lane
probe can send PostgreSQL integer arrays, including under Native AOT. Standard data sources enable arrays by default.

Options are init-only and available through `IOptionsMonitor<PostgreSqlOrderedDispatchOptions>.Get("orders")`.
An overload accepts `IConfiguration`, for example `configuration.GetSection("Outbox:Orders")`, with explicit
Native AOT-safe binding. Settings are snapshotted at registration; restart to apply changes. Lane counts
must be 1–65,536, batch sizes 1–10,000, lease durations one second to one hour, and poll intervals positive
and less than one third of the lease duration. Invalid settings fail registration.

Normal route registration validates native ordering support. Persisted rows referencing missing adapters,
missing ordering keys or endpoints without `OrderedByKey` fail dispatch with actionable errors. Destination
capabilities are retained for at most 256 successful `(transport, destination)` lookups per worker,
shared across its lanes. FIFO eviction removes the oldest inserted entry; hits do not change eviction
order. Before sending to an evicted destination again, the worker resolves and validates its native
`OrderedByKey` capability. Failed lookups and endpoints without native ordering are never cached and
can recover on a later attempt without restarting the worker. Retained entries remain snapshots;
restart workers after changing topology rather than relying on eviction to refresh them.

This bounds retained endpoint metadata when tenant destinations change. Working sets larger than
256 entries can cause repeated native capability lookups, including any allocations performed by
the native client. There is no cache setting. Eviction does not change lane ownership, durable claims
or the broker's ordering and send semantics.

## Ownership and order

The writer hashes each UTF-8 ordering key with FNV-1a modulo the lane count. Worker membership uses
`member:<unique instance ID>` rows in `relayer_leases`. Members heartbeat using the database clock. Sorted
live members divide the lanes by rank, differing by at most one lane. Members stop renewing lanes outside
their share; a new owner waits for expiry. Graceful shutdown releases membership and owned lanes. A crash
leaves leases to expire. Expired membership rows are removed during heartbeats.

Membership refreshes after one third of `LeaseDuration` has elapsed, including between slow lane batches.
New membership can therefore take that interval to affect assignments, before any existing lane lease
expires. Every scan uses one read-only query to probe assigned lanes' first non-parked heads. Empty lanes
and lanes blocked by a scheduled or live-claimed head cause no lease writes. The worker re-reads under
the lane lock before sending; a probe never grants ownership or permits overtaking a head.

A worker scans its assigned lanes and sends one bounded batch per lane per cycle. After any committed progress it immediately starts another cycle, including when the time budget shortened a batch. PollInterval applies only to scans with no progress, so a backlog is not limited to one batch per polling interval. Each lane lease has
an instance-specific owner token. Its database row stays locked in a transaction until native sends and
fenced row outcomes finish. A competing owner cannot take over while that transaction remains connected,
even after lease expiry. The worker sets a deadline of half the lease duration for acquiring, reading,
sending and committing each batch. It stops admitting rows after one quarter of the lease duration, reserving the remaining deadline for the final admitted send and commit. Slow healthy batches therefore commit a confirmed prefix instead of repeatedly replaying it. A single send must still fit the configured hard deadline. It awaits cancellation completion before freeing borrowed payloads.

Reads follow the partial `(lane, seq)` pending index, in database-assigned insertion order. Scheduling and existing claims
never cause the worker to skip a head row. A future or live-claimed head temporarily stalls its lane.
Later rows with a parked head's ordering key stay parked while other keys can continue, using the parked-key
index comparing `seq`, so host clock differences cannot let a later same-key row bypass a parked row. After `MaxDispatchAttempts` failures (default 10), this worker atomically dead-letters the row and establishes that barrier. [Inspect, redrive or purge poison rows](postgres-store.md#poison-rows-and-key-release) through the keyed `IDeadLetterManager`.

Messages are sent sequentially through the registered native `ITransport`, preserving original bodies,
headers, identities and ordering keys. Due delivery timestamps are consumed by the database rather than
scheduled again in the broker. Successful rows are deleted with their claim tokens; failures persist attempts
and the dispatch error and stop the batch. Preparation failures, including missing adapters, missing ordering keys
and unsupported native ordering, use the same durable failure path. Completed rows before the failure commit,
so a bad row does not repeatedly replay a successful prefix. Database or completion uncertainty rolls back the batch, retaining
rows for retry. Up to `BatchSize` accepted sends can replay when their batch does not commit. Unordered rows are untouched.
The single dispatch loop reuses mutation commands and typed parameters across rows and batches. Each
successful row still uses one conditional claim UPDATE and one fenced DELETE around its native send.

## Guard a projection against stale delivery

Ordered PostgreSQL rows carry their database insert sequence in the reserved
`relayer.outbox.sequence` header. The dispatcher replaces any producer-supplied value with the row's
sequence, including on retries. `context.OutboxSequence` reads the positive `long` lazily from native
text or UTF-8 headers; it returns `null` when the header is missing or malformed. Unordered dispatch
does not add this header; a producer-supplied value on an unordered route has no ordering meaning.
Other producers and stores may omit it.

Compare values only for the **same ordering key, outbox table and tenant**. Values increase for
strictly sequential transactions, including when the later commit has a smaller UUIDv7. They are
not gap-free, are not comparable across tables or tenants, and give no defined order to genuinely
concurrent transactions. Do not reset the source identity sequence. When rebuilding or switching a
source table, rebuild the projection's stored sequence too.

Keep the version guard and projection update in one atomic database statement. For example, a
handler for a full-state `OrderUpdated` event can use PostgreSQL's conditional upsert (the projection
has a primary key on `(tenant_id, order_id)` and a `last_sequence bigint NOT NULL` column):

```csharp
[Message("order.updated")]
public sealed record OrderUpdated(string TenantId, string OrderId, string Status);

public sealed class OrderProjectionHandler(NpgsqlDataSource projections) : IHandler<OrderUpdated>
{
    public async ValueTask HandleAsync(OrderUpdated message, MessageContext context,
        CancellationToken cancellationToken)
    {
        var sequence = context.OutboxSequence
            ?? throw new InvalidOperationException("This projection requires an ordered outbox sequence.");
        await using var command = projections.CreateCommand("""
            INSERT INTO order_projection (tenant_id, order_id, status, last_sequence)
            VALUES (@tenant, @id, @status, @sequence)
            ON CONFLICT (tenant_id, order_id) DO UPDATE
            SET status = EXCLUDED.status, last_sequence = EXCLUDED.last_sequence
            WHERE order_projection.last_sequence < EXCLUDED.last_sequence
            """);
        command.Parameters.AddWithValue("tenant", message.TenantId);
        command.Parameters.AddWithValue("id", message.OrderId);
        command.Parameters.AddWithValue("status", message.Status);
        command.Parameters.AddWithValue("sequence", sequence);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }
}
```

Here every event for an order uses that order's ordering key and comes from one source outbox table.
A duplicate or older event leaves the stored state unchanged. This example assumes full-state events;
rejecting an older delta cannot reconstruct a missed intermediate change. Use an application business
version when concurrent writers need a domain-defined order.

## Failure boundaries

Ordering means ascending database-assigned `seq` among rows visible to a lane scan, using the broker's own
key-ordering semantics. UUIDv7 remains the stable message identity, not the ordering cursor. Two strictly
sequential transactions dispatch in commit order even when the later message has a smaller UUID because
application clocks differ. Rows in one transaction receive sequence values in send order, including sends
interleaved across EF contexts sharing that transaction.

Genuinely concurrent transactions still have no defined order: sequence allocation precedes commit, so a
lower-sequence row may become visible after a higher one has already dispatched. Applications needing a
business order across concurrent writers must serialize those business writes and use idempotent,
version-guarded consumers. Sequence values may have gaps; retain the identity sequence's default cache
of one and never reset it. Upgrade existing tables with the explicit [sequence migration](postgres-schema.md#upgrade-existing-tables-to-insert-sequence-ordering)
before starting upgraded workers. Existing rows retain their former UUID order during that migration;
their historical insertion order cannot be reconstructed.

Delivery remains at-least-once. A crash after the broker accepts a message but before database commit replays
that identity. Cancellation and database fencing cannot retract a native request already accepted by the
broker. If a database connection dies while a native request remains in flight, its duplicate can arrive after
a successor's sends. The protocol prevents overlapping connected database owners, but does not promise
exactly-once delivery or fence native broker requests. Consumers needing duplicate suppression must use
idempotent processing or the [EF Core consumer inbox](ef-core-inbox.md). A transport must observe cancellation for prompt
shutdown. A stalled connected transaction deliberately delays takeover until it ends.
