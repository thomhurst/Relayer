---
title: PostgreSQL recovery and cleanup
description: "Recover undispatched PostgreSQL outbox messages and apply leased, bounded retention."
---

Register maintenance alongside [direct dispatch](direct-dispatch.md) to recover unordered messages left
by process death, queue overflow, failed sends, or a future delivery time. [Ordered lane dispatch](ordered-dispatch.md)
already recovers its own lanes; the sweeper never sends ordered rows.
Optional [PostgreSQL CDC dispatch](postgres-cdc.md) wakes unordered recovery from logical replication
commits. Keep maintenance for failed sends, expired claims, future schedules and retention.

## Registration

Deploy the [schema](postgres-schema.md), register your named `ITransport` adapters, then register maintenance
on every instance. Use the same application-owned `NpgsqlDataSource`, schema and prefix as your writer:

```csharp
services.AddRelayerPostgreSqlMaintenance("orders", dataSource,
    new PostgreSqlMaintenanceOptions
    {
        Schema = "messaging",
        TablePrefix = "orders_",
        BatchSize = 100,
        RecoveryDelay = TimeSpan.FromSeconds(30),
        SweepInterval = TimeSpan.FromSeconds(5),
        LeaseDuration = TimeSpan.FromSeconds(30),
        ClaimDuration = TimeSpan.FromSeconds(30),
        CleanupInterval = TimeSpan.FromMinutes(1),
        DeadRetention = TimeSpan.FromDays(30),
        InboxRetention = TimeSpan.FromDays(7)
    });
```

The host starts and stops the worker. This registration does not create tables, register a writer or
start ordered dispatch. Stop the host before disposing the data source. Graceful shutdown cancels work,
awaits in-flight operations, and releases this worker's leases. After a crash, leases and row claims expire.
With `NpgsqlSlimDataSourceBuilder`, enable arrays for the reused batch claim and completion operations.
Each pass uses one connection for leadership, recovery claims, completion and retention. Maintenance does not
hold a pool connection while waiting to borrow another; a single-connection pool works, with application work serialized.

Use `services.AddRelayerPostgreSqlMaintenance("orders", dataSource, configuration.GetSection("Maintenance"))`
to bind the same property names without reflection. Set `DeadRetention` or `InboxRetention` to `"infinite"` in configuration,
or `null` in code, to disable deletion from that table. Settings are immutable snapshots, readable through
`IOptionsMonitor<PostgreSqlMaintenanceOptions>.Get("orders")`; restart to change them. Batch size must be
1–10000. Delays and retention must be positive (recovery age can be zero); leadership leases must be 1 second–1 hour.
All instances sharing these tables must agree on retention and timing settings.

## Recovery and ownership

Only one instance holds the `sweeper` lease for these tables. A separate `cleanup` lease permits only one
retention job. Both use `relayer_leases` (with your configured prefix), the database clock, unique owner
identities and transaction locks. A connected job holds its lease row lock until its pass finishes; even
an expired lease cannot let another connected owner overlap that pass. Leadership expiry restarts at completion.
An idle leader renews on its next pass; another instance can take over after expiry or explicit release.

Each recovery pass reads at most `BatchSize` pending unordered rows in ID order, with no live claim and a
due delivery time. `RecoveryDelay` measures age from stored `created_at`, not from a separately recorded
commit timestamp. Recent rows remain available for immediate commit-triggered dispatch. Each keyset traversal
captures the highest pending unordered ID and scans only through that fixed boundary. It wraps after an empty
pass within that boundary, so continuous newer traffic cannot prevent retries of failed sends, newly due schedules
or expired claims behind the cursor. A failed destination cannot permanently hide later rows. Each pass waits
`SweepInterval` before the next; this also bounds retry frequency. Large backlogs require multiple passes.

Recovery reuses direct dispatch's conditional claims, transport/destination grouping, native batch limits,
and fenced batch deletion. Recovery's claim/completion mutations share the leadership transaction and commit
with lease renewal; a crash or shutdown before commit rolls these mutations back. Direct dispatch continues
to use independently committed claims. A late error can therefore replay an earlier send in the same recovery pass. It preserves identity, body and headers, including the original creation context.
Already-due schedules request immediate native delivery. A concurrent direct dispatcher can win a row claim;
recovery then skips that row. Claim deadlines use the same UTC-clock protocol as direct dispatch.
Choose `ClaimDuration` longer than expected broker and database latency and keep instance clocks synchronized.

Delivery is at-least-once. A crash after native acceptance and before deletion replays the same identity.
Database fencing cannot retract an accepted native request, and connection loss or a native send that outlives
its claim can still duplicate delivery. Connected leadership does not provide broker-side fencing.
Failures are logged and retried on later passes up to `MaxDispatchAttempts` (default 10), then atomically moved to [dead storage](postgres-store.md#poison-rows-and-key-release). Retry backoff remains planned.

## Retention

Successfully dispatched outbox rows are deleted immediately in fenced batches. **Pending and parked outbox
rows never expire by age.** Cleanup cannot discard undelivered messages, including future schedules.

Each cleanup pass deletes at most `BatchSize` dead rows older than `DeadRetention` (from `failed_at`) and at
most `BatchSize` inbox records older than `InboxRetention` (from `processed_at`). Recent rows stay intact.
Cleanup becomes due every `CleanupInterval` and runs after a sweep, so a slow sweep can delay cleanup.
Retention applies to existing tables. The [EF Core consumer boundary](ef-core-inbox.md) supplies inbox recording;
[Poison management](postgres-store.md#poison-rows-and-key-release) supplies inspection, redrive and explicit purge. Dead rows with an active ordered key barrier are retained until management releases them.

**Inbox retention bounds deduplication.** After deletion, an old identity can invoke its consumer again.
Choose retention longer than your broker replay/redelivery window, or disable it. Dead-row deletion is
permanent; use indefinite retention if operational investigation or audit requires keeping those records.


### Sent-message history

Sent history is disabled by default. To retain diagnostic evidence after successful sends,
set `RetainSentMessages = true` on the `PostgreSqlOutboxOptions` passed to direct dispatch
and durable stores, and on every `PostgreSqlOrderedDispatchOptions`,
`PostgreSqlChangeCaptureOptions` and `PostgreSqlMaintenanceOptions` for those tables.
Tenant catalog dispatch reads the flag from each store's `PostgreSqlOutboxOptions`.

```csharp
services.AddRelayerPostgreSqlDirectDispatch("orders", dataSource,
    new PostgreSqlOutboxOptions { RetainSentMessages = true });
services.AddRelayerPostgreSqlMaintenance("orders", dataSource,
    new PostgreSqlMaintenanceOptions
    {
        RetainSentMessages = true,
        SentRetention = TimeSpan.FromDays(7)
    });
```

Apply the current [schema script](postgres-schema.md) before enabling sent history in workers or the
lookup tool; rerunning it adds the history table and index to existing schemas.
`outbox_sent` stores only ID, destination and `sent_at`, never body, headers or errors.
A fenced completion deletes the outbox row and inserts this metadata in one PostgreSQL
statement; stale or expired claims record nothing. Ordered and recovery transactions
commit or roll back both changes together. Native acceptance followed by a crash before
completion can still redeliver the message.

Maintenance deletes at most `BatchSize` history rows per cleanup pass, oldest first,
using the existing `cleanup` lease. If completion refreshes history for a reused message ID
while cleanup waits on its row lock, cleanup rechecks the refreshed timestamp and keeps
the entry until it is old enough. `SentRetention` defaults to seven days and must be
positive and finite; configuration uses the same property names. Tenant dispatch exposes
`SentRetention` on `PostgreSqlTenantDispatchOptions`. Keep maintenance running and
`RetainSentMessages` enabled there while history exists: stopping cleanup or disabling
that flag leaves existing history intact. The window is an age policy, not a hard row
count or size limit: cleanup lag can retain older entries until later passes.

When disabled, dispatch uses the original delete SQL with no extra branch, parameters,
copy, allocation or database round trip. When enabled, each completed message adds one
metadata insert/upsert and its primary-key and retention-index writes in the same round
trip. No body crosses back to the application. The PostgreSQL tenant-dispatch benchmark
compares enabled and disabled completion including native database I/O.
[Message lookup](postgres-store.md#find-a-missing-message) reports retained entries as `sent` when its `RetainSentMessages` option or CLI `--sent-history` switch is enabled.
