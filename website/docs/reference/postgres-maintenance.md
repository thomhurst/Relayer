---
title: PostgreSQL recovery and cleanup
description: "Recover undispatched PostgreSQL outbox messages and apply leased, bounded retention."
---

Register maintenance alongside [direct dispatch](direct-dispatch.md) to recover unordered messages left
by process death, queue overflow, failed sends, or a future delivery time. [Ordered lane dispatch](ordered-dispatch.md)
already recovers its own lanes; the sweeper never sends ordered rows.

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
to bind the same property names without reflection. Set a retention value to `"infinite"` in configuration,
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
commit timestamp. Recent rows remain available for immediate commit-triggered dispatch. Keyset traversal
wraps after an empty pass, so a failed destination cannot permanently hide later rows. Each pass waits
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
Failures are logged and retried on later passes. Poison-row movement, retry backoff and release of parked keys
remain planned; a permanently failing row remains durable.

## Retention

Successfully dispatched outbox rows are deleted immediately in fenced batches. **Pending and parked outbox
rows never expire by age.** Cleanup cannot discard undelivered messages, including future schedules.

Each cleanup pass deletes at most `BatchSize` dead rows older than `DeadRetention` (from `failed_at`) and at
most `BatchSize` inbox records older than `InboxRetention` (from `processed_at`). Recent rows stay intact.
Cleanup becomes due every `CleanupInterval` and runs after a sweep, so a slow sweep can delay cleanup.
Retention applies to existing tables. The [EF Core consumer boundary](ef-core-inbox.md) supplies inbox recording;
poison-row movement remains planned.

**Inbox retention bounds deduplication.** After deletion, an old identity can invoke its consumer again.
Choose retention longer than your broker replay/redelivery window, or disable it. Dead-row deletion is
permanent; use indefinite retention if operational investigation or audit requires keeping those records.
