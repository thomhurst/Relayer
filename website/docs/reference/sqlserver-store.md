---
title: SQL Server durable store
description: "SQL Server ADO.NET/Dapper transactions, commit-triggered dispatch, leased recovery, poison isolation and bounded retention."
---

`Relayer.Outbox.SqlServer` implements the [durable store contract](outbox-stores.md) using
`Microsoft.Data.SqlClient` and SQL Server's native connection pooling. It supplies reads, atomic claims,
renewal, completion, failure persistence, eligible backlog counts and scoped ADO.NET/Dapper transaction
enlistment, opt-in consumer inbox deduplication, unordered dispatch, leased recovery, poison isolation,
retention and ordered dispatch with fenced lane leases. **EF Core integration remains planned for SQL Server.**
PostgreSQL's worker registrations do not accept this provider.
Registering a durable store alone does not make `ISender` transactional.

**Native AOT and trimming are currently unsupported for SQL Server runtime operations.** SqlClient 7.1.1
retains reflection in authentication, configuration and diagnostics. Outbox/store construction and registration
carry `RequiresUnreferencedCode` and `RequiresDynamicCode` diagnostics; see
[the upstream Native AOT issue](https://github.com/dotnet/SqlClient/issues/1947).
Schema SQL/options remain AOT safe. This restriction is specific to this optional native provider.

## Deployment and registration

Write the initial schema with your application's deployment tooling. Initial creation is idempotent and
does not validate or upgrade an existing table. Schema evolution and rollback remain application-owned.
This script creates the outbox and inbox tables, pending/ordered indexes and a filtered `(lane, seq)` parked-row index, native insert sequence, maintenance leases and a separate `ordered_leases` membership table.
Existing outbox definitions are unchanged; use the explicit upgrade below for pre-sequence tables. It also creates the dead-letter table and retention indexes.

```csharp
using Microsoft.Extensions.DependencyInjection;
using Relayer.Outbox.SqlServer;

var options = new SqlServerOutboxOptions
{
    Schema = "messaging", // null selects dbo
    TablePrefix = "orders_"
};
var sql = SqlServerOutboxSchema.CreateScript(options);
// Execute sql through your normal deployment tooling before using the store.
services.AddRelayerSqlServerOutboxStore("orders", connectionString, options);
```

Resolve `IOutboxStore` with `GetRequiredKeyedService<IOutboxStore>("orders")` inside a DI scope.
Store names are explicit and case-sensitive. Each scope gets a separate instance; await each operation
before starting another on that instance. The native connection string passes through unchanged, including
pooling, authentication and encryption settings. Connections return to SqlClient's pool after each operation.
Schema and prefix options are immutable. Identifiers are escaped, with a 128 UTF-16 code unit limit for
each schema, table and index name. Keep deployment and registration options identical.

The optional `Relayer.HealthChecks` package can adapt this named store through
`AddRelayerOutboxHealthProvider("orders")`; see [health checks](../guides/health-checks.md).

## ADO.NET and Dapper transactions

Register one scoped enlistment and explicitly deploy the schema using the same options:

```csharp
using Dapper;
using Relayer.Outbox.SqlServer;

services.AddRelayerSqlServerOutbox(new SqlServerOutboxOptions { Lanes = 32 });

// In an application DI scope:
var outbox = serviceProvider.GetRequiredService<SqlServerOutbox>();
var sender = serviceProvider.GetRequiredService<ISender>();
await using var transaction = await outbox.BeginTransactionAsync(connectionString);
await transaction.Connection!.ExecuteAsync(
    "INSERT INTO orders (id) VALUES (@Id)", new { Id = order.Id }, transaction);
await sender.SendAsync(new OrderPlaced(order.Id));
await transaction.CommitAsync();
```

`BeginTransactionAsync` returns `DbTransaction`. Create business commands or call Dapper through
**`transaction.Connection`**, passing this transaction. SqlClient commands accept only `SqlTransaction`;
the connection view creates native commands and unwraps the matching transaction. Commands created on
the original `SqlConnection` cannot accept the wrapper. Native parameters, command settings and isolation
levels pass through unchanged.

An overload accepts an already open, caller-owned `SqlConnection`; it stays open after completion.
The connection-string overload owns its pooled native connection and disposes it after completion,
without changing native configuration. Disposing the borrowed connection view does not close that
connection. Commit, rollback and transaction disposal support synchronous and asynchronous calls.
Disposing the enlistment scope abandons an unfinished transaction.

Inside an active transaction, `ISender.SendAsync` and `SendViaOutboxAsync` copy the original envelope
into reusable owned buffers. No broker send occurs. Commit writes all rows through native `SqlBulkCopy`
with `CheckConstraints` and `FireTriggers`, using the **same native transaction** as business commands,
then commits. Defaults initialize dispatch state. Large batches do not create one SQL parameter per
field or hit the 2100-parameter limit. Rollback, disposal, insertion failure and cancellation discard
buffered rows and roll back uncommitted business writes. Start another unit after completion; old
commands cannot attach to it. A lost commit acknowledgement can still have committed, so use application
idempotency for unknown outcomes.

Bulk writes retain SqlClient's independent `BulkCopyTimeout` default of **30 seconds**.
A business command's `CommandTimeout` (including Dapper's timeout) and the connection string's
`Command Timeout` do not change that limit. Relayer does not infer a bulk-copy timeout from a business
command: several commands in the same transaction can have different timeouts. This preserves
[native SqlClient bulk-copy behavior](https://github.com/dotnet/SqlClient/blob/v7.1.1/src/Microsoft.Data.SqlClient/src/Microsoft/Data/SqlClient/SqlBulkCopy.cs#L174).
Size application transactions so their bulk write fits this limit under expected database contention;
if a timeout occurs, the unit rolls back rather than committing business writes alone.

Options are init-only. `AddRelayerSqlServerOutbox(configurationSection)` binds `Schema`, `TablePrefix`
and positive `Lanes` without runtime reflection, validates at host startup, and reloads for new scopes.
Keep the lane count identical across writers. Lanes use the UTF-8 ordering key's FNV-1a hash modulo
`Lanes`, matching PostgreSQL. One scoped `IOutboxEnlistment` integration is allowed.

Use one unit at a time per scope. Ambient `TransactionScope`, nested transactions and savepoints are
rejected. Do not complete native transactions, close native connections or issue SQL transaction-control
commands behind the wrapper. Native per-send callbacks cannot be persisted. No custom retry, pool,
scheduler is installed by transaction enlistment alone. Future scheduled sender calls fail explicitly until a SQL Server
scheduling worker exists. Low-level `IOutboxEnlistment.EnlistAsync` can preserve a supplied `DeliverAt`
for a future dispatcher. Cancellation is passed to SqlClient; some server-side bulk-copy waits can end
only when the server responds or the native timeout expires ([SqlClient #4965](https://github.com/dotnet/SqlClient/issues/4965)). Cancellation before native commit still
rolls back the complete unit.

## Atomic consumer inbox

Register the scoped outbox above, deploy its schema, then register the SQL Server consumer boundary
first in the single-message consume pipeline:

```csharp
pipeline.SqlServer("orders-v1", connectionString, inbox: true);

// Inside a handler, inject SqlServerOutbox and ISender:
var transaction = outbox.CurrentTransaction!;
await transaction.Connection!.ExecuteAsync(
    "INSERT INTO orders (id) VALUES (@Id)", new { Id = order.Id }, transaction);
await sender.SendAsync(new OrderPlaced(order.Id));
// Relayer owns commit, rollback and disposal.
```

`inbox` defaults to `false`; enable it explicitly to deduplicate. Use a stable consumer name across
replicas and restarts, at most 256 UTF-16 code units with no trailing space. Names are case-sensitive.
Deduplication requires the original nonempty UUIDv7 message ID and keys it by consumer. The inbox uses
`uniqueidentifier` for equality only; the outbox's RFC byte ordering remains unchanged.

The boundary opens one native read-committed transaction. SQL Server's `UPDLOCK, HOLDLOCK` key-range
locks and primary-key uniqueness serialize competing deliveries of the same identity. A committed key
skips handling and increments `relayer.inbox.duplicates`; rollback permits redelivery to run the handler.
Duplicate detection uses an explicit SQL result and works when connections inherit `SET NOCOUNT ON`.
Business writes through `CurrentTransaction.Connection`, inbox identity and outgoing rows commit
atomically before broker settlement. An acknowledgement failure can redeliver committed work safely.
Handler failures, cancellation and requested retry/dead-letter outcomes roll back before existing failure
handling and settlement. Cancellation during an inbox lock wait can surface SqlClient’s native
`SqlException` server-attention error instead of `OperationCanceledException`; rollback still completes
before the failure leaves the boundary. Completion is requested before commit when none is pending; scope disposal
cannot change an already durable outcome.

Do not commit, roll back, dispose or replace the consumer transaction inside the handler. Commit and
rollback calls are rejected, while premature disposal rolls back and causes handling to fail. Business
commands must use the borrowed connection, and must not issue SQL transaction-control commands or
close its native connection. Nested boundaries and ambient transactions are unsupported. This integration
supports ADO.NET/Dapper only, not SQL Server EF Core, saga persistence or batch consumers. There is no
provider retry strategy or automatic conflict replay: existing failure handling receives native failures.
The connection string selects one fixed database per pipeline; outbox options select its deployed schema and prefix at composition.
Changing schema or prefix requires rebuilding the pipeline; a mismatching scope fails before opening its transaction. Native AOT/trimming restrictions above apply to this boundary too.

Retain inbox keys for as long as messages can be redelivered. SQL Server maintenance can clean up inbox keys;
deleting a key allows that identity to execute again. Outgoing messages still require the separately registered
dispatcher and recovery described below.

## Commit-triggered unordered dispatch

Deploy the schema first. Select a named worker explicitly in the scoped outbox options:

```csharp
var options = new SqlServerOutboxOptions
{
    Schema = "messaging",
    TablePrefix = "orders_",
    DispatchStoreName = "orders"
};
services.AddRelayerSqlServerOutbox(options);
services.AddRelayerSqlServerDirectDispatch("orders", connectionString, options,
    new SqlServerDirectDispatchOptions
    {
        QueueCapacity = 256,
        BatchSize = 100,
        ClaimDuration = TimeSpan.FromSeconds(30)
    });
```

Register the named `ITransport` services used by the stored routes and start the application's host.
The registration adds a hosted worker and a keyed `IOutboxStore`. It does not create tables.
Writer and dispatcher must use the same schema, prefix and database; mismatches fail before starting
the writer transaction. The dispatcher connection string must specify `Initial Catalog` (or `Database`),
with the database's exact spelling. Server aliases must match the writer's native `DataSource`.
This validates caller-owned connections, including `ChangeDatabase`, without opening another pooled
connection or relying on a login's default database. `DispatchStoreName` is optional: without it, transactions only persist messages.
The configuration overload for transaction enlistment also binds `DispatchStoreName`.

After a successful synchronous or asynchronous commit, due unordered buffers move to a bounded
in-process queue. The committing scope can end or begin another transaction while the worker sends.
No activity or scoped service travels with the queued buffers. Native endpoint capabilities and
`BatchSize` bound each send; single messages use `SendAsync`, groups use `SendBatchAsync`.
`SqlBatch` groups fenced claim and completion statements through the native client. Claims use fresh
tokens, and completion deletes only rows still owned by that live token. An expired claim leaves the
row available for later recovery, even if the broker accepted the message; delivery remains at least once.
Explicitly retryable native send failures record the error, increment a diagnostic attempt count (saturating at 30), and
release live claims with a durable `deliver_at` deadline. Retry delays start at one second, double after
each failed send and stop growing at five minutes. Native clients still own retries within each send;
the dispatcher adds no in-process send retry loop. A completion failure after a successful send retains
the claim without incrementing send attempts.

**This worker does not poll or recover rows.** Ordered rows, future schedules, overflow and commits
after shutdown remain durable. A full or stopped queue logs an error without making an already committed
transaction fail. Shutdown drains queued work until the host's cancellation deadline. Applications must
register the maintenance worker below to recover missed sends. Future scheduled `ISender` calls still fail explicitly: this worker cannot establish scheduling support.

Commit notification is local to this process. It does not enable Service Broker or query notifications,
create conversation queues or claim cross-process wakeup support. Application-owned native Service Broker
configuration remains separate from this worker.

## Recovery, poison rows and retention

Deploy `SqlServerOutboxSchema.CreateScript(options)` explicitly, including on existing stores: it now
creates missing `outbox_dead` and `leases` tables and retention indexes without changing existing columns.
Register a named worker against the same database, schema, prefix and `MaxDispatchAttempts` as direct dispatch:

```csharp
services.AddRelayerSqlServerMaintenance("orders", connectionString,
    new SqlServerMaintenanceOptions
    {
        BatchSize = 100,
        RecoveryDelay = TimeSpan.FromSeconds(30),
        SweepInterval = TimeSpan.FromSeconds(5),
        LeaseDuration = TimeSpan.FromSeconds(30),
        ClaimDuration = TimeSpan.FromSeconds(30),
        CleanupInterval = TimeSpan.FromMinutes(1),
        MaxDispatchAttempts = 10,
        DeadRetention = TimeSpan.FromDays(30),
        InboxRetention = TimeSpan.FromDays(7)
    });
```

An `IConfiguration` overload snapshots the same init-only properties into named Options settings.
Use `"infinite"` for either retention in configuration, or `null` in code, to disable that cleanup.
Keep the inbox retention window longer than the application's replay horizon: deleting a key permits
that message to run again.

Each recovery pass reads at most `BatchSize` due, unordered pending rows older than `RecoveryDelay`,
using a fixed UUID high-water mark before wrapping. Failed, previously claimed and newly due older rows
are revisited even while newer rows arrive. `SweepInterval` delays every bounded pass, including full
batches; tune it together with `BatchSize` for the desired recovery rate and database load.
Ordered lanes and parked rows remain untouched.
Stored schedules are respected; this registration does not yet enable future scheduled `ISender` calls.
Recovery reuses direct dispatch's endpoint grouping, capability cache and fenced claims in one database
transaction. A crash or database error rolls back the whole pass, including completions for messages
already accepted by the broker; those messages can all replay with their original identities. Delivery
is at least once. Recovery decrements `relayer.outbox.pending` only after the pass commits;
a rolled-back pass publishes no pending-count decrement. Row locks remain held through native sends until the pass commits or rolls back.

Separate durable `sweeper` and `cleanup` leases use the database clock. Native transaction application
locks serialize connected passes without waiting behind another leader. Idle leadership expires after
`LeaseDuration`; graceful shutdown releases owned leases. Choose `ClaimDuration` above database and broker
latency. These workers poll; they do not enable Service Broker, query notifications or CDC.

Malformed encoded headers are transferred immediately to `outbox_dead`, with the raw payload and an
actionable `last_error`. Recovery records missing transports and invalid endpoint capabilities as failed
attempts without stalling unrelated groups. Preparation and unclassified native send failures use
`MaxDispatchAttempts` (default 10). Unordered rows that exhaust this budget leave the outbox.

Set `RetrySendFailure` on `SqlServerOutboxOptions`, `SqlServerMaintenanceOptions` and
`SqlServerOrderedDispatchOptions` to the same predicate to identify known retryable native failures.
For example, with Azure Service Bus use:

```csharp
RetrySendFailure = static error =>
    error is Azure.Messaging.ServiceBus.ServiceBusException { IsTransient: true }
```

If the predicate throws, Relayer logs the classification error and records the original send failure
against the poison budget.

Only true opts into indefinite durable retries, with one-second exponential delays capped at five
minutes. Classified sends remain eligible beyond `MaxDispatchAttempts`, including after worker
replacement. Null (the default) or false preserves bounded poison handling for permanently rejected
or unclassified sends. Use native exception information; do not classify permanent rejections as retryable.
A failed native batch applies the same classification to all its claims because delivery may be partial.
Explicit low-level `RecordFailureAsync` calls retain the poison transition. Completion errors after broker
success do not consume send attempts.

Each cleanup pass deletes at most `BatchSize` expired dead rows and `BatchSize` expired inbox keys,
ordered by their failure or processing time. A failed recovery pass is logged and retention is still
attempted on its own schedule. It never expires pending or parked outbox messages.

## Storage and UUID ordering

The outbox columns match `OutboxRecord`: `id`, `lane`, `ordering_key`, `transport`, `destination`,
`message_type`, `headers`, `body`, `created_at`, `deliver_at`, `attempts`, `last_error`, `tenant_id`,
plus `seq`, `status`, `claim_token` and `claim_expires_at`. `seq` is assigned by a native SQL Server sequence; UUIDs remain stable identities and unordered read cursors.

`id` and `claim_token` use **binary(16) in RFC/network byte order**. Use
`id.ToByteArray(bigEndian: true)` when writing through native SQL, and
`new Guid(bytes, bigEndian: true)` when reading. Do not use the default mixed-endian `Guid.ToByteArray()`,
SQL Server `uniqueidentifier` ordering, or a direct cast from `uniqueidentifier` to binary. Sorting and
exclusive `AfterId` cursors compare the same 16 RFC bytes, including across UUIDv7 timestamp boundaries.
Message IDs must be nonempty UUIDv7 values supplied by the writer; the schema rejects empty IDs.

Strings use `nvarchar(max)` except the constrained `pending`/`parked` status. Bodies and headers use
`varbinary(max)`. Timestamps use `datetimeoffset(7)`, preserving 100 ns precision and comparing UTC
instants even when offsets differ. Lanes are nullable nonnegative SQL `int` values. Attempts start at zero.
Token and expiry must both be null or both be present, and a present token must be nonempty.

The binary header format is shared with PostgreSQL: little-endian 32-bit version `1`, then entry count,
then each UTF-8 name and raw value prefixed by a little-endian 32-bit byte length. Duplicate names,
entry order, empty values and arbitrary value bytes survive storage. Corrupt headers fail explicitly.
Do not populate `headers` with JSON or empty bytes. Native writers must preserve the original CloudEvents
identity, stable versioned wire message type, creation context, body, ordering key and schedule. They must
reject native callbacks, which cannot be persisted. Use the scoped transaction integration below to enqueue through `ISender`.

## Ordered dispatch

Deploy the schema, register named native `ITransport` adapters, and add a worker:

```csharp
services.AddRelayerSqlServerOutbox(new SqlServerOutboxOptions { Lanes = 32 });
services.AddRelayerSqlServerOrderedDispatch("orders", connectionString,
    new SqlServerOrderedDispatchOptions
    {
        Lanes = 32,
        BatchSize = 100,
        LeaseDuration = TimeSpan.FromSeconds(30),
        PollInterval = TimeSpan.FromSeconds(1),
        MaxDispatchAttempts = 10
    });
```

Writers and workers must target the same database, schema, prefix and lane count. Drain before changing
lane count. A configuration-section overload binds these immutable settings through named options without
reflection; native SqlClient runtime trimming/AOT restrictions still apply. Register unordered direct dispatch
separately. Ordered dispatch does not require `DispatchStoreName`.

The database clock controls membership, lane leases, schedules and fenced message claims. Ordered
membership uses `ordered_leases`, separate from the maintenance `leases` table. Members
rebalance lanes into fair shares; each worker stops renewing lanes outside its share. A new owner may
wait up to `LeaseDuration` for the previous owner's lease to expire. A lane transaction
locks its ownership row across sequential native sends, preventing another connected worker from stealing
an in-flight lane even after expiry. Hard send deadlines use half the lease duration; admission stops after
one quarter. Cancellation and shutdown roll back uncertain batches and release membership/leases when possible;
otherwise they expire. A disconnected or cancellation-insensitive native send may still complete, so delivery
remains at least once. Up to `BatchSize` accepted sends can replay after a rollback.

Lanes read by database `seq`, including scheduled and claimed heads. Future or live-claimed heads block
the lane; the worker never skips them. Explicitly retryable native send failures persist the same one-second-to-five-minute
backoff as unordered dispatch. The head holds the lane until its deadline, then retries automatically;
classified broker outages never park its key. Exhausting `MaxDispatchAttempts` on preparation or
unclassified send failures parks the
original row, preserving its sequence and error. Later rows with exactly the same key remain behind that barrier, while other keys
in the lane can proceed. SQL Server dead-letter inspection/redrive/purge APIs remain planned; operate on
parked rows only with application-owned tooling that preserves the row and sequence. The worker records
`relayer.outbox.poison` when parking; it does not add an unordered recovery or retention worker.

A native sequence range is reserved immediately before each ordered bulk flush and assigned in send
position order. Sequential transactions therefore dispatch in commit order even when their UUIDv7 values
sort backwards. Concurrent transactions have no defined commit order. Gaps are normal: do not reset or
reuse the sequence. Each ordered send replaces producer-supplied `relayer.outbox.sequence` headers with
the database value; see [sequence guards and native-send limits](ordered-dispatch.md#guard-a-projection-against-stale-delivery).
Native transport ordering and settlement are unchanged; destinations must advertise native ordering by key.

### Upgrade pre-sequence tables

Stop all writers and workers. Execute `SqlServerOutboxSchema.UpgradeOrderedDispatchScript(options)` in
one explicit, application-owned transaction, then commit before restarting. The script refuses autocommit
and already-upgraded tables. It assigns existing rows their historical UUID order because their original
insert order cannot be recovered, creates the ordered index and `ordered_leases` table, and continues the sequence
after the migrated maximum. Test your deployment/rollback procedure on a database copy first.

```csharp
await using var connection = new SqlConnection(connectionString);
await connection.OpenAsync();
await using var transaction = (SqlTransaction)await connection.BeginTransactionAsync();
await using var upgrade = new SqlCommand(
    SqlServerOutboxSchema.UpgradeOrderedDispatchScript(options), connection, transaction);
await upgrade.ExecuteNonQueryAsync();
await transaction.CommitAsync();
```

## Reads and fenced outcomes

Reads return at most the destination buffer length, selecting one lane or unordered rows with a null lane.
They include only pending rows that are due and have no live claim. Counts apply the same eligibility
rules across all lanes. A schedule becomes due and a claim expires exactly at the supplied time.
Parked rows stay excluded. Body and header buffers are reused after their high-water mark; finish using
borrowed rows before the next read and clear caller-owned destination slots when finished.

Claiming uses one conditional SQL `UPDATE`. Renewal never shortens an expiry. Renewal, completion and
failure recording require the current fencing token and a live claim. Completion deletes the row;
failure increments attempts, stores the error and releases the claim in one statement. Repeating a failed
attempt with its released token cannot increment attempts again. Row claims alone do not establish ordering.

Operations use native autocommit so mutations are durable before returning. An ambient `TransactionScope`
is rejected before opening a connection. Database errors and cancellation propagate instead of becoming
an empty backlog. A cancelled or failed operation may already have committed, so callers must tolerate
at-least-once delivery. No custom retry loop or connection pool is installed.

## Performance coverage

`SqlServerOutboxReadBenchmarks` measures real database reads at 1 and 64 rows with `[MemoryDiagnoser]`.
Set `RELAYER_SQLSERVER_BENCHMARK_CONNECTION` to an isolated database with schema permissions and run:

```shell
dotnet run -c Release --project benchmarks/Relayer.Benchmarks -- --filter '*SqlServerOutboxReadBenchmarks*'
```

`SqlServerConsumerBenchmarks` compares native inbox transactions with the consume pipeline, including
scoped enlistment, UUID parsing, native key-range locking and commit. It covers fresh and duplicate identities
with `[MemoryDiagnoser]`; select it with the same connection variable and `--filter`.

`SqlServerOutboxBufferBenchmarks` attributes reusable copy/header/lane work at 256 and 4096 bytes without database I/O.
`SqlServerOutboxCommitBenchmarks` covers unordered and ordered bulk commits at 1, 64 and 7,000 rows,
with and without an `AFTER INSERT` row-count trigger. It compares raw `SqlBulkCopy` from a `DataTable`,
raw streaming copy from a `DataTableReader`, and Relayer's buffered commit with the same 1 KiB bodies
and headers. Ordered Relayer commits additionally reserve native sequence ranges; raw copies use the
table's sequence default. Use unordered cases to isolate bulk-copy cost.
`SqlServerOrderedDispatchBenchmarks` measures complete owned-lane batches, including SQL reads, reusable decoding and fenced outcomes; `OrderedDispatchBenchmarks` isolates managed preparation.
`SqlServerOutboxDispatchBenchmarks` compares reusable envelope buffers with commit-to-worker ownership
transfer and destination grouping at 1 and 64 rows, excluding database I/O and native allocations.
Use the same connection variable and select either class with `--filter`. Each commit invocation truncates
its previous batch in the same transaction to keep table size stable without measuring per-row deletion.
Iteration cleanup verifies the persisted row count and the trigger's batch size outside the measurement.
For the large-batch investigation alone, use `--filter '*SqlServerOutboxCommitBenchmarks*Rows: 7000*Ordered: False*'`.
Compare both raw paths before attributing streaming or server costs to Relayer; timings on emulated
SQL Server images are diagnostic, not production capacity estimates.

The database benchmarks create and remove their own tables. Results include SqlClient's allocations and I/O;
they are diagnostic and do not establish the direct-send/receive throughput budget. Relayer reuses
UUID buffers, header entries, payload buffers and boxed parameter values; native SqlClient commands,
parameters, readers and decoded strings remain driver allocations.
