---
title: MySQL durable store
description: "MySQL atomic consumer inbox, transactional enqueue, commit-triggered dispatch, Dapper integration and fenced claims."
---

See the [database integration support matrix](outbox-stores.md#database-integration-support) for EF Core and ADO.NET inbox/outbox availability.

`Relayer.Outbox.MySql` implements the [durable store contract](outbox-stores.md) using
MySqlConnector and MySQL 8.4+ InnoDB. It supplies bounded reads, atomic claims, renewal,
completion, failure persistence, leased recovery, bounded retention and eligible backlog counts. Scoped ADO.NET transactions
commit business writes and buffered sends together. An optional hosted worker dispatches due unordered messages after commit.

## Deployment and registration

Apply the schema with your application deployment tooling. The connection must select an
existing database; Relayer does not create databases or upgrade existing table definitions.
Keep the prefix identical in deployment and registration. Prefixes allow at most 52 BMP characters,
reserving space for the table suffix and MySQL's generated CHECK constraint names. NUL and
supplementary Unicode characters are rejected; embedded backticks are quoted safely.

```csharp
using Microsoft.Extensions.DependencyInjection;
using MySqlConnector;
using Relayer;
using Relayer.Outbox.MySql;

await using var dataSource = new MySqlDataSource(connectionString);
var options = new MySqlOutboxOptions { TablePrefix = "orders_" };
await using (var command = dataSource.CreateCommand(MySqlOutboxSchema.CreateScript(options)))
{
    await command.ExecuteNonQueryAsync();
}
services.AddRelayerMySqlOutboxStore("orders", dataSource, options);
```

Resolve `IOutboxStore` with `GetRequiredKeyedService<IOutboxStore>("orders")` inside a DI scope.
Each scope owns and disposes its store's cached commands. Your application owns the data source;
dispose it after all scopes finish. Registration rejects duplicate names and invalid identifiers.
An optional [outbox health provider](../guides/health-checks.md) can use this keyed store.

Native connection settings pass through unchanged. `UseAffectedRows` must be `false` (the
MySqlConnector default): ownership renewal must report a matching live row even when its expiry
does not change. Construction and registration reject `UseAffectedRows=true` with an actionable
error. `GuidFormat` and `DateTimeKind` do not change Relayer's stored representation.

## Reads and fencing

Reads select unordered rows or one lane, honor the caller's buffer length and exclusive ID
cursor, and return ascending UUIDv7 IDs. UUIDs use network-order `BINARY(16)` bytes, without the
time-field swap used by some UUIDv1 integrations. Payload bytes, duplicate headers, header order,
ordering keys and tenant IDs survive reads.

Eligibility means pending, due at the supplied UTC instant, and without a live claim. Counts
span all lanes. A conditional InnoDB update claims one eligible row atomically. Renewal never
shortens expiry; renewal, completion and failure require the matching token and a live claim.
Failure increments attempts and releases the claim. At `MaxDispatchAttempts` (default `10`), an InnoDB transaction copies the raw row to dead storage and deletes an unordered row, or parks an ordered row in place. Keep this limit identical across stores and workers. Repeated or stale mutations return `false`.

Dates use UTC `DATETIME(6)` values with microsecond precision. Await each operation before the
next operation on the same store. Finish using borrowed rows before another read. Database
errors and cancellation propagate; they never become a zero backlog. A cancelled mutation may
already have committed.

Store operations require native autocommit and reject an ambient `TransactionScope` before
opening a connection, including reads and counts. MySqlConnector's default `AutoEnlist=true`
would otherwise enlist the connection and defer durability until the caller's transaction commits.
Call the store outside the ambient transaction; native connection settings remain unchanged.

The `headers` column uses the same versioned binary codec as the PostgreSQL store: little-endian
32-bit version `1` and count, followed by length-prefixed UTF-8 names and byte values. Empty values,
duplicate names and arbitrary value bytes are preserved. Invalid versions, lengths or trailing bytes
fail explicitly. Empty headers require the encoded version and zero count, not an empty BLOB.

## Transactional enqueue with ADO.NET or Dapper

Register one scoped outbox integration, separately from the named read/claim store:

```csharp
services.AddRelayerMySqlOutbox(new MySqlOutboxOptions
{
    TablePrefix = "orders_",
    Lanes = 64
});
```

Resolve `MySqlOutbox` and `ISender` from the same scope. Configure the message's route with
`SendTransactionRequirement.Outbox` as described in the [send guide](../guides/sending.md).
Begin a transaction, use its `Connection` for business commands, and commit through the
returned `DbTransaction`:

```csharp
using Dapper;
using System.Data.Common;

await using DbTransaction transaction = await outbox.BeginTransactionAsync(dataSource,
    cancellationToken: ct);
await transaction.Connection!.ExecuteAsync(new CommandDefinition(
    "INSERT INTO orders (id) VALUES (@Id)", new { Id = orderId },
    transaction: transaction, cancellationToken: ct));
await sender.SendAsync(new OrderPlaced(orderId), ct);
await transaction.CommitAsync(ct);
```

`BeginTransactionAsync` also accepts an already open `MySqlConnection` and an isolation level.
The application retains ownership of that connection. The data-source overload owns and
disposes only the connection it opens. Neither overload disposes the data source. Use one
transaction at a time per scope; overlapping operations are unsupported. Configuration can
also bind through `AddRelayerMySqlOutbox(configurationSection)`, with immutable Options
snapshots per scope, reload for new scopes, and startup validation.

Enqueue takes an owned copy of the original UUIDv7 identity, stable message type, payload,
headers, ordering key and tenant metadata. At commit, Relayer uses MySqlConnector's native
`MySqlBatch` inside the same transaction. Native batching controls packet construction and
execution; it does not promise a single round trip on MySQL. `MySqlBulkCopy` is not required,
so neither `AllowLoadLocalInfile` nor server local-infile settings need changing. Cached
buffers and parameter views are reused after warm-up. Dates and UUIDs use the representation
described above, regardless of native `DateTimeKind` and `GuidFormat` settings.

Both synchronous and asynchronous commit, rollback and disposal are supported. An insert
failure or cancellation before commit rolls back the entire transaction, including earlier
successful inserts in the batch. Disposing the transaction or scope without committing
discards buffered sends and rolls back business writes. As with the native client, loss of
connection or cancellation during COMMIT can leave the outcome unknown; retry the complete
unit with application idempotency rather than assuming it failed.

Use InnoDB for every participating business table. Do not issue transaction-control SQL,
DDL, `LOCK TABLES`, or other statements that implicitly commit through business commands.
MySQL's native implicit commits and nontransactional tables cannot be rolled back by Relayer.
Complete the transaction only through the returned wrapper. Use its `Connection` for Dapper;
assigning the wrapper to a command created directly on a native connection is unsupported.
Savepoints and `CommandBehavior.CloseConnection` are rejected because their effects would
not match the buffered sends. Disposing the borrowed connection view has no effect; dispose
the transaction instead. Ambient `TransactionScope` is rejected before beginning a unit.

Ordering keys use the shared stable lane hash. All writers and ordered workers must
agree on `Lanes` (default `64`); changing it requires draining or migrating existing rows.
The enlistment reports no scheduling support until matching due-row workers are implemented.
Registering this integration alone does not send persisted rows to a broker.

## Atomic consumer inbox

Register `AddRelayerMySqlOutbox` and configure the consumer boundary before any other
middleware. Deduplication is opt-in with `inbox: true`:

```csharp
using Relayer.Pipeline;
services.AddRelayerMySqlOutbox(options);

// On your single-message ConsumePipelineBuilder, after selecting its generated handler:
pipeline.MySql(dataSource, "orders", inbox: true);
```

The boundary opens an InnoDB transaction from the application-owned data source. A handler
injects the scoped `MySqlOutbox` and `ISender`, uses `outbox.CurrentTransaction.Connection`
for ADO.NET or Dapper business commands, and sends outgoing messages through `ISender`.
Return from the handler without committing or disposing the transaction. Relayer commits
business writes, the inbox key and buffered outgoing rows together before native broker
settlement and after requesting a complete outcome. Scope disposal happens before settlement;
it cannot change an outcome that has already committed. Handler calls to commit or roll back
are rejected. Disposing the transaction prematurely rolls back and fails the attempt.

`MySqlOutboxSchema.CreateScript` also creates the prefixed `inbox` table. Apply its script
when upgrading an existing installation; it does not alter existing tables. Identity is the
stable consumer name plus the original nonempty UUIDv7 message ID. Consumer names are exact
UTF-8 bytes (case and trailing spaces matter), at most 768 bytes; use the same name on every
replica and restart. Identity bytes ignore the application's `GuidFormat` setting.

The native unique key serializes concurrent duplicate deliveries. `INSERT IGNORE` skips a
committed key; a competing rollback lets the waiting insert succeed. Validated bounded inputs
fit the inbox schema, so ignored duplicate inserts are the expected no-op. A duplicate skips
the handler, commits its read-only outcome, and records `relayer.inbox.duplicates`. This works
with either native `UseAffectedRows` setting. Inbox commands and UUID scratch buffers are reused
across delivery scopes by a provider-owned pool, separated by table prefix so configuration
reloads do not retarget existing scopes. The pool retains the observed concurrency high-water
mark until provider disposal. Retaining inbox keys retains deduplication;
application-owned deletion deliberately ends that protection.

Handler errors, cancellation before commit, outbox insert errors, and retry or dead-letter
requests roll back the complete unit before the existing failure pipeline runs. A lost
acknowledgement after commit redelivers the same key without repeating business work. A lost
COMMIT response still propagates as an unknown outcome; redeliver the original identity to
resolve it through the inbox. There is no separate native retry loop or commit-verification
connection. Configure ordinary consumer failure handling for complete-attempt retries.

Pass an optional native `IsolationLevel` to `MySql`. InnoDB lock timeouts, deadlocks and other
native errors propagate to the ordinary failure policy. The same InnoDB, implicit-commit,
ambient-transaction and command-connection restrictions as transactional enqueue apply.
The boundary supports single-message ADO.NET/Dapper handlers, with optional unordered commit
dispatch; it does not provide EF Core integration, saga persistence, batch consumers or inbox retention jobs. With `inbox: false` it still owns the atomic business/outbox
transaction, but every redelivery runs the handler.

## Commit-triggered unordered dispatch

Register a named worker and select it on the scoped outbox. Register the application's named
transports before starting the host; they perform the actual broker sends.

```csharp
var options = new MySqlOutboxOptions
{
    TablePrefix = "orders_",
    DispatchStoreName = "orders"
};
services.AddRelayerMySqlOutbox(options);
services.AddRelayerMySqlDirectDispatch("orders", dataSource, options,
    new MySqlDirectDispatchOptions
    {
        QueueCapacity = 256,
        BatchSize = 100,
        ClaimDuration = TimeSpan.FromSeconds(30)
    });
```

The worker registration includes the named `IOutboxStore`; do not also register the same store
name separately. `AddRelayerMySqlDirectDispatch` also accepts an `IConfiguration` section for
its immutable dispatch options, read once at registration; configuration reload does not change
running workers. Set limits through this overload or the explicit options object. The application
owns the data source and disposes it after
workers stop. Its connection string must specify `Database`; writer and worker must use the
same server, port, database spelling and table prefix. Server aliases such as `localhost` and
`127.0.0.1` must also match exactly. Mismatches fail before beginning a transaction.

Synchronous and asynchronous commits transfer owned buffers to a bounded queue only after the
native transaction succeeds. Scope disposal and later transactions cannot overwrite in-flight
payloads. Rollback and abandoned scopes never notify the worker. The worker groups by native
transport and destination and respects both `BatchSize` and each endpoint's native batch limit.
It uses native `SendAsync`/`SendBatchAsync`, preserving message identity, bytes and headers.

Each due unordered row receives a fresh expiring claim token through native conditional updates.
Claim and completion commands use `MySqlBatch`, including native `ROW_COUNT()` results for exact
per-command outcomes. A successful broker send deletes only rows whose token still matches and
whose claim has not expired. After the native client finishes its own send retries, a broker failure
records the error and a durable exponential retry deadline in `deliver_at`: 1 second, 2 seconds,
4 seconds, up to 5 minutes. The diagnostic attempt count saturates at 30. Native send errors do
not exhaust `MaxDispatchAttempts` or move rows to dead storage. A completion error after a successful
send leaves attempts unchanged. An expired or interrupted
send may be delivered again: this is at-least-once delivery, not an atomic broker/database commit.
Size `ClaimDuration` for the slowest expected database and broker operation.

Queue overflow or a stopped worker logs an error and leaves rows durable without failing an
already committed transaction. Graceful shutdown drains accepted work until the host's shutdown
deadline; cancellation leaves unfinished rows durable. Ordered rows and future-due rows are not
queued. Register maintenance below to recover missed notifications, failures and crashes.

## Ordered dispatch

Register the native named transports and an ordered worker alongside the transactional writer:

```csharp
services.AddRelayerMySqlOutbox(new MySqlOutboxOptions { Lanes = 64 });
services.AddRelayerMySqlOrderedDispatch("orders", dataSource,
    new MySqlOrderedDispatchOptions
    {
        Lanes = 64,
        LeaseDuration = TimeSpan.FromSeconds(30),
        PollInterval = TimeSpan.FromSeconds(1),
        BatchSize = 100,
        MaxDispatchAttempts = 10
    });
```

An overload accepts an `IConfiguration` section. Immutable options are read once at worker startup.
Use the same database, prefix and lane count on every writer and worker. The application owns
the data source and disposes it after workers stop. Destinations must support native ordering
by key; invalid routing records a durable failure without sending.

Native `AUTO_INCREMENT` assigns each row a `seq`. The existing `MySqlBatch` flush inserts rows
in send order. Ordered reads follow insertion sequence among visible rows, including when a
later sequential commit has a smaller UUID. Concurrent transactions have no defined commit
order. UUIDs remain stable message and claim identities. The worker replaces producer copies
of `relayer.outbox.sequence` with the database sequence in decimal. Compare it only within one
key, source table and tenant. Gaps are normal; never reset the sequence. Ordinary store read
cursors remain UUID-based.

Database-time membership shares lanes fairly. Conditional InnoDB updates renew ownership;
the native lease-row lock remains held through sequential broker sends and database completion.
A rival cannot steal an in-flight send. Expired ownership transfers after the transaction releases
its lock. Idle scans do not renew lane leases. Hosted batches stop admitting sends after a quarter
lease and request cancellation after half a lease; native completion is awaited before releasing
ownership, including shutdown. Size leases for expected send and database latency.

A future-due or live-claimed head blocks later rows. Database due checks remove `deliver_at`
from the native send. Native send failures persist attempts, errors and the same exponential retry
deadline as direct dispatch, and commit a successful prefix. They never park a row. Until its deadline,
a retrying head stalls its lane, including other keys. Preparation failures such as missing transports,
unsupported ordering and malformed rows retain the poison budget. At `MaxDispatchAttempts`,
the original row parks with its sequence intact;
only later rows for that exact key are blocked, while other keys continue. Malformed headers and
empty required routing fields also consume attempts and park the original raw row. Inspect and repair
parked rows through application-owned SQL. Resume by updating the original row to
`status = 'pending', attempts = 0`; deleting and reinserting it would change its order.
If the repaired row exhausts again through the outbox store, its dead copy is atomically
replaced with the latest raw contents, attempts, error and failure time while the original
row parks again. You do not need to delete the previous dead copy before resuming it.

Completion requires the same token and an unexpired claim. Cancellation, connection loss or
uncertain completion rolls back the batch; broker sends can be duplicated. Delivery remains
at least once. Shutdown releases this member's leases after outstanding sends finish; crashes
leave leases to expire. Ordered rows are polled after restart. Register the separate maintenance
worker below for unordered recovery and retention. Writer scheduling capabilities remain false; registration does not enable
delayed `ISender` calls.

### Upgrade existing tables

Stop **all writers and workers**, back up the database and apply
`MySqlOutboxSchema.UpgradeOrderedScript(options)` exactly once before restarting. It assigns
existing rows their former UUID order because historical insertion order is unavailable, then
enables native identity assignment and creates leases. UUIDs and payloads remain intact.
MySQL DDL commits implicitly: this multi-step upgrade is neither transactional nor idempotent.
After any failure, finish or restore the backup before restarting. Fresh databases use
`CreateScript`, which does not alter existing tables.

## Recovery, poison rows and retention

Apply `MySqlOutboxSchema.CreateScript(options)` during upgrade: it creates missing `dead` and `leases`
tables without altering existing definitions. Register maintenance with the same application-owned
data source, prefix and attempt limit as other stores and dispatchers:

```csharp
services.AddRelayerMySqlMaintenance("orders", dataSource, new MySqlMaintenanceOptions
{
    TablePrefix = "orders_",
    MaxDispatchAttempts = 10,
    BatchSize = 100,
    RecoveryDelay = TimeSpan.FromSeconds(30),
    SweepInterval = TimeSpan.FromSeconds(5),
    ClaimDuration = TimeSpan.FromSeconds(30),
    LeaseDuration = TimeSpan.FromSeconds(30),
    CleanupInterval = TimeSpan.FromMinutes(1),
    CleanupBatchLimit = 10,
    DeadRetention = TimeSpan.FromDays(30),
    InboxRetention = TimeSpan.FromDays(7)
});
```

The overload taking an `IConfiguration` section snapshots immutable named options through the
Options pattern. Use `infinite` for either retention setting to retain records indefinitely.
Invalid settings, duplicate names and a pooled `MaximumPoolSize` below two fail before connecting.
Workers do not create tables.
Keep the data source alive until workers stop. Allow at least two pooled connections: one
native session admits a pass while the existing dispatcher claims and sends rows.

Separate database-time `sweeper` and `cleanup` leases elect leaders across instances. Native
`GET_LOCK(..., 0)` admission avoids waiting behind a connected pass even after its idle lease
expires. Named locks are explicitly released before returning pooled sessions. Release uses a three-second
native command timeout (plus the configured MySqlConnector cancellation timeout); a release failure
is logged without replacing the original pass error or cancellation. Broken sessions are discarded
by MySqlConnector. A crashed process
releases its session lock; rivals recover after leadership and row claims expire. Graceful
shutdown releases this worker's leases. Use one writable MySQL primary: named locks are local
to that server and are unsuitable for independent multi-primary writers.

Workers wait `SweepInterval` after each recovery pass, including a full batch. With the defaults,
a pass reads at most 100 rows before the five-second wait; transport and database time add to that interval.

Each bounded recovery pass selects due, old-enough **unordered** pending rows without live claims.
A fixed upper UUID boundary ensures failed, scheduled and previously claimed rows are revisited
while new rows keep arriving. Ordered lanes and parked barriers stay untouched. The existing
direct dispatcher performs native sends and fenced completion; there is no second sender.
Native send failures use the durable backoff above and remain pending regardless of
`MaxDispatchAttempts`; failed completion after a successful send does not consume attempts.
Expired claims and an unknown broker outcome can duplicate delivery, as with direct dispatch.

Malformed headers or empty routing fields are copied **raw** to dead storage under a live claim,
with an actionable `last_error` and error log; valid rows continue in the same pass. An unregistered
transport or invalid destination consumes recovery attempts and eventually reaches dead storage.
Direct notifications leave invalid routes unclaimed for recovery to inspect.
Malformed ordered rows retain their persisted attempt count, so parking at the configured retry
limit records the outbox poison counter once.

Unordered recovery records `relayer.outbox.dead_lettered` and decrements
`relayer.outbox.pending` only after the fenced transaction commits a poison deletion.
This includes invalid transport destinations and malformed-row quarantine. Retries, stale claims and
rolled-back transitions do not change these counters.

All native send exceptions are retried indefinitely, including a permanent broker rejection, because
native batch errors do not supply a portable per-row permanent classification. A failed native batch
applies backoff to every claimed row in that batch; partial acceptance can duplicate delivery on retry.
Inspect persistent errors and repair routing, payloads or broker configuration through application-owned
SQL. `MaxDispatchAttempts` governs explicit `RecordFailureAsync` calls and preparation failures,
including prior diagnostic send attempts: a preparation error after a long outage may exhaust that
budget immediately. Reset `attempts` when repairing such a row. Clearing `deliver_at` makes a repaired
row eligible immediately; otherwise its persisted deadline survives process restarts and worker changes.

Dead rows expire from their failure time and inbox records from their processing time.
Pending and parked outbox rows never expire; dead payloads referenced by parked rows are protected.
Cleanup visits each enabled table in rounds, deleting at most `BatchSize` rows per statement and
at most `CleanupBatchLimit` batches per table per turn (default 10, range 1–100). A table stops for
that turn when a delete removes fewer than `BatchSize` rows. Each batch commits and releases its
connection and native admission lock before the next batch rechecks leadership. Cancellation or
lease loss stops further admission; already committed batches remain deleted. The first table
rotates between turns so a busy table cannot repeatedly prevent other tables from making progress.

A failed cleanup waits for `CleanupInterval` before retrying, while recovery sweeps continue.
A full final batch schedules another cleanup after the next recovery sweep, rather than waiting
`CleanupInterval`. Recovery therefore gets a turn even under sustained expiry. Once all tables have
returned a partial batch, cleanup returns to `CleanupInterval`. These limits bound work admitted per
turn, not database latency; native command timeouts and cancellation still govern a blocked delete.
They are not a measured cleanup capacity guarantee. Tune the limits for your workload and monitor
retained row counts and database load.

MySQL Event Scheduler executes scheduled SQL but cannot call application transports. It is
neither enabled nor required; recovery uses bounded application passes.

## Remaining integration

Atomic inbox, ordered dispatch, leased unordered recovery, poison handling and bounded retention
are implemented. Parked-row repair remains application-owned SQL. The schema helper is not exposed
through `dotnet relayer db` yet. Full parity is tracked by
[#41](https://github.com/thomhurst/Relayer/issues/41).
