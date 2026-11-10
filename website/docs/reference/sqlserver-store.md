---
title: SQL Server durable store
description: "SQL Server ADO.NET/Dapper transactions, commit-triggered dispatch, bounded reads and fenced claims."
---

`Relayer.Outbox.SqlServer` implements the [durable store contract](outbox-stores.md) using
`Microsoft.Data.SqlClient` and SQL Server's native connection pooling. It supplies reads, atomic claims,
renewal, completion, failure persistence, eligible backlog counts and scoped ADO.NET/Dapper transaction
enlistment, opt-in consumer inbox deduplication and commit-triggered unordered dispatch. **EF Core, ordered workers, lane leases, poison-row movement and
maintenance remain planned for SQL Server.** PostgreSQL's worker registrations do not accept this provider.
Registering a durable store alone does not make `ISender` transactional.

**Native AOT and trimming are currently unsupported for SQL Server runtime operations.** SqlClient 7.1.1
retains reflection in authentication, configuration and diagnostics. Outbox/store construction and registration
carry `RequiresUnreferencedCode` and `RequiresDynamicCode` diagnostics; see
[the upstream Native AOT issue](https://github.com/dotnet/SqlClient/issues/1947).
Schema SQL/options remain AOT safe. This restriction is specific to this optional native provider.

## Deployment and registration

Write the initial schema with your application's deployment tooling. Initial creation is idempotent and
does not validate or upgrade an existing table. Schema evolution and rollback remain application-owned.
This script creates the outbox table, pending index and inbox table. Re-run it when upgrading from
a version without the inbox table; existing outbox definitions are unchanged. It does not create dead or lease tables.

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

Retain inbox keys for as long as messages can be redelivered. SQL Server inbox cleanup is application-owned;
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
Native send failures increment attempts and release live claims. A completion failure after a successful
send retains the claim without incrementing send attempts.

**This worker does not poll or recover rows.** Ordered rows, future schedules, overflow and commits
after shutdown remain durable. A full or stopped queue logs an error without making an already committed
transaction fail. Shutdown drains queued work until the host's cancellation deadline. Applications must
operate their own recovery process until [SQL Server recovery and maintenance](https://github.com/thomhurst/Relayer/issues/331)
ships. Future scheduled `ISender` calls still fail explicitly: this worker cannot establish scheduling support.

Commit notification is local to this process. It does not enable Service Broker or query notifications,
create conversation queues or claim cross-process wakeup support. Application-owned native Service Broker
configuration remains separate from this worker.

## Storage and UUID ordering

The outbox columns match `OutboxRecord`: `id`, `lane`, `ordering_key`, `transport`, `destination`,
`message_type`, `headers`, `body`, `created_at`, `deliver_at`, `attempts`, `last_error`, `tenant_id`,
plus `status`, `claim_token` and `claim_expires_at`.

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
`SqlServerOutboxCommitBenchmarks` compares native bulk-copy commits with buffered commits at 1 and 64 rows.
`SqlServerOutboxDispatchBenchmarks` compares reusable envelope buffers with commit-to-worker ownership
transfer and destination grouping at 1 and 64 rows, excluding database I/O and native allocations.
Use the same connection variable and select either class with `--filter`. Each commit invocation deletes
its previous batch in the same transaction to keep table size stable.

The database benchmarks create and remove their own tables. Results include SqlClient's allocations and I/O;
they are diagnostic and do not establish the direct-send/receive throughput budget. Relayer reuses
UUID buffers, header entries, payload buffers and boxed parameter values; native SqlClient commands,
parameters, readers and decoded strings remain driver allocations.
