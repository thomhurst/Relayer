---
title: MySQL durable store
description: "MySQL atomic consumer inbox, transactional enqueue, commit-triggered dispatch, Dapper integration and fenced claims."
---

`Relayer.Outbox.MySql` implements the [durable store contract](outbox-stores.md) using
MySqlConnector and MySQL 8.4+ InnoDB. It supplies bounded reads, atomic claims, renewal,
completion, failure persistence and eligible backlog counts. Scoped ADO.NET transactions
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
Failure increments attempts and releases the claim. Repeated or stale mutations return `false`.

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

Ordering keys use the shared stable lane hash. All writers and future ordered workers must
agree on `Lanes` (default `64`); changing it requires draining or migrating existing rows.
The enlistment reports no scheduling support until matching due-row workers are implemented.
Registering this integration alone does not send persisted rows to a broker.

## Atomic consumer inbox

Register `AddRelayerMySqlOutbox` and configure the consumer boundary before any other
middleware. Deduplication is opt-in with `inbox: true`:

```csharp
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
with either native `UseAffectedRows` setting. Retaining inbox keys retains deduplication;
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
dispatch; it does not provide EF Core integration, saga persistence, batch consumers, ordered
workers or inbox retention jobs. With `inbox: false` it still owns the atomic business/outbox
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
whose claim has not expired. A broker failure records the error and increments attempts; a
completion error after a successful send leaves attempts unchanged. An expired or interrupted
send may be delivered again: this is at-least-once delivery, not an atomic broker/database commit.
Size `ClaimDuration` for the slowest expected database and broker operation.

Queue overflow or a stopped worker logs an error and leaves rows durable without failing an
already committed transaction. Graceful shutdown drains accepted work until the host's shutdown
deadline; cancellation leaves unfinished rows durable. Ordered rows and future-due rows are not
queued. This worker does not poll after restart, retry failed rows, or recover overflow: recovery
remains planned in [#345](https://github.com/thomhurst/Relayer/issues/345). Use it only with an
application-owned recovery path until that worker ships; enqueue/notification alone does not
guarantee eventual delivery after a crash.

## Remaining integration

Ordered lane ownership is tracked by [#344](https://github.com/thomhurst/Relayer/issues/344),
and recovery/poison/retention by [#345](https://github.com/thomhurst/Relayer/issues/345).
Atomic inbox integration is implemented above; ordered dispatch and recovery remain planned.
The schema helper is not exposed through `dotnet relayer db` yet. Full SQL Server and MySQL
parity remains tracked by [#41](https://github.com/thomhurst/Relayer/issues/41).
