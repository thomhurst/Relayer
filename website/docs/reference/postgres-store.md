---
title: PostgreSQL durable store
description: "Register PostgreSQL outbox reads, atomic claims, fenced outcomes and backlog health queries."
---

`Relayer.Outbox.PostgreSql` implements `IOutboxStore` over the [shared PostgreSQL schema](postgres-schema.md).
It supplies durable reads, atomic claims, renewal, completion, failure persistence and eligible backlog counts.
[EF Core transaction enlistment](ef-core-outbox.md) and [ADO.NET/Dapper transactions](postgres-outbox.md)
supply atomic enqueue. [Unordered direct dispatch](direct-dispatch.md) can send due rows immediately after
an EF commit. [Ordered dispatch with lane leases](ordered-dispatch.md) is available through separate
registration. [Leased recovery and cleanup](postgres-maintenance.md) are available separately.
[Inbox deduplication](ef-core-inbox.md) is available through the EF Core consumer boundary.

## Registration

Apply the schema with your normal deployment tooling first. Keep the schema and prefix identical in deployment
and store registration. The data source belongs to your application; dispose it after its users stop.

```csharp
using Microsoft.Extensions.DependencyInjection;
using Npgsql;
using Relayer;
using Relayer.HealthChecks;
using Relayer.Outbox.PostgreSql;

await using var dataSource = NpgsqlDataSource.Create(connectionString);
services.AddRelayerPostgreSqlOutboxStore("orders", dataSource, new PostgreSqlOutboxOptions
{
    Schema = "messaging",
    TablePrefix = "orders_"
});
services.AddRelayerOutboxHealthProvider("orders");
services.AddRelayerHealthChecks(new RelayerHealthCheckOptions { OutboxNames = ["orders"] });
```

Resolve `IOutboxStore` with `GetRequiredKeyedService<IOutboxStore>("orders")` inside a DI scope. Names are explicit
and case-sensitive. Each scope has its own store. The registration snapshots schema/prefix, validates identifiers,
and does not take ownership of the data source. Health registration requires the optional `Relayer.HealthChecks` package.
Registration does not create tables, enable `IOutboxEnlistment`, or start workers.

## Reads and mutations

The store follows the [core contract](outbox-stores.md):

- Reads select one lane, or unordered rows with a null lane, in ascending PostgreSQL UUID order. They honor the
  buffer length and exclusive ID cursor. Body, header bytes, duplicate header names and their order are preserved.
- Reads and counts include due schedules and expired claims exactly at the supplied UTC instant. They exclude
  future schedules, parked rows and live claims. Counts span every lane. Dead rows reside in a separate table.
- Claiming uses one conditional `UPDATE`. A competing claim succeeds only after the prior claim expires.
- Renewal uses the greater of the existing and requested expiry. Renewal, completion and failure recording require
  the current token and an unexpired claim. Expired or replaced owners return false without mutation.
- Failure recording increments attempts, saves the error and releases the claim atomically. Repeating it with the
  released token returns false. Completion deletes only the currently claimed row.

Use UTC instants consistently. PostgreSQL timestamps have microsecond precision. Await each operation before
starting another on the same store, and finish using borrowed rows before another read. Database errors and
cancellation propagate; they never become an empty backlog. A cancelled mutation might already have committed.
The store does not establish native broker delivery guarantees or ordered lane ownership.

## Header storage

The `headers` column contains the provider's versioned binary format, shared with its EF Core transactional writer.
The codec is shared internally with the [MySQL store](mysql-store.md); corruption errors use provider-neutral
outbox header messages. The binary format is unchanged.
It starts with little-endian 32-bit version `1` and a header count. Each entry contains a length-prefixed UTF-8 name
and a length-prefixed value. Values retain their bytes, including empty or malformed UTF-8 values. Duplicate names
and ordering survive storage. Corrupt lengths, unsupported versions and trailing bytes fail explicitly.

Do not populate this column with JSON or empty bytes. EF Core interceptors and PostgreSQL outbox transactions
write compatible rows through the shared codec and batch writer.
The store does not generate identities, serialize messages, or rewrite envelopes during reads.

## Poison rows and key release

Dispatch stops retrying after `MaxDispatchAttempts` failed sends (default **10**).
Set this positive limit on the direct dispatch's `PostgreSqlOutboxOptions` and on the ordered,
maintenance and CDC worker options. Keep dispatchers for the same store consistent.
Broker success followed by database completion failure does not consume an attempt.

One fenced statement persists the final error and attempt count and copies the original identity,
body, headers, transport, destination, tenant and schedule into `relayer_outbox_dead` atomically.
Unordered rows leave the pending table. An ordered row remains as a `parked` barrier at its original
insert sequence: later rows with its key wait while unrelated keys continue. A rollback rolls back
the attempt, dead copy and barrier together.

```csharp
services.AddRelayerPostgreSqlDeadLetters("orders", dataSource, storeOptions);
var manager = serviceProvider.GetRequiredKeyedService<IDeadLetterManager>("orders");
var page = await manager.PeekAsync("orders", maxMessages: 20);
var details = page.Messages[0].Features.GetRequired<PostgreSqlDeadLetterDetails>();
await manager.RedriveAsync("orders", message => message.MessageId == selectedId, maxMessages: 1000);
```

Use the same application-owned data source, schema and prefix as the workers. `List()` exposes the
configured store as one source. Peek is non-destructive and pages by an opaque cursor.
`PostgreSqlDeadLetterDetails` exposes the owned original `OutboxRecord` and `FailedAt` timestamp.
Redrive restores pending rows for existing workers, resets attempts and clears the previous error;
it never sends directly. Ordered redrive preserves the original sequence before later same-key rows.
`PurgeAsync` permanently discards dead rows and their barriers, releasing later same-key rows without
replaying the failed row. Calls scan at most the requested batch (1–1000 rows); filtered redrive can
change fewer rows. The bound counts scanned rows, not matches. Each call starts at the lowest ID;
repeating the same filtered call does not advance past unmatched rows. Peek cursors only page inspection,
not redrive. Select a bound that includes the desired row (up to 1000); this API cannot selectively redrive
a row beyond that prefix. Drain earlier rows with unfiltered redrive, or explicitly discard them with
purge, before reaching deeper rows. Neither operation is an inspection-only way to advance the prefix.
Concurrent management calls skip locked dead rows. Filter errors and database failures roll back the entire batch.

An ordered dead row must still have its original parked row. A missing or changed barrier indicates
inconsistent durable state; redrive fails with the row ID and rolls back every change in that batch.
It never invents a new sequence or silently skips the inconsistency. Restore the original durable row
at its original sequence before retrying, or explicitly purge the dead row if it should be discarded.
A bounded purge also starts at the lowest ID and can discard earlier rows, so inspect that prefix first.

Dead retention excludes rows with an active parked barrier, retaining their inspection and redrive data
until explicit redrive or purge. Unordered dead rows remain subject to configured retention.
`relayer.outbox.dead_lettered` increments after a poison transition commits. A crash around commit
can lose a measurement; durable management queries remain the source of truth.
