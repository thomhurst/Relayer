---
title: MySQL durable store
description: "MySQL schema, bounded durable reads, atomic claims and fenced outcomes."
---

`Relayer.Outbox.MySql` implements the [durable store contract](outbox-stores.md) using
MySqlConnector and MySQL 8.4+ InnoDB. It supplies bounded reads, atomic claims, renewal,
completion, failure persistence and eligible backlog counts.

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

The `headers` column uses the same versioned binary codec as the PostgreSQL store: little-endian
32-bit version `1` and count, followed by length-prefixed UTF-8 names and byte values. Empty values,
duplicate names and arbitrary value bytes are preserved. Invalid versions, lengths or trailing bytes
fail explicitly. Empty headers require the encoded version and zero count, not an empty BLOB.

## Remaining integration

Transactional enqueue and Dapper transaction integration remain planned in
[#289](https://github.com/thomhurst/Relayer/issues/289). Inbox deduplication, direct and ordered
dispatch, recovery, poison-row handling and retention workers remain planned in
[#291](https://github.com/thomhurst/Relayer/issues/291). This package currently creates only the
outbox table and registers read/claim primitives. It does not register `IOutboxEnlistment`, write
sends automatically, establish lane ownership or start workers. The schema helper is not exposed
through `dotnet relayer db` yet. Full SQL Server and MySQL parity remains tracked by
[#41](https://github.com/thomhurst/Relayer/issues/41).
