---
title: PostgreSQL durable store
description: "Register PostgreSQL outbox reads, atomic claims, fenced outcomes and backlog health queries."
---

`Relayer.Outbox.PostgreSql` implements `IOutboxStore` over the [shared PostgreSQL schema](postgres-schema.md).
It supplies durable reads, atomic claims, renewal, completion, failure persistence and eligible backlog counts.
[EF Core transaction enlistment](ef-core-outbox.md) and [ADO.NET/Dapper transactions](postgres-outbox.md)
supply atomic enqueue. [Unordered direct dispatch](direct-dispatch.md) can send due rows immediately after
an EF commit. [Ordered dispatch with lane leases](ordered-dispatch.md) is available through separate
registration. Recovery polling remains planned.
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
It starts with little-endian 32-bit version `1` and a header count. Each entry contains a length-prefixed UTF-8 name
and a length-prefixed value. Values retain their bytes, including empty or malformed UTF-8 values. Duplicate names
and ordering survive storage. Corrupt lengths, unsupported versions and trailing bytes fail explicitly.

Do not populate this column with JSON or empty bytes. EF Core interceptors and PostgreSQL outbox transactions
write compatible rows through the shared codec and batch writer.
The store does not generate identities, serialize messages, or rewrite envelopes during reads.
