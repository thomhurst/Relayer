---
title: PostgreSQL schema and scripts
description: "Create the shared outbox, dead outbox, inbox and lease tables with SQL or an application-owned EF Core migration."
---

Relayer implements PostgreSQL schema creation in `Relayer.Outbox.PostgreSql`, plus an EF Core migration
extension in `Relayer.Outbox.EntityFrameworkCore`. Schema creation does **not** enable durable sending,
transaction enlistment, dispatch, lease acquisition, poison-row handling or inbox deduplication.
Register [EF Core transaction enlistment](ef-core-outbox.md) separately to persist outgoing rows.
[Ordered dispatch and lease acquisition](ordered-dispatch.md) are implemented separately. Unordered dispatch, poison-row handling and inbox deduplication remain [planned](../roadmap.md).
SQL Server and MySQL scripts are not implemented.

## Print SQL

No NuGet packages are published yet. From a repository checkout, run:

```shell
dotnet run -c Release --project src/dotnet-relayer -- db script --provider postgres --schema messaging --prefix orders_
```

The packable `dotnet-relayer` tool exposes the same command as `dotnet relayer db script --provider postgres`
after installation. To build and install it locally:

```shell
dotnet pack src/dotnet-relayer -c Release -o .artifacts/packages
dotnet tool install dotnet-relayer --tool-path .artifacts/tools --add-source .artifacts/packages
```

Run `.artifacts/tools/dotnet-relayer db script --provider postgres` to print SQL. The installed tool on `PATH`
also supports `dotnet relayer db script --provider postgres`. SQL goes to stdout; invalid options and unsupported
providers produce stderr and a nonzero exit code without emitting SQL. Apply the output with your normal
PostgreSQL deployment tooling. Use a transaction if the deployment must be atomic.

For code that manages database deployment directly:

```csharp
using Relayer.Outbox.PostgreSql;

var sql = PostgreSqlOutboxSchema.CreateScript(new PostgreSqlOutboxOptions
{
    Schema = "messaging",
    TablePrefix = "orders_"
});
```

The defaults are schema `public` and prefix `relayer_`. An empty prefix is allowed. Schema, table and index
names are quoted as PostgreSQL identifiers, including embedded quotes, punctuation and Unicode. Each generated
identifier must fit PostgreSQL's 63 UTF-8 byte limit; invalid names fail before SQL is returned. All deployment
and [store registrations](postgres-store.md) must use the same schema and prefix.

## Tables and constraints

- `relayer_outbox` preserves the original UUID, transport, destination, stable message type, serialized headers
  and body, creation time, optional ordering key, lane, delivery time and tenant ID. Attempts default to zero;
  `last_error` stores failure details. Status defaults to `pending` and permits `pending` or `parked`.
- `relayer_outbox_dead` preserves the message and attempt/error data with a required `failed_at` timestamp.
  Dead rows live in this table, outside the eligible backlog.
- `relayer_inbox` uses `(consumer, message_id)` as its primary key and stores `processed_at`.
- `relayer_leases` uses `name` as its primary key and stores `owner` and `expires_at`.

Lane numbers use PostgreSQL `integer` to match the nonnegative `int` contract. Attempts cannot be negative.
Message IDs and claim tokens cannot be empty UUIDs. A claim token and its expiry must both be null or both be
present. The [PostgreSQL store](postgres-store.md) enforces conditional, live-token updates; columns and constraints alone
do not implement [fencing](outbox-stores.md#claims-and-outcomes).

The outbox has a partial `(lane, ordering_key, id)` index for parked-key barriers, a partial `(lane, id)` index for `status = 'pending'` and a partial `deliver_at` index for non-null
schedules. The inbox has a `processed_at` retention index. Primary keys create their normal unique indexes.

Creation uses `IF NOT EXISTS` for schemas, tables and indexes. Running the script again preserves existing data.
It creates missing objects; it does **not** repair incompatible tables or upgrade an older schema. Review existing
definitions before adopting tables with the same names. Future schema changes require explicit migrations.

## EF Core migration path

Use a PostgreSQL application migration with the Npgsql EF Core provider. Reference
`Relayer.Outbox.EntityFrameworkCore` and add this call to its `Up` method:

```csharp
using Relayer.Outbox.EntityFrameworkCore;
using Relayer.Outbox.PostgreSql;

protected override void Up(MigrationBuilder migrationBuilder)
{
    migrationBuilder.CreateRelayerPostgreSqlSchema(new PostgreSqlOutboxOptions
    {
        Schema = "messaging",
        TablePrefix = "orders_"
    });
}
```

The extension adds the same SQL as the CLI to the migration and lets EF own the transaction and migration
history. It rejects other providers. Relayer tables stay outside the application's EF model snapshot, so
no entity mapping, `DbSet`, `EnsureCreated` call or transaction interceptor is required. Keep schema evolution
in explicit SQL migration operations. There is no automatic destructive `Down`: choose a retention policy
before writing a rollback that drops durable messages. Back up durable data before a schema change.

## Eligible backlog predicate

A provider's count query across all lanes uses an explicit UTC instant:

```sql
SELECT count(*)
FROM "public"."relayer_outbox"
WHERE status = 'pending'
  AND (deliver_at IS NULL OR deliver_at <= @now)
  AND (claim_expires_at IS NULL OR claim_expires_at <= @now);
```

Schedules and expired claims exactly at `@now` are included. Future schedules, live claims, parked rows and
rows in the dead table are excluded. The real PostgreSQL tests seed these cases, then verify the health
integration at counts 9, 10, 19 and 20 with thresholds 10 and 20. This fixture validates the schema and predicate;
the [PostgreSQL store](postgres-store.md) implements this predicate in its backlog and claim queries.
[Unordered direct dispatch](direct-dispatch.md) uses these claims after EF commits; recovery polling and
ordered lane dispatch remain planned.
