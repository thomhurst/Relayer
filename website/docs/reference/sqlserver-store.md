---
title: SQL Server durable store
description: "Explicit SQL Server outbox schema, bounded reads, fenced claims and backlog queries."
---

`Relayer.Outbox.SqlServer` implements the [durable store contract](outbox-stores.md) using
`Microsoft.Data.SqlClient` and SQL Server's native connection pooling. It supplies reads, atomic claims,
renewal, completion, failure persistence and eligible backlog counts. **Transactional enqueue, EF Core and
ADO.NET/Dapper enlistment, inbox deduplication, dispatch workers, lane leases, poison-row movement and
maintenance remain planned for SQL Server.** Registering this package does not enable those features or
make `ISender` transactional. PostgreSQL's worker registrations do not accept this provider.

**Native AOT and trimming are currently unsupported for SQL Server runtime operations.** SqlClient 7.1.1
retains reflection in authentication, configuration and diagnostics. Store construction and registration
carry `RequiresUnreferencedCode` and `RequiresDynamicCode` diagnostics; see
[the upstream Native AOT issue](https://github.com/dotnet/SqlClient/issues/1947).
Schema SQL/options remain AOT safe. This restriction is specific to this optional native provider.

## Deployment and registration

Write the initial schema with your application's deployment tooling. Initial creation is idempotent and
does not validate or upgrade an existing table. Schema evolution and rollback remain application-owned.
This script creates only an outbox table and its pending index; it does not create inbox, dead or lease tables.

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
reject native callbacks, which cannot be persisted. This package currently provides no enqueue API.

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

The benchmark creates and removes its own table. Results include SqlClient's allocations and I/O;
they are diagnostic and do not establish the direct-send/receive throughput budget. Relayer reuses
UUID buffers, header entries, payload buffers and boxed parameter values; native SqlClient commands,
parameters, readers and decoded strings remain driver allocations.
