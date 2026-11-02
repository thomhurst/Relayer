---
title: PostgreSQL transactions and Dapper
description: "Commit business changes and buffered messages atomically through a PostgreSQL outbox transaction."
---

`Relayer.Outbox.PostgreSql` registers a scoped `PostgreSqlOutbox` and `IOutboxEnlistment`.
Messages sent in its active transaction are copied into owned buffers. `Commit` or `CommitAsync` writes
all messages in one PostgreSQL insert, then commits the native transaction containing your business changes.
No broker send occurs on this path. ADO.NET commit-triggered dispatch remains planned.
[Ordered PostgreSQL dispatch and lane leases](ordered-dispatch.md) are available through separate registration.
[Inbox deduplication](ef-core-inbox.md) is available through the EF Core consumer boundary.
Use [EF Core enlistment](ef-core-outbox.md) instead when EF owns the transaction.

## Registration

Deploy the [shared schema](postgres-schema.md) first. Keep schema, prefix and lane count identical across
writers and readers of the same tables. Changing the lane count requires draining existing rows first.

```csharp
using Relayer.Outbox.PostgreSql;

services.AddRelayerPostgreSqlOutbox(new PostgreSqlOutboxOptions
{
    Schema = "messaging",
    TablePrefix = "orders_",
    Lanes = 32
});
```

Resolve `PostgreSqlOutbox` and `ISender` from the same DI scope. Only one `IOutboxEnlistment` integration can
own sends in a scope; do not register this alongside EF Core enlistment. Each scope supports one transaction
at a time and must not be used concurrently. Registration does not create tables, register a read store,
start workers or take ownership of a data source.

`PostgreSqlOutboxOptions` has init-only properties. For configuration binding and startup validation, pass an
`IConfiguration` section to `services.AddRelayerPostgreSqlOutbox(configuration.GetSection("Outbox"))`.
New scopes read updated configuration through `IOptionsSnapshot<PostgreSqlOutboxOptions>`.
`LeaseDuration` and `MaxDispatchAttempts` do not configure enqueue; enqueue uses schema, prefix and lanes. Configure dispatch and recovery through their separate worker options.
To expose durable reads and backlog health, register the [keyed store](postgres-store.md) separately.

## ADO.NET and Dapper

```csharp
using Dapper;
using Npgsql;
using Relayer;
using Relayer.Outbox.PostgreSql;

// dataSource belongs to the application. outbox and sender belong to this DI scope.
await using var connection = await dataSource.OpenConnectionAsync(cancellationToken);
await using var transaction = await outbox.BeginTransactionAsync(connection,
    cancellationToken: cancellationToken);

await transaction.Connection!.ExecuteAsync(new CommandDefinition(
    "INSERT INTO orders (id) VALUES (@Id)", new { order.Id }, transaction,
    cancellationToken: cancellationToken));
await sender.SendAsync(new OrderPlaced(order.Id), cancellationToken: cancellationToken);
await transaction.CommitAsync(cancellationToken);
```

The result is a `DbTransaction` subclass. **Use `transaction.Connection` for business commands.** Npgsql's
`NpgsqlCommand.Transaction` accepts only a native `NpgsqlTransaction`; assigning this wrapper to a command
created from the original `NpgsqlConnection` fails. The returned connection view creates commands that unwrap
the transaction for Npgsql. It supports ordinary ADO.NET commands and Dapper's synchronous and asynchronous
commands, scalar results and readers, including PostgreSQL array parameters such as `WHERE id = ANY(@Ids)`.
Dapper is an application dependency, not a Relayer package dependency. `DbCommand.Connection` returns the
underlying `NpgsqlConnection` so Dapper retains native provider detection. This does not change command
creation: use `transaction.Connection` to create commands that can accept the wrapper transaction. Keep
connection and transaction lifecycle operations on the outbox wrapper even when inspecting a command's native connection.

You can also let the transaction own its opened connection:

```csharp
await using var transaction = await outbox.BeginTransactionAsync(dataSource,
    cancellationToken: cancellationToken);
// Use transaction.Connection for the business commands, then send and commit as above.
```

The connection overload requires an already open connection and leaves it open after completion.
The data source overload closes its opened connection on commit, rollback, failure or disposal, without
disposing the application-owned data source. Disposing the borrowed connection view does not dispose the native
connection. Both overloads accept an optional native `IsolationLevel` unchanged.

## Durability and recovery

- `SendAsync` enlists while this transaction is active. Outside it, `SendAsync` uses the normal send path.
  `SendViaOutboxAsync` requires an active transaction; `SendNowAsync` deliberately bypasses the outbox.
- Serialized bodies, header names, duplicate headers, header bytes and ordering keys are copied before
  `EnlistAsync` completes. The original UUIDv7 ID, stable wire type, creation time, tenant and `DeliverAt` persist.
  Ordered rows use the shared FNV-1a lane calculation; unordered rows have a null lane. Large transactions
  use a fixed number of array parameters rather than one parameter set per message.
- Rollback, disposal of an uncommitted transaction and scope disposal discard the buffer and roll back business
  work. A failed or cancelled commit ends the unit; never retry `Commit` on that wrapper. Start a new transaction
  and retry the whole business unit. The scope can be reused after completion; disposal of an older wrapper
  cannot discard the next transaction's rows.
- A lost connection or cancellation during native commit can leave the outcome unknown. Inspect durable business
  state using an application idempotency key before retrying; no client can undo an acknowledged server commit.
- Native broker callbacks are rejected before persistence because delegates cannot survive storage.
  Headers and ordering remain available. Scheduled sends through `ISender` require a matching [maintenance worker](postgres-maintenance.md) for unordered rows or [ordered dispatcher](ordered-dispatch.md) for ordered rows; commit-only dispatch cannot deliver future rows. This database transaction does not enlist a broker's
  native transaction or change its delivery guarantees.

Commit, rollback and disposal must go through the returned transaction. Do not close the original connection,
issue SQL transaction-control commands, or obtain and complete a separate native transaction behind the wrapper.
Savepoints and nested transactions are unsupported and fail explicitly through the wrapper API: rolling back
business changes to a savepoint would also require rewinding the message buffer. `CommandBehavior.CloseConnection`
is rejected because it would close an active transaction. Use normal reader disposal and transaction disposal.

The buffers retain their peak capacity for reuse within the scope. Bound the size of business units to bound
memory usage. The default Npgsql data source includes array mappings; with `NpgsqlSlimDataSourceBuilder`, call
`EnableArrays()` before building the data source. Native AOT applications must use Npgsql's AOT-compatible
data source configuration and source-generated message serializers.
