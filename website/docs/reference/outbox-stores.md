---
title: Durable outbox store contract
description: "Durable row reads, fenced claims, memory ownership and optional health integration for outbox provider authors."
---

The core package defines `IOutboxStore`, `OutboxRecord`, `OutboxReadRequest` and `OutboxClaim` for database
integration authors. These contracts, the optional health adapter and [PostgreSQL schema scripts](postgres-schema.md)
are implemented, together with [PostgreSQL durable reads and fenced claims](postgres-store.md)
and [EF Core transaction enlistment for PostgreSQL](ef-core-outbox.md).
[Unordered PostgreSQL direct dispatch](direct-dispatch.md) is available for EF commits.
[Ordered PostgreSQL dispatch and lane leases](ordered-dispatch.md) are implemented separately.
[Leased recovery and cleanup](postgres-maintenance.md) are available separately.
[Inbox deduplication](ef-core-inbox.md) is implemented
through the EF Core consumer boundary, separately from the read/claim store contract.

## Register a named store

Use normal keyed DI with an explicit, case-sensitive name. This example assumes an application-supplied
`OrdersOutboxStore` that implements `IOutboxStore`; Relayer does not ship that type:

```csharp
using Relayer;
using Relayer.HealthChecks;

services.AddKeyedScoped<IOutboxStore, OrdersOutboxStore>("orders-store");
services.AddRelayerOutboxHealthProvider("orders-store");
services.AddRelayerHealthChecks(new RelayerHealthCheckOptions
{
    OutboxNames = ["orders-store"]
});
```

The optional `Relayer.HealthChecks` package adapts the store's pending count to health thresholds. The core
does not depend on health abstractions. Register stores separately from scoped `IOutboxEnlistment` services:
enlistment writes through the application's native transaction, while `IOutboxStore` serves dispatch and health.
Neither registration enables a background dispatcher.

The send pipeline calls `IOutboxEnlistment.EnlistAsync(OutboxRecord, CancellationToken)` with the original
identity, wire type and creation time. The enlistment takes owned copies of borrowed payload memory before
returning and persists buffered rows before transaction commit. These metadata fields remain available in raw
envelope mode and with custom header names. New records have no dispatch attempts, error or caller-supplied lane.

## Rows and bounded reads

An `OutboxRecord` contains the original UUIDv7 ID, named transport, stable wire message type including version,
serialized `OutgoingMessage`, creation time, optional lane, attempt count, last error and optional tenant ID.
Never substitute a CLR type name or generate a new envelope when reading a row. Preserve native header names
and values, the CloudEvents identity and creation context, body, ordering key and delivery time. Reject native
callbacks before transactional writes; callbacks cannot be persisted.

`ReadPendingAsync(request, destination, cancellationToken)` fills caller-owned `Memory<OutboxRecord>` and returns
the number of rows written. A zero-length buffer returns zero. `OutboxReadRequest` selects one nonnegative lane,
or null for unordered rows, a fixed time instant and an optional exclusive `AfterId`. Results use ascending UUIDv7
ID order. A cursor bounds a scan; a later sweep must start without a cursor to revisit rows that become eligible.

Rows are eligible when they are pending, not parked or dead, their `DeliverAt` is null or at most the query time,
and they have no live claim. A claim expires when its expiry is at most the query time. Compare timestamps as
UTC instants. Reads do not claim rows, so the dispatcher must claim again before sending. A read or claim does
not establish ordering: ordered dispatch also needs a lane lease protocol, such as the [PostgreSQL worker](ordered-dispatch.md).

Providers may reuse payload buffers. Borrowed body, headers and ordering-key memory remain valid until the
next read on that store instance or its disposal. Finish all sends before another read. Callers own the row slots
and clear the slots after use. Store instances need not support concurrent calls.

## Claims and outcomes

`OutboxClaim` combines the message ID with a new nonempty fencing token for one attempt. Never reuse a token.
`TryClaimAsync` checks current eligibility atomically and stores the token and expiry. The expiry must be later
than the supplied current time. An absent or ineligible row returns false without mutation.

`RenewClaimAsync` extends a matching live claim without shortening its expiry. `CompleteAsync` deletes a sent
row. `RecordFailureAsync` increments attempts, persists the error and releases the claim in one operation.
Repeating a failure operation with the released token must not increment attempts again. All three operations
require a matching token and a claim that is still live at the supplied time; otherwise they return false without
mutation. Empty IDs/tokens, invalid claim expiry and null error text are invalid arguments. Poison-row movement
and key parking belong to a separate integration.

All mutations are atomic and durable before completion. Observe cancellation and propagate database errors.
Cancellation or an error can occur after a database commit; callers must tolerate that uncertainty and
at-least-once delivery. An expired dispatcher cannot update a row reclaimed with a newer token. Fencing database
updates does not undo a broker send, so duplicate delivery after a crash or lost claim remains possible.

## Eligible backlog

`GetPendingCountAsync(now, cancellationToken)` uses the same eligibility rules across all lanes without an ID
filter. Count rows, not keys or lanes. Include schedules exactly at `now` and expired claims; exclude future
schedules, live claims and parked/dead rows. Return zero only after a successful query finds no eligible rows.
Return null for an unavailable count and propagate query errors rather than returning zero.

The [health adapter](../guides/health-checks.md) forwards the query time and cancellation token without changing
the result. Provider implementations must verify these predicates and threshold inputs against real database
fixtures; the core adapter's unit tests do not establish database correctness.

## Shared provider tests

Repository integration tests share `tests/Shared/OutboxStoreContractTests.cs`. Link that source into a provider's
TUnit test project, derive a concrete class from `OutboxStoreContractTests`, and add `[InheritsTests]` so TUnit
discovers the inherited cases. Implement `CreateFixtureAsync` with a real database deployment; the PostgreSQL
example is `tests/Relayer.Outbox.PostgreSql.Tests/SharedStoreContractTests.cs`.

Each fixture supplies independent store instances against the same database, native row seeding and async
cleanup. Keep database setup and seeding in the provider fixture. The inherited cases call only `IOutboxStore`
and check bounded UUIDv7 scans, lane and schedule eligibility, backlog counts, competing claims, renewal,
failure recording, exact-expiry recovery, stale-owner fencing, completion, invalid inputs and cancellation.
Read, count and claim eligibility also run at one UTC instant expressed with a nonzero offset.

### Recovery and retention

Link `tests/Shared/OutboxMaintenanceContractTests.cs` and derive a concrete `[InheritsTests]` class from
`OutboxMaintenanceContractTests` to reuse the maintenance cases. The PostgreSQL adapter is
`tests/Relayer.Outbox.PostgreSql.Tests/SharedMaintenanceContractTests.cs`. This test fixture is separate from
`IOutboxStore`: each provider supplies its own native maintenance worker, lease protocol and database setup.

The fixture seeds original message IDs in `ce_id` headers with body bytes `7, 8, 9`, creates independent workers
against the same database, and reports durable row counts and attempt counts. Configure recovery with a one-hour
minimum row age, a caller-selected batch size, 30-day dead-row retention and seven-day inbox retention. Either
retention can be disabled independently. Expose one bounded sweep, one bounded cleanup and explicit lease
release through the worker adapter. A sweep reports rows attempted, including failed sends; cleanup reports
rows deleted across the two retention collections.

The shared cases verify age, schedule, claim, lane and parked-row eligibility, failure isolation, cursor wrap
under continuing arrivals, bounded cleanup, idle lease exclusion and handoff, disabled retention, and preservation
of pending, parked and recent rows. Seed explicit timestamps and implement `MakeDueAndExpireClaims` by changing
durable eligibility; do not wait for wall-clock expiry. Keep real native transactions, claims and leases in the
worker. Release worker resources before disposing the database fixture, including after failed assertions.

### Inbox transaction boundaries

For inbox transaction boundaries, also link `tests/Shared/InboxContractTests.cs`, derive from
`InboxContractTests` and add `[InheritsTests]`. The PostgreSQL/EF adapter lives in
`tests/Relayer.Outbox.PostgreSql.Tests/InboxTests.cs`. Each fixture creates independent consumer hosts against
one database and reads committed business, inbox and outbox counts through a separate native connection.
Each consumer uses the real consume pipeline, exposes its instrumentation and invocation/send counts, and
supports hooks after business writes and before native settlement. The adapter must verify that the handler
hook runs inside its native transaction.

The shared inbox cases cover atomic commit before settlement, duplicate suppression and telemetry, identity
scoped by consumer, concurrent commit/rollback, handler failure, cancellation, retry/dead-letter rollback and
redelivery after acknowledgement loss. The contention hook must observe a native lock wait or transaction
conflict before releasing the first handler; elapsed time alone is not evidence of contention. Tests release
and drain concurrent deliveries before disposing their hosts and database.

These cases supplement provider-specific tests. Borrowed-memory ownership, native transaction retries,
configuration, scope disposal, ordered workers, connected-owner fencing and process-crash recovery need
additional native integration tests. Passing the shared cases alone does not establish those guarantees.
All three suites currently run against
PostgreSQL; MongoDB support remains planned.
