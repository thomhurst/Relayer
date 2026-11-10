---
title: Running repository tests
description: Release validation and how the core suite shares broker containers.
---

Run the core suite from the repository root with .NET SDK selected by `global.json`
and Docker running:

```bash
dotnet test -c Release --project tests/Relayer.Tests
```

Tests run in parallel with no concurrency limiter. Broker containers are shared for
the whole test run instead of started per test. `SharedBrokers` starts each one on
first use, so a filtered run starts only what it needs, and removes them when the
run ends:

| Broker | Shared instance | What each test owns |
|---|---|---|
| RabbitMQ | one broker, and a second with the delayed-message plugin | a virtual host, through `RabbitMqTopologyTests.Broker.StartAsync` |
| Service Bus emulator | one emulator with its SQL Server | queues, topics and subscriptions it creates through the administration client |
| LocalStack | one container for SQS and SNS | uniquely named queues and topics |

A test that changes the whole broker, such as stopping it to simulate an outage, asks
for a dedicated one with `dedicated: true`. Keep those rare: each dedicated Service
Bus emulator brings its own SQL Server, and ten of them starting together exhaust
Docker's memory.

When you add a broker test, use the shared instance and name everything the test
creates uniquely, for example with `TestContext.Current!.Isolation`. Do not add
`[ParallelLimiter]` or `[NotInParallel]` to make a test pass.

## PostgreSQL outbox suite

`tests/Relayer.Outbox.PostgreSql.Tests` starts one PostgreSQL container per test run,
on the first test that needs it. `PostgreSqlFixture` creates a database on that server
for each test and drops it afterwards, so the suite runs at full parallelism without a
limiter. A test that queries server-wide views such as `pg_stat_activity` or
`pg_replication_slots` must scope the query to its own database, application name or
replication slot; name server-wide objects with `TestContext.Current!.Isolation`.

The suite opens a new connection for most commands. On macOS, Docker Desktop forwards
about 8,000 new connections per 30 seconds across the whole machine before it starts
closing them, and one run of this suite uses about a third of that. Repeating the
whole suite several times back to back can therefore fail with
`Exception while reading from stream` on every test. Run the tests that cover your
change instead, or wait 30 seconds between full runs. Linux hosts and CI are not
affected.
