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
