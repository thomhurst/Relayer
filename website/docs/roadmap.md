---
title: Roadmap
description: "What is implemented in Relayer today and the phases that follow."
---

Relayer is in early development. Track progress in [GitHub issues](https://github.com/thomhurst/Relayer/issues).

## Implemented

- Configuration, named transports, routes and consumers, with startup validation of declarations, options and endpoint capabilities.
- Source-generated message registration, handler registration and dispatch, and System.Text.Json serialization.
- Compile-time analyzers and a code fix ([diagnostics](reference/diagnostics.md)).
- The scoped send pipeline, send middleware and outbox enlistment hooks.
- Provider-owned tracing primitives and direct single/batch send tracing, integrated with the in-memory adapter. Automatic consume/settlement and outbox tracing remain planned.
- The [durable outbox store contract](reference/outbox-stores.md), with bounded reads, fenced claims and an optional keyed health adapter. Database implementations and dispatch workers remain planned.
- The consume pipeline and the native receive SPI.
- Generated batch handler execution, durable partial-failure decisions, and validated native/client-side receive executors for transport authors.
- Typed failure classification, in-process retries, delayed redelivery and dead-letter copies with error history.
- Manual settlement requests through `ISettlementFeature`, with exception precedence and durable capability fallbacks.
- Declared topology with validate and provision modes, and JSON export.
- The [in-memory transport](transports/in-memory.md), with native queue and partitioned log semantics.
- Core tracing, consume metrics and generated failure logs; native transport and outbox instrumentation remains planned.
- The optional [health integration](guides/health-checks.md), with connectivity, consumer liveness and backlog checks. The outbox adapter accepts application-provided durable stores; native broker adapters remain planned.

## Phases

| Phase | Scope | Exit criteria |
|---|---|---|
| 0 | Core abstractions, pipeline, source generator and analyzers, in-memory transport, test harness, telemetry | Handler round trip under Native AOT; benchmarks in CI |
| 1 | Kafka and Azure Service Bus transports; failure pipeline; DLQ and redrive | Broker-restart and rebalance chaos tests pass |
| 2 | EF Core and PostgreSQL outbox and inbox (lanes, direct dispatch, cleanup) | Crash and duplicate tests with multiple instances pass; at most 5% overhead |
| 3 | SQS/SNS and RabbitMQ; SQL Server and MySQL outbox; topology export; multi-tenancy | Every transport passes the conformance suite |
| 4 | Sagas, request/response, change-data-capture dispatch, more transports | — |

The [design document](https://github.com/thomhurst/Relayer/blob/main/docs/design.md) describes each planned feature in detail.
