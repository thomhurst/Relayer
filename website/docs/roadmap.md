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
- The consume pipeline and the native receive SPI.
- Typed failure classification, in-process retries, delayed redelivery and dead-letter copies with error history.
- Declared topology with validate and provision modes, and JSON export.
- The [in-memory transport](transports/in-memory.md), with native queue and partitioned log semantics.

## Phases

| Phase | Scope | Exit criteria |
|---|---|---|
| 0 | Core abstractions, pipeline, source generator and analyzers, in-memory transport, test harness, telemetry | Handler round trip under Native AOT; benchmarks in CI |
| 1 | Kafka and Azure Service Bus transports; failure pipeline; DLQ and redrive | Broker-restart and rebalance chaos tests pass |
| 2 | EF Core and PostgreSQL outbox and inbox (lanes, direct dispatch, cleanup) | Crash and duplicate tests with multiple instances pass; at most 5% overhead |
| 3 | SQS/SNS and RabbitMQ; SQL Server and MySQL outbox; topology export; multi-tenancy | Every transport passes the conformance suite |
| 4 | Sagas, request/response, change-data-capture dispatch, more transports | — |

The [design document](https://github.com/thomhurst/Relayer/blob/main/docs/design.md) describes each planned feature in detail.
