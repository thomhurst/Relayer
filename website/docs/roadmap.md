---
title: Roadmap
description: "What is implemented in Relayer today and the phases that follow."
---

Relayer is in early development. Track progress in [GitHub issues](https://github.com/thomhurst/Relayer/issues).

## Implemented

- Configuration, named transports, routes and consumers, with startup validation of declarations, options and endpoint capabilities.
- Source-generated message registration, handler registration and dispatch, and System.Text.Json serialization.
- Required CI publishes and runs the Native AOT smoke app with reflection-disabled JSON, generated in-memory handler round trips, messaging spans and creation-context propagation assertions. Trimming and AOT warnings fail the build.
- Compile-time analyzers and a code fix ([diagnostics](reference/diagnostics.md)).
- The scoped send pipeline, send middleware and outbox enlistment hooks.
- Provider-owned tracing and direct single/batch send tracing, integrated with the in-memory adapter's delivery and native settlement spans. Outbox tracing remains planned.
- The [durable outbox store contract](reference/outbox-stores.md), with bounded reads, fenced claims and an optional keyed health adapter, plus the [PostgreSQL read/claim implementation](reference/postgres-store.md). Dispatch workers remain planned.
- [PostgreSQL schema scripts and database CLI](reference/postgres-schema.md), including an application-owned EF Core migration path.
- [EF Core transaction enlistment for PostgreSQL](reference/ef-core-outbox.md): implicit saves, explicit transactions, atomic rollback, multiple contexts and one batched insert per transaction. Dispatch and inbox remain planned.
- [PostgreSQL ADO.NET and Dapper transactions](reference/postgres-outbox.md): scoped enlistment, owned message copies and one atomic batched insert on commit.
- The consume pipeline and the native receive SPI.
- [Service Bus receive primitives](transports/service-bus-receive.md): the native callback binding and manual settlement view. The complete Service Bus transport remains planned.
- Generated batch handler execution, durable partial-failure decisions, and validated native/client-side receive executors for transport authors.
- Typed failure classification, in-process retries, delayed redelivery and dead-letter copies with error history.
- Manual settlement requests through `ISettlementFeature`, with exception precedence and durable capability fallbacks.
- Declared topology with validate and provision modes, and JSON export.
- The [in-memory transport](transports/in-memory.md), with native queue and partitioned log semantics.
- The [Kafka producer](transports/kafka.md), with named registration, route-derived keys, native batches, application logging and creation-context preservation. Consumer execution, health adapters and full broker conformance remain planned.
- The packaged [transport conformance suite](transports/conformance.md), passing in-memory queue and log modes, with capability skips and native fixture hooks for third-party adapters.
- The [test harness and standalone handler context](guides/testing.md), with logical send and native dispatch observations, deterministic in-memory idle detection, reset and provider isolation.
- Core tracing primitives, single and batch delivery spans, native settlement wrappers, consume metrics and generated failure logs. Kafka send instrumentation is implemented; native receive adapters and outbox instrumentation remain planned.
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
