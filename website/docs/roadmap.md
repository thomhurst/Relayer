---
title: Roadmap
description: "What is implemented in Relayer today and the phases that follow."
---

Relayer is in early development. Track progress in [GitHub issues](https://github.com/thomhurst/Relayer/issues).

## Implemented

- [NuGet packaging and tag-triggered releases](contributing/releases.md), with shared metadata, git-derived versions, SourceLink, portable symbols and OIDC publishing. This infrastructure does not mean packages have been released.
- Configuration, named transports, routes and consumers, with startup validation of declarations, options and endpoint capabilities, including [declared scheduling and transaction requirements](guides/sending.md).
- Source-generated message registration, handler registration and dispatch, and System.Text.Json serialization.
- Required CI publishes and runs the Native AOT smoke app with reflection-disabled JSON, generated in-memory handler round trips, messaging spans and creation-context propagation assertions. Trimming and AOT warnings fail the build.
- Compile-time analyzers and a code fix ([diagnostics](reference/diagnostics.md)).
- The scoped send pipeline, send middleware and outbox enlistment hooks.
- Provider-owned tracing and direct single/batch send tracing, integrated with the in-memory adapter's delivery and native settlement spans. PostgreSQL outbox enqueue, dispatch and consumer creation links are implemented.
- The [durable outbox store contract](reference/outbox-stores.md), with bounded reads, fenced claims and an optional keyed health adapter, plus the [PostgreSQL read/claim implementation](reference/postgres-store.md).
- [PostgreSQL schema scripts and database CLI](reference/postgres-schema.md), including an application-owned EF Core migration path.
- [EF Core transaction enlistment for PostgreSQL](reference/ef-core-outbox.md): implicit saves, explicit transactions, atomic rollback, multiple contexts and one batched insert per transaction.
- [Ordered PostgreSQL dispatch and lane leases](reference/ordered-dispatch.md).
- [EF Core consumer inbox](reference/ef-core-inbox.md): PostgreSQL deduplication, concurrent redelivery, atomic business/inbox/outbox commits and duplicate metrics. Automatic for the selected outbox context, opt-in for inbox-only consumers.
- [PostgreSQL ADO.NET and Dapper transactions](reference/postgres-outbox.md): scoped enlistment, owned message copies and one atomic batched insert on commit.
- [Unordered PostgreSQL direct dispatch](reference/direct-dispatch.md): background sends immediately after EF commit, fenced claims, native transport batches and fenced batch deletion. Register [leased recovery and cleanup](reference/postgres-maintenance.md) for unordered crash recovery and bounded dead/inbox retention. Poison-row handling remains planned.
- The consume pipeline and the native receive SPI.
- [Bounded partition completion watermarks](transports/partition-completion.md) for transport authors, with opaque native checkpoints and ownership-fenced tokens. Key scheduling, backpressure and bounded drain remain planned.
- Explicit [message upcaster chains](guides/messages.md#versioning-and-unknown-types), composed at startup for single-message and batch handlers.
- [Service Bus native sending and topology](transports/service-bus.md): named registration, owned/caller-owned clients, queues/topics, byte-limited native batches, scheduling, native callbacks, explicit session verification and non-destructive provisioning. [Receive primitives](transports/service-bus-receive.md) are available separately; hosted receiver composition remains planned.
- Generated batch handler execution, durable partial-failure decisions, and validated native/client-side receive executors for transport authors.
- Typed failure classification, in-process retries, delayed redelivery and dead-letter copies with error history.
- Manual settlement requests through `ISettlementFeature`, with exception precedence and durable capability fallbacks.
- Declared topology with validate and provision modes, and JSON export.
- The [in-memory transport](transports/in-memory.md), with native queue and partitioned log semantics.
- The [Kafka adapter](transports/kafka.md), with named producers, serial consumer groups, native batches and checkpoints, typed tombstones, durable dead-letter copies, offset and timestamp replay, application logging, receive telemetry, optional native connectivity and receiver liveness health adapters, and Testcontainers conformance. Parallel partition scheduling remains planned.
- The packaged [transport conformance suite](transports/conformance.md), passing in-memory queue and log modes, with capability skips and native fixture hooks for third-party adapters.
- The [test harness and standalone handler context](guides/testing.md), with logical send and native dispatch observations, deterministic in-memory idle detection, reset and provider isolation.
- The [performance regression gate](contributing/performance.md), covering end-to-end in-memory send/receive, concurrent delivery, allocation attribution and 100-handler composition. Raw broker comparisons remain planned with each adapter.
- Core tracing primitives, single and batch delivery spans, native settlement wrappers, consume metrics and generated failure logs. Kafka send, serial receive and batch-commit instrumentation are implemented; PostgreSQL outbox trace propagation, pending deltas and dispatch lag are implemented; the other native receive adapters remain planned.
- The optional [health integration](guides/health-checks.md), with connectivity, consumer liveness and backlog checks. Kafka provides native adapters; the outbox adapter accepts application-provided durable stores. Other native broker adapters remain planned.

## Phases

| Phase | Scope | Exit criteria |
|---|---|---|
| 0 | Core abstractions, pipeline, source generator and analyzers, in-memory transport, test harness, telemetry | Handler round trip under Native AOT; benchmarks in CI |
| 1 | Kafka and Azure Service Bus transports; failure pipeline; DLQ and redrive | Broker-restart and rebalance chaos tests pass |
| 2 | EF Core and PostgreSQL outbox and inbox (lanes, direct dispatch, cleanup) | Crash and duplicate tests with multiple instances pass; at most 5% overhead |
| 3 | SQS/SNS and RabbitMQ; SQL Server and MySQL outbox; topology export; multi-tenancy | Every transport passes the conformance suite |
| 4 | Sagas, request/response, change-data-capture dispatch, more transports | — |

The [design document](https://github.com/thomhurst/Relayer/blob/main/docs/design.md) describes each planned feature in detail.
