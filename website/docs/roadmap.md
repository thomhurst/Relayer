---
title: Roadmap
description: "What is implemented in Relayer today and the phases that follow."
---

Relayer is in early development. Track progress in [GitHub issues](https://github.com/thomhurst/Relayer/issues).

## Implemented

- [Kafka and Service Bus consumer failure circuits](guides/failure-handling.md#consumer-circuit-breaking): native Kafka pause/resume and Service Bus processor stop/drain/restart, retained failures, one half-open probe and endpoint state metrics. SQS and RabbitMQ integrations remain planned.

- [PostgreSQL CDC dispatch](reference/postgres-cdc.md): native logical replication wakes unordered recovery; persistent slots and startup catch-up retain at-least-once delivery. Maintenance handles retries and schedules. SQL Server CDC remains planned.

- [Scoped tenant header propagation](guides/multi-tenancy.md) for sends, requests, replies and single-message consumers. Native scoped tenant stores and authorized durable tenant metadata binding are supported; bounded dispatch remains planned.
- [Explicit saga process diagrams](guides/saga-diagrams.md), with immutable declarations and deterministic Mermaid export, independent of typed runtime transitions.
- [NuGet packaging and tag-triggered releases](contributing/releases.md), with shared metadata, git-derived versions, SourceLink, portable symbols and OIDC publishing. This infrastructure does not mean packages have been released.
- Configuration, named transports, routes and consumers, with startup validation of declarations, options and endpoint capabilities, including [declared scheduling and transaction requirements](guides/sending.md).
- Source-generated message registration, handler registration and dispatch, and System.Text.Json serialization.
- Optional [Protobuf serialization](guides/messages.md#protobuf) and [MessagePack serialization](guides/messages.md#messagepack) with explicit generated contracts, native buffer APIs and Native AOT smoke coverage, and [Avro/schema registry serialization](guides/messages.md#avro) with explicit generated contracts and startup preparation. Avro AOT/trimming support is limited by upstream schema preparation.
- Required CI publishes and runs the Native AOT smoke app with reflection-disabled JSON, generated in-memory handler round trips, messaging spans and creation-context propagation assertions. Trimming and AOT warnings fail the build.
- Compile-time analyzers and a code fix ([diagnostics](reference/diagnostics.md)).
- The scoped send pipeline, send middleware and outbox enlistment hooks.
- [Request/response](guides/request-response.md) with explicit endpoints, mandatory timeouts, bounded process-local correlation and ordinary reply durability. Arbitrary reply destinations and temporary queues remain unsupported.
- Provider-owned tracing and direct single/batch send tracing, integrated with the in-memory adapter's delivery and native settlement spans. PostgreSQL outbox enqueue, dispatch and consumer creation links are implemented.
- The [durable outbox store contract](reference/outbox-stores.md), with bounded reads, fenced claims and an optional keyed health adapter, plus the [PostgreSQL read/claim implementation](reference/postgres-store.md).
- [SQL Server durable store primitives](reference/sqlserver-store.md): explicit schema, UUIDv7-ordered reads, atomic fenced mutations and eligible counts. SQL Server ADO.NET/Dapper transactional enqueue, opt-in atomic consumer inbox and commit-triggered unordered dispatch are implemented; EF Core, ordered workers, recovery and maintenance remain planned.
- [MySQL durable store, transactional enqueue and direct dispatch](reference/mysql-store.md), with InnoDB schema SQL, network-order UUIDs, shared store/dispatch contract suites and ADO.NET/Dapper transaction ownership. Opt-in atomic ADO.NET/Dapper consumer inbox is implemented; ordered dispatch and recovery/retention remain planned.
- [PostgreSQL schema scripts and database CLI](reference/postgres-schema.md), including an application-owned EF Core migration path.
- [EF Core transaction enlistment for PostgreSQL](reference/ef-core-outbox.md): implicit saves, explicit transactions, atomic rollback, multiple contexts and one batched insert per transaction.
- [Ordered PostgreSQL dispatch and lane leases](reference/ordered-dispatch.md), including insert-sequence headers and version-guarded consumers.
- [EF Core consumer inbox](reference/ef-core-inbox.md): PostgreSQL deduplication, concurrent redelivery, atomic business/inbox/outbox commits, explicit transaction isolation, bounded conflict replay and duplicate metrics. Automatic for the selected outbox context, opt-in for inbox-only consumers.
- [Native tenant store selection](reference/tenant-stores.md): database/schema isolation through application-owned scoped factories, including incoming single-message EF consumers without a fallback tenant. Authorized durable tenant metadata binding is implemented; bounded tenant dispatch remains planned.
- [Saga state transitions](guides/sagas.md): explicit process state, PostgreSQL optimistic concurrency and atomic state/inbox/outbox commits using the existing EF Core boundary. [Durable saga timeouts](guides/saga-timeouts.md) compose typed messages, explicit wait correlation and existing transactional scheduled sends.
- [PostgreSQL ADO.NET and Dapper transactions](reference/postgres-outbox.md): scoped enlistment, owned message copies and one atomic batched insert on commit.
- [SQL Server commit-triggered unordered dispatch](reference/sqlserver-store.md#commit-triggered-unordered-dispatch): bounded owned buffers, native batches and fenced outcomes. SQL Server recovery, ordered dispatch and maintenance remain planned.
- [Unordered PostgreSQL direct dispatch](reference/direct-dispatch.md): background sends immediately after EF commit, fenced claims, native transport batches and fenced batch deletion. Register [leased recovery and cleanup](reference/postgres-maintenance.md) for unordered crash recovery and bounded dead/inbox retention. [Poison-row limits and key-scoped parking](reference/postgres-store.md#poison-rows-and-key-release) are implemented, with durable inspection, redrive and purge.
- The consume pipeline and the native receive SPI.
- [Bounded partition completion watermarks](transports/partition-completion.md) and the core [fallback partition scheduler](transports/partition-scheduler.md) for transport authors whose clients have no equivalent native runtime, with key ordering, bounded concurrency, backpressure and ownership-fenced drain. Kafka parallel receive remains planned and will wrap Dekaf's `RunPartitionedAsync`.
- Explicit [message upcaster chains](guides/messages.md#versioning-and-unknown-types), composed at startup for single-message and batch handlers.
- [Service Bus transport](transports/service-bus.md): named sending and queue/subscription processors, owned/caller-owned clients, native sessions, manual settlement, durable failure handling, telemetry, optional keyed health adapters and Testcontainers conformance. Native send batching, scheduling and non-destructive topology remain available; positive delayed abandon and transactions are not advertised. [Receive primitives](transports/service-bus-receive.md) support application-owned processors.
- [SQS native sending and queue topology](transports/amazon-sqs.md): named AWS SDK v4 clients, standard/FIFO queues, native timers, byte/count-limited batches, partial-failure responses, send telemetry and non-destructive topology. SNS topic publishing, native batch partial failures, raw SQS subscriptions, queue policies and create-only topology are also implemented. Receivers, full conformance and health adapters remain planned.
- [RabbitMQ connection and topology](transports/rabbitmq.md): named native connections, exchanges, queues, queue bindings and DLX arguments, non-destructive provisioning and passive existence checks with explicit validation limits. Confirmed sends, binary CloudEvents headers, send telemetry and verified optional delayed exchanges are implemented. Receive and health adapters remain planned.
- [RabbitMQ Streams connection and topology](transports/rabbitmq-streams.md): owned native stream systems, existence validation and create-only native stream specifications. Message execution remains blocked on native recovery of failed processing offsets.
- [Google Cloud Pub/Sub](transports/google-pubsub.md): native publishing, streaming pull, non-destructive topology, durable poison copies and emulator conformance, with a prepared-client Native AOT round trip. Native scheduling and metadata-preserving durable retries are unsupported.
- [NATS JetStream native connection and topology](transports/nats.md): named native options, owned connections and non-destructive stream/consumer provisioning. Message execution and full read-only configuration equivalence checks remain planned; the native byte-backed header API is an upstream prerequisite for allocation-free sends.
- Generated batch handler execution, durable partial-failure decisions, and validated native/client-side receive executors for transport authors.
- Typed failure classification, in-process retries, delayed redelivery and dead-letter copies with error history. [Service Bus dead-letter management](transports/service-bus.md#inspect-and-redrive-dead-letters) and [Kafka dead-letter management](transports/kafka.md#inspect-and-redrive-dead-letters) provide bounded peek, filtered redrive and native purge; optional [authorized HTTP endpoints](transports/service-bus.md#optional-http-management) are available through `Relayer.AspNetCore`.
- Manual settlement requests through `ISettlementFeature`, with exception precedence and durable capability fallbacks.
- Declared topology with validate and provision modes, JSON export and [AsyncAPI 3.0.0 export](guides/topology.md#asyncapi-export) using source-generated JSON contract schemas. Protocol bindings and custom wire-format schemas remain unsupported.
- The [in-memory transport](transports/in-memory.md), with native queue and partitioned log semantics.
- The [Kafka adapter](transports/kafka.md), with named producers, serial consumer groups, native batches and checkpoints, typed tombstones, durable dead-letter copies, native consumer circuit breaking, offset and timestamp replay, independent startup-prepared JSON schema registries, application logging, receive telemetry, optional native connectivity and receiver liveness health adapters, and Testcontainers conformance. Parallel partition scheduling and dynamic schema discovery remain planned.
- The packaged [transport conformance suite](transports/conformance.md), passing in-memory queue and log modes, Kafka's supported cases and Service Bus queue/session cases, with capability skips and native fixture hooks for third-party adapters.
- The [test harness and standalone handler context](guides/testing.md), with logical send and native dispatch observations, deterministic in-memory idle detection, reset and provider isolation.
- The [performance regression gate](contributing/performance.md), covering end-to-end in-memory send/receive, concurrent delivery, allocation attribution and 100-handler composition. Kafka and Service Bus include raw-client send/receive benchmarks; production budget validation remains separate from local diagnostic runs.
- Core tracing primitives, single and batch delivery spans, native settlement wrappers, consume metrics and generated failure logs. Kafka send, serial receive and batch-commit instrumentation, plus Service Bus process and native settlement instrumentation and Google Pub/Sub send/process and callback acknowledgement tracing, are implemented; PostgreSQL outbox trace propagation, pending deltas and dispatch lag are implemented; SQS and RabbitMQ receive adapters remain planned.
- The optional [health integration](guides/health-checks.md), with connectivity, consumer liveness and backlog checks. Kafka and Service Bus companions supply native connectivity and receiver-owned liveness. The outbox adapter accepts application-provided durable stores; SQS and RabbitMQ adapters remain planned.

## Native client prerequisites

- [Apache Pulsar](transports/pulsar.md) remains planned. The client assessment found upstream prerequisites for byte-backed UTF-8 properties and a verified Native AOT byte-message path. No Pulsar adapter or package implementation is available yet.

## Phases

| Phase | Scope | Exit criteria |
|---|---|---|
| 0 | Core abstractions, pipeline, source generator and analyzers, in-memory transport, test harness, telemetry | Handler round trip under Native AOT; benchmarks in CI |
| 1 | Kafka and Azure Service Bus transports; failure pipeline; DLQ and redrive | Broker-restart and rebalance chaos tests pass |
| 2 | EF Core and PostgreSQL outbox and inbox (lanes, direct dispatch, cleanup) | Crash and duplicate tests with multiple instances pass; at most 5% overhead |
| 3 | SQS/SNS and RabbitMQ; SQL Server and MySQL outbox; topology export; multi-tenancy | Every transport passes the conformance suite |
| 4 | Sagas, request/response, change-data-capture dispatch, more transports | — |

The [design document](https://github.com/thomhurst/Relayer/blob/main/docs/design.md) describes each planned feature in detail.
