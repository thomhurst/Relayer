# Relayer — Decision Record

This document records the architectural decisions behind Relayer: what we chose, why, and what we rejected. [`design.md`](design.md) describes *how* the system works; this document explains *why* it is shaped that way.

Each decision has a status:
- **Accepted** — in force.
- **Proposed** — under discussion.
- **Superseded** — replaced by a later decision, which is linked.

To change a decision, add a new entry that supersedes it rather than editing history.

| # | Decision | Status |
|---|---|---|
| [D1](#d1-wrap-native-clients-do-not-abstract-brokers) | Wrap native clients; do not abstract brokers | Accepted |
| [D2](#d2-target-net-10-and-later) | Target .NET 10 and later | Accepted |
| [D3](#d3-mit-licence-permanently) | MIT licence, permanently | Accepted |
| [D4](#d4-explicit-registration-source-generation-no-reflection) | Explicit registration, source generation, no reflection | Accepted |
| [D5](#d5-handlers-implement-ihandlert) | Handlers implement `IHandler<T>` | Accepted |
| [D6](#d6-native-access-through-a-feature-collection) | Native access through a feature collection | Accepted |
| [D7](#d7-stable-message-names-and-cloudevents-on-the-wire) | Stable message names and CloudEvents on the wire | Accepted |
| [D8](#d8-systemtextjson-source-generation-by-default) | System.Text.Json source generation by default | Accepted |
| [D9](#d9-at-least-once-delivery-plus-idempotency) | At-least-once delivery plus idempotency | Accepted |
| [D10](#d10-relayer-owns-the-failure-pipeline) | Relayer owns the failure pipeline | Accepted |
| [D11](#d11-handlers-may-settle-manually) | Handlers may settle manually | Accepted |
| [D12](#d12-outbox-dispatch-direct-for-unordered-lane-leases-for-ordered) | Outbox dispatch: direct for unordered, lane leases for ordered | Accepted; claim protocol superseded by [D29](#d29-durable-dispatch-claims-use-expiring-fencing-tokens) |
| [D13](#d13-kafka-gets-its-own-consumption-model) | Kafka gets its own consumption model | Accepted |
| [D14](#d14-transports-declare-capabilities-validated-at-startup) | Transports declare capabilities, validated at startup | Superseded by [D24](#d24-capabilities-are-valued-and-declared-per-endpoint) |
| [D15](#d15-opentelemetry-in-the-core) | OpenTelemetry in the core | Accepted |
| [D16](#d16-topology-is-declared-default-mode-is-validate) | Topology is declared; default mode is Validate | Accepted |
| [D17](#d17-testing-is-a-first-class-product) | Testing is a first-class product | Accepted |
| [D18](#d18-performance-budget-enforced-in-ci) | Performance budget enforced in CI | Accepted |
| [D19](#d19-dependency-policy) | Dependency policy | Accepted |
| [D20](#d20-sagas-are-out-of-scope-for-v1) | Sagas are out of scope for v1 | Accepted |
| [D21](#d21-repository-conventions) | Repository conventions | Accepted |
| [D22](#d22-the-kafka-transport-wraps-dekaf) | The Kafka transport wraps Dekaf | Accepted |
| [D23](#d23-a-generic-transport-spi-owned-by-the-core) | A generic transport SPI owned by the core | Accepted |
| [D24](#d24-capabilities-are-valued-and-declared-per-endpoint) | Capabilities are valued and declared per endpoint | Accepted |
| [D25](#d25-telemetry-follows-the-opentelemetry-messaging-span-model) | Telemetry follows the OpenTelemetry messaging span model | Accepted |
| [D26](#d26-generate-default-json-metadata-with-the-official-systemtextjson-generator) | Generate default JSON metadata with the official System.Text.Json generator | Accepted |
| [D27](#d27-configuration-declarations-are-independent-of-pipeline-execution) | Configuration declarations are independent of pipeline execution | Accepted |
| [D28](#d28-a-batch-attempt-owns-one-scope-and-one-direct-send-buffer) | A batch attempt owns one scope and one direct-send buffer | Accepted |
| [D29](#d29-durable-dispatch-claims-use-expiring-fencing-tokens) | Durable dispatch claims use expiring fencing tokens | Accepted |
| [D30](#d30-share-postgresql-schema-sql-with-application-owned-ef-migrations) | Share PostgreSQL schema SQL with application-owned EF migrations | Accepted |
| [D31](#d31-ef-core-buffers-sends-for-an-explicitly-owned-context-and-flushes-at-commit) | EF Core buffers sends for an explicitly owned context and flushes at commit | Accepted |
| [D32](#d32-postgresql-outbox-transactions-own-commit-and-expose-a-compatible-command-connection) | PostgreSQL outbox transactions own commit and expose a compatible command connection | Accepted |
| [D33](#d33-commit-triggered-dispatch-transfers-owned-buffers-to-a-named-worker) | Commit-triggered dispatch transfers owned buffers to a named worker | Accepted |
| [D34](#d34-ordered-postgresql-dispatch-drains-a-locked-lane-before-transfer) | Ordered PostgreSQL lane ownership and transfer | Accepted |

---

## D1. Wrap native clients; do not abstract brokers

**Context.** Existing libraries model every broker as a queue. That causes real bugs: for example, Kafka being "force fit" into a RabbitMQ shape (Wolverine #3134) and offsets only being committed after a restart (Brighter #2269). Users also lose access to native features.

**Decision.**
- Each transport wraps a native .NET client for its technology (see D22 for Kafka). Native send, receive, ordering, settlement, and dead-lettering semantics are kept unchanged.
- Native configuration objects are passed through, never re-modelled.
- Relayer adds cross-cutting concerns only.

**Consequences.**
- Portable features (retry, outbox, telemetry) are uniform across transports. Transport-specific behaviour stays transport-specific.
- Moving from one broker to another is *easier* but not free. That is intentional.

**Alternatives rejected.**
- A unified broker model, as in MassTransit or Brighter: it hides semantics and creates lowest-common-denominator bugs.

## D2. Target .NET 10 and later

**Context.** .NET 8 support ends in November 2026. Supporting older targets limits the APIs available for AOT and performance work.

**Decision.** Target `net10.0` (current LTS) and later, with the latest C# language version. The source generator targets `netstandard2.0`, as Roslyn requires.

**Consequences.** We can use modern BCL APIs freely. Users on .NET 8 cannot adopt Relayer.

## D3. MIT licence, permanently

**Context.** MassTransit v9 and MediatR moved to commercial licences, and NServiceBus is costly. Users are actively looking for permissive alternatives.

**Decision.** MIT. No dual licensing and no commercial-only features in this repository.

## D4. Explicit registration, source generation, no reflection

**Context.**
- Reflection and runtime code generation block Native AOT (MassTransit #4772) and cause slow cold starts (Wolverine #1577).
- Assembly scanning causes surprises: missed types (NSB #6967) and hidden process-wide state (Wolverine #3521).

**Decision.**
- Handlers, routes, and consumers are registered explicitly in code.
- An incremental source generator produces dispatch code, serializer contexts, and the message-type registry, plus an optional generated `AddHandlersFromThisAssembly()` that lists handlers explicitly.
- No reflection on the hot path. All runtime packages set `IsAotCompatible`, and CI runs a Native AOT smoke test.

**Consequences.**
- Startup is fast and the behaviour is debuggable.
- Users must reference the generator, which ships inside the `Relayer` package.

**Alternatives rejected.**
- Runtime scanning plus `Expression` compilation: incompatible with AOT.

## D5. Handlers implement `IHandler<T>`

**Context.** Convention-based handlers (any method named `Handle`) are concise but implicit and hard to analyse.

**Decision.**
- Handlers implement `IHandler<TMessage>` with `ValueTask HandleAsync(TMessage, MessageContext, CancellationToken)`.
- Batch handlers implement `IBatchHandler<TMessage>` and return a `BatchResult` that can report per-message failures.
- Each message or batch is handled in a fresh DI scope, so scoped services such as a `DbContext` just work (Brighter #1560).

**Consequences.** It is slightly more verbose than conventions, but compile-time analysis and tooling become simple.

**Alternatives rejected.**
- Convention-based method discovery. It could be revisited later as an opt-in generator feature.

## D6. Native access through a feature collection

**Context.** Wrapping must never block access to the native client.

**Decision.**
- `MessageContext` exposes a typed feature collection, following the ASP.NET Core `HttpContext.Features` pattern. Transports register features such as `IKafkaConsumeFeature`, and extension methods such as `context.Kafka()` make them easy to find.
- On the send side, `SendOptions` accepts typed callbacks that receive the native outgoing message.

**Consequences.** Everything native stays reachable. Feature lookup must be allocation-free, so we use a small typed array or dictionary, not `Dictionary<Type, object>` boxing per call.

## D7. Stable message names and CloudEvents on the wire

**Context.**
- Using CLR type names on the wire breaks on rename (Rebus #905) and forces services to share contract assemblies (Rebus #852).
- Users want interop with non-Relayer applications (Wolverine #1592) and several message types on one topic (confluent #1183).

**Decision.**
- `[Message("name", Version = n)]` defines the wire identity.
- The default wire format is CloudEvents in binary mode, with attributes in native headers.
- A raw mode supports legacy producers.
- Versions are upgraded through explicitly registered upcasters.
- Unknown types are dead-lettered with a reason.

## D8. System.Text.Json source generation by default

**Decision.**
- The default serializer is System.Text.Json with a source-generated context built from the declared message types.
- `IMessageSerializer` is pluggable, with Protobuf, Avro, and MessagePack in separate packages.
- The serializer API is buffer-based (`IBufferWriter<byte>` / `ReadOnlySequence<byte>`).

## D9. At-least-once delivery plus idempotency

**Context.** Exactly-once delivery across arbitrary systems is not achievable. Claims of it mislead users.

**Decision.**
- The guarantee is at-least-once delivery.
- The outbox makes sends atomic with business data.
- The inbox deduplicates by a stable message ID (UUIDv7).
- Exactly-once is offered only Kafka-to-Kafka, using native Kafka transactions.

## D10. Relayer owns the failure pipeline

**Context.** Retry and dead-letter behaviour is inconsistent and buggy across libraries: Brighter #3667 and #1519, Rebus #1154 and #1198, Wolverine #469, SlimMessageBus #454. Polly models in-process retries but cannot model native redelivery or settlement.

**Decision.**
- One pipeline: classify → immediate retry → delayed redelivery (native, or outbox fallback) → dead-letter → redrive.
- Deserialization and contract errors are permanent by default.
- Error metadata covers every attempt.
- The circuit breaker pauses the consumer natively.
- Sends from a failed attempt never escape.

**Consequences.** Relayer maintains its own small retry implementation. Users can still use Polly *inside* handlers.

## D11. Handlers may settle manually

**Decision.**
- Settlement is automatic by default.
- `ISettlementFeature` allows `DeadLetter(reason)`, `Retry(delay)`, and `Complete()` to be requested from a handler.
- Requests are deferred until handling, middleware and async scope disposal succeed. `Complete` also waits for buffered sends. A handler or middleware exception cancels the request and follows the normal failure policy; cancellation, disposal errors, flush errors and fallback errors propagate without settlement.
- One request is allowed per attempt; another request throws. Immediate retries and pooled deliveries start with no request.
- `Retry(delay)` requests a new delivery with the exact delay, without jitter or the exception policy's in-process retries. It uses native redelivery, scheduled copy, then durable scheduler in capability order. Unsupported runtime delays fail at the request call. `Retry` and `DeadLetter` discard buffered direct sends; applications remain responsible for transactional rollback and native side effects.
- Batch requests are per item. Reported failures override the corresponding requests, and a thrown batch exception overrides all requests. Mixed completing/non-completing outcomes with shared buffered sends fail the entire attempt because sends cannot be attributed to individual items.
- Manual outcomes preserve prior exception history without creating a fictitious exception. A dead-letter copy carries the reason in `FailureHeaders.DeadLetterReason` (`relayer.dead_letter.reason`).
- Native extras, such as ASB `Defer`, are reached through the transport feature.

## D12. Outbox dispatch: direct for unordered, lane leases for ordered

**Context.** Polling with `SKIP LOCKED` scans is slow and expensive (CAP #1599) and produces duplicates (CAP #1429). Per-replica cleanup storms are common (CAP #1811), and polling adds latency (SlimMessageBus #275).

**Decision.**
- **Enlistment:** EF Core interceptors, so no `SaveChanges` override is needed and multiple DbContexts are supported. For ADO.NET, a `DbTransaction` subclass, so the normal API is the correct API.
- **Unordered routes:** the committing instance dispatches immediately after commit, using a conditional claim on the row.
- **Ordered routes:** rows are hashed into lanes. Lanes are leased with heartbeats and read in ID order.
- The sweeper and cleanup are single-leader through the same lease table.
- A poison row parks only its own ordering key.
- The schema ships with indexes and migrations.

**Consequences.**
- Low latency, and correct with any number of instances.
- The lease table is an extra moving part, covered by chaos tests.

**Alternatives rejected.**
- Pure polling: too slow and too expensive.
- CDC only: too much operational burden for v1. It remains a later option.

## D13. Kafka gets its own consumption model

**Decision.**
- Key-ordered parallelism within a partition.
- Offsets are committed at the contiguous-completed watermark.
- Cooperative-sticky rebalancing, with drain-on-revoke and pause tracking per partition.
- Tombstones are first-class.
- Seek and replay API.
- Multiple clusters, each with its own schema registry.
- Client logs flow through `ILoggerFactory` (Dekaf supports this natively).

## D14. Transports declare capabilities, validated at startup

Superseded by [D24](#d24-capabilities-are-valued-and-declared-per-endpoint).

**Decision.**
- Every transport publishes `TransportCapabilities` (delayed delivery, dead-letter, ordering, batching, transactions, maximum message size).
- Each feature either uses the native mechanism or a documented fallback. Otherwise startup fails with a clear error. Behaviour is never silently degraded.

## D15. OpenTelemetry in the core

**Context.** Telemetry is missing or bolted on in many libraries (EasyNetQ #1609, SlimMessageBus #149, Rebus #1163, KafkaFlow #465).

**Decision.**
- One `ActivitySource` and one `Meter`, both named `Relayer`, following the OTel messaging semantic conventions.
- W3C trace context is propagated in native headers.
- The outbox captures trace context at enqueue time.
- Processing duration is measured after settlement.
- Immediate retries share one delivery duration. A retry that completes handling, scope disposal and buffered send flushing clears earlier failures from the duration's `error.type`; each failed attempt is still logged. Unrecovered failures retain the tag, including after durable dead-letter handling or subsequent delivery-token cancellation. A settlement error marks the duration as failed; delivery-token cancellation alone does not.
- Generated failure logs open an identity scope only around the log call. `LoggerFactory` allocates when it pushes an external scope, so successful deliveries must not open a Relayer scope. Failure scopes contain read-only snapshots rather than pooled context references, allowing providers to retain them for asynchronous export. Logging provider failures must not replace the delivery error or bypass durable failure handling.
- Enrichers are supported.

## D16. Topology is declared; default mode is Validate

**Decision.**
- Topology comes from the declared routes and consumers.
- Modes: `Provision`, `Validate` (the default, needing least privilege), and `None`.
- The topology can be exported to JSON, Bicep, or Terraform.

## D17. Testing is a first-class product

**Decision.**
- `Relayer.Testing` provides:
  - an in-memory transport with configurable capabilities;
  - a harness that sees messages through the outbox, waits deterministically for idle, resets, and supports multiple buses;
  - Testcontainers fixtures.
- A transport conformance suite is mandatory for every transport.
- No mutable process-wide statics.
- Relayer's own tests use **TUnit**.

## D18. Performance budget enforced in CI

**Decision.**
- Budget:
  - ≤ 5% throughput overhead versus the raw client on the receive and direct-send paths;
  - zero allocations per message from Relayer's own pipeline in steady state;
  - pipeline build under 50 ms for 100 handlers.
- BenchmarkDotNet suites run in CI, and a regression fails the build.
- Amended 2026-11: the regression gate is an advisory workflow, not part of the required `CI Passed` check. It runs on every push to `main`, on demand, and on pull requests labelled `performance` or that change the gate. Running three full benchmark passes on every PR was slow and failed for reasons unrelated to the change (harness builds against `main`, allocation noise), which blocked merges without catching regressions.
- Amended 2026-11 (#155): verified byte-identical comparison inputs make throughput and allocation differences inconclusive execution variation, with all timings, confidence bounds and allocation totals retained. Separate benchmark processes can produce separated confidence intervals without a source change. Changed or unverified inputs keep the 5% throughput and 0.5 B/op regression thresholds; invalid inputs and the absolute 50 ms startup budget still fail. This classification does not establish the raw-client or zero-allocation budgets. Only explicitly documented non-build tooling is excluded from verification; unknown inputs remain hashed.

## D19. Dependency policy

**Decision.**
- The core depends only on `Microsoft.Extensions.*` abstractions and the BCL.
- A transport package depends only on its native client, and supports **only the latest major version** of that client.
- Versions are managed centrally in `Directory.Packages.props`.

## D20. Sagas are out of scope for v1

**Decision.** Sagas and process managers ship later as `Relayer.Sagas`, built on the inbox and outbox. v1 focuses on a sound core.

## D21. Repository conventions

**Decision.**
- `Relayer.slnx` solution file.
- Central Package Management.
- Shared settings in `Directory.Build.props`.
- Nullable reference types on, and warnings treated as errors.
- Layout: `src/`, `tests/`, `benchmarks/`, `docs/`.
- CI on GitHub Actions.

## D22. The Kafka transport wraps Dekaf

**Context.** Confluent.Kafka wraps the native librdkafka library. That brings native binaries, interop overhead, logs that bypass `ILogger` (confluent #2386), and an open Native AOT gap (confluent #2146). Those conflict with D4 and D18.

**Decision.**
- `Relayer.Kafka` wraps [Dekaf](https://github.com/thomhurst/Dekaf), a pure C# Kafka client with no native dependencies, Native AOT coverage, and zero-allocation hot paths.
- Dekaf's `ProducerBuilder` and `ConsumerBuilder` are exposed to users unchanged, and its `ConsumeResult` is exposed through `IKafkaConsumeFeature`.
- Where Dekaf already provides a feature (retry and dead-letter topics, transactions, OpenTelemetry instrumentation), the transport reuses it rather than re-implementing it, as long as it fits Relayer's uniform failure pipeline and telemetry.

**Consequences.**
- No native binaries, and AOT works end to end on Kafka.
- Relayer.Kafka's release cadence follows Dekaf's.
- Consumer handler retries and durable dead-letter copies use the core failure pipeline. Dekaf's hosted retry/dead-letter helpers own dispatch and settlement, so they would bypass the portable classifier and durable-copy contract. The serial consumer foundation instead reuses native fetch batches, next-offset checkpoints, producer acknowledgments and explicit commits. Parallel scheduling and drain-on-revoke remain planned under D13 and D23.
- Until Kafka failure copies have a persistable representation for tombstones and null-valued headers, durable redelivery policies fail at startup even when a durable scheduler is registered. The Kafka consumer does not expose that scheduler to manual settlement either. In-process retries and acknowledged dead-letter copies remain available. This explicit limitation avoids accepting a policy that fails only after receiving native metadata.
- Users who need Confluent.Kafka specifically could be served later by a separate `Relayer.Kafka.Confluent` package. That is not planned.

**Alternatives rejected.**
- Confluent.Kafka: native dependency, AOT gap, and logging that bypasses `ILogger`.

## D23. A generic transport SPI owned by the core

**Context.**
- D1 splits the work correctly: the core owns cross-cutting concerns and each transport owns native behaviour. But `ITransport` only declared a lifecycle. The core had no way to hand a message to a transport, a transport had no way to feed the consume pipeline, and the failure pipeline (D10) had no way to ask a transport to settle.
- Without that seam, every transport would reimplement serialization, the CloudEvents envelope, trace propagation, retries, the dead-letter and delay fallbacks, outbox dispatch and telemetry, each slightly differently. Adding a technology would mean rewriting the basics.
- Log-based brokers (Kafka, Event Hubs, Kinesis, Pulsar, RabbitMQ Streams, JetStream) share one consumption model. Planning it inside `Relayer.Kafka` only (D13) would make each of them rebuild it.
- The SPI must not cost throughput or allocations (D18), and must not hide the native client (D1, D6).

**Decision.**
- The core owns everything that is the same on every broker. A transport owns the native client, the receive loop, the mapping to native messages, and settlement. The SPI is described in [design §2.4](design.md#24-transport-spi).
- **Generics carry native types through the core.** A transport provides a struct `IMessageBinding<TIncoming>` with static abstract members (body, header lookup, delivery count). The core's `ReceivePipeline<TIncoming, TBinding>` is therefore specialized per transport by the JIT and the AOT compiler, with direct, inlinable calls and no boxing. `TIncoming` is whatever the native receive callback provides, such as Dekaf's `ConsumeResult` or ASB's `ProcessMessageEventArgs`.
- **The core decides; the transport settles.** The pipeline returns a `Disposition` struct (`Complete`, `Redeliver(delay)`, `DeadLetter(info)`, or `SettledByHandler`), and the transport maps it to native settlement. The core never returns a disposition the endpoint's capabilities do not declare (D24); it performs every fallback itself, using the transport's send path. Transports contain no fallback code.
- **One send shape.** The core sends through `SendAsync(OutgoingMessage)` and `SendBatchAsync(ReadOnlyMemory<OutgoingMessage>)`. `OutgoingMessage` has the same fields as an outbox row, so direct sends, outbox dispatch, scheduled re-sends and dead-letter copies share one path.
- **Headers are read in place.** `WireString` holds a header value as the native client stores it (UTF-16 or UTF-8) and converts it only when user code asks. The transport's `HeaderNames` encodes its CloudEvents protocol binding. This replaces materializing headers into a dictionary per message.
- **Native access is generic too.** The pipeline exposes the native message as a pooled `NativeMessage<TIncoming>` feature, and each transport's `context.Kafka()`-style extension wraps it in a struct view. No per-transport feature class is allocated per message.
- **Builders are generic.** The core implements `RouteBuilder<TMessage, TRouteOptions>` and `ConsumerBuilder<TConsumerOptions>` once (handlers, inbox, failure policy, ordering keys). Transports supply option types and add typed extension methods on their own instantiation, so transport-specific settings cannot be applied to the wrong transport.
- **Log-based consumption lives in the core.** `PartitionScheduler<TIncoming, TBinding, TPartition, TPosition>` implements key-ordered parallelism, contiguous-watermark commits, backpressure, drain on revoke and pause tracking. A log transport implements `IPartitionSource<TPartition, TPosition>` (`Commit`, `Pause`, `Resume`). D13 still holds; its mechanisms are now reusable.
- The in-memory transport (phase 0) is the first SPI implementation, and the transport conformance suite is the SPI's executable contract.

**Consequences.**
- A new transport is a binding, a receive loop, a send method, capabilities, topology and option types. Everything else is inherited and behaves the same across transports.
- The SPI is public, so third-party transports use the same seam as first-party ones.
- `SendOptions.ConfigureNative<TNative>` is applied through `ApplyNative<TNative>(TNative)`, without a wrapping closure. Native configurators cannot be persisted, so a send that has them and is enlisted in the outbox fails immediately with an explanation instead of silently dropping them.
- `MessageContext.Headers` moves from `IReadOnlyDictionary<string, string>` to a lazy reader over `WireString` values.

**Alternatives rejected.**
- An abstract `TransportBase<TIncoming, TOutgoing>` with template methods: it couples transports to core internals through inheritance and makes the core harder to evolve. Small generic pieces composed by the transport keep the contract explicit.
- Instance-based binding interfaces: interface dispatch on every header read, and no specialization when `TIncoming` is a reference type. Static abstract members on a struct binding avoid both.
- A headers dictionary per message: it allocates on every receive and loses the native representation.
- Fallbacks implemented by each transport: they would diverge, which is the bug class D10 exists to prevent.

## D24. Capabilities are valued and declared per endpoint

Supersedes [D14](#d14-transports-declare-capabilities-validated-at-startup).

**Context.**
- D14 modelled capabilities as a `[Flags]` enum on the transport. Flags cannot carry limits that change behaviour: SQS delays up to 15 minutes, SQS visibility up to 12 hours, SQS batches of 10, broker message size limits. D14 also listed maximum message size, which a flag cannot express.
- Capabilities differ per endpoint, not per transport: an SQS FIFO queue orders by key and a standard queue does not, an ASB queue may or may not have sessions, and a RabbitMQ exchange may or may not use the delayed-message plugin.
- Some brokers have a native dead-letter queue but cannot dead-letter one message on demand with error metadata. SQS redrive moves a message only after it exceeds `maxReceiveCount`, and the RabbitMQ DLX carries only `x-death`.

**Decision.**
- Transports declare a `TransportCapabilities` record per endpoint through `GetCapabilities(endpoint)`. It includes:
  - `MaxDeliveryDelay` (native scheduled send; `null` when none);
  - `MaxRedeliveryDelay` (native delayed redelivery of a received message; `TimeSpan.Zero` when only immediate redelivery is possible; `null` when none);
  - `DeadLetter` (true only if the broker can dead-letter a specific message on demand and keep Relayer's error metadata);
  - `OrderedByKey`, `MaxBatchSize`, `Transactions`, `PauseResume`, `Replay`, `MaxMessageBytes`;
  - `ConsumptionModel` (`Queue` or `Log`).
- The record holds only what core features need to decide. Transport-specific features stay typed extensions (D6), so the record does not need to be extensible by transports.
- The core resolves each failure action against the endpoint's capabilities:
  - **Redelivery after a delay:** native redelivery if the delay is within `MaxRedeliveryDelay`; otherwise a scheduled re-send of a copy (same message ID, next attempt) if within `MaxDeliveryDelay`, then complete; otherwise an outbox row with `deliver_at`, then complete.
  - **Dead-letter:** native if `DeadLetter` is true; otherwise send a copy with error metadata to the consumer's dead-letter endpoint, then complete.
- Startup validates every route and consumer against its endpoint's capabilities, including the longest configured redelivery delay and the dead-letter path. If no native mechanism or fallback applies, startup fails with a clear error. Behaviour is never silently degraded.

**Consequences.**
- Re-sends and dead-letter copies are at-least-once. A crash between the re-send and the completion can produce a duplicate, which the inbox removes because the message ID is unchanged (D9).
- The in-memory transport can emulate any broker by declaring the same capability values.

## D25. Telemetry follows the OpenTelemetry messaging span model

Refines [D15](#d15-opentelemetry-in-the-core).

**Context.**
- D15 committed to the OpenTelemetry messaging semantic conventions, but design §8 listed spans (`outbox.enqueue`, `outbox.dispatch`, `receive`) and metrics (`relayer.process.duration`, `relayer.send.duration`) that the conventions do not define. Backends and dashboards built on the conventions would not recognize them.
- The conventions define five operation types (`create`, `send`, `receive`, `process`, `settle`), a fixed span kind for each, span names of the form `{messaging.operation.name} {destination}`, and a per-message *creation context* that consumers link to.
- The conventions are still in Development status, so details can change before they become stable.
- Native clients (Dekaf, the Azure SDK, the AWS SDK) have their own instrumentation, which can emit overlapping spans and write trace headers.

**Decision.**
- Relayer emits only operation types, span kinds, span names, attributes and metrics defined by the conventions. Anything Relayer adds uses the `relayer.` prefix. The full mapping is in [design §8](design.md#8-observability).
- **The outbox maps onto `create` and `send`.** Enqueue is a `create` span (`PRODUCER`) whose context is stored in the row as the creation context. Dispatch is a `send` span (`CLIENT`) that links to it. This replaces the `outbox.enqueue` and `outbox.dispatch` spans.
- **One creation context per message,** never modified after it is written. Core-made re-sends and dead-letter copies keep the original's.
- **Process spans link to the creation context.** For a single message with no ambient span, the creation context is also the parent, so producer and consumer share a trace; an option turns this off. With an ambient span, the process span is its child. Batch process spans use links only.
- **No `receive` spans.** Handlers are push-based from the application's point of view, so Relayer emits `process` spans and leaves pull-level spans to the native client's instrumentation.
- **Settlement gets `settle` spans,** named with the system's native operation (`complete`, `ack`, `commit` and so on), as children of the process span. Process duration therefore includes settlement, as D15 requires.
- Single and batch consumers use `ConsumePipelineBuilder.Tracing` with the provider-owned source and native endpoint metadata. Their optional native settlement callback runs after durable work and scope disposal, inside the process span. Batch callbacks receive each resolved disposition; successful manual settlement takes precedence over an earlier failure disposition. Single-message metric recorders remain rejected by batch composition.
- **Metrics use the convention instruments** `messaging.client.operation.duration`, `messaging.process.duration`, `messaging.client.sent.messages` and `messaging.client.consumed.messages`, in seconds. `relayer.process.duration` and `relayer.send.duration` are dropped.
- **Exceptions are logged, not recorded as span events.** Failed spans set `error.type` and status `Error`.
- Relayer pins the conventions version in one place, publishes it as the telemetry schema URL, and migrates through `OTEL_SEMCONV_STABILITY_OPT_IN` once messaging becomes stable.

**Consequences.**
- Messages from Relayer and non-Relayer producers or consumers correlate in any backend that follows the conventions.
- Each transport adapter implements its system's destination format and attributes, and must keep native instrumentation from overwriting the creation context. The conformance suite checks both.
- Span names and constant tags are precomputed per endpoint, so naming costs nothing per message. The zero-allocation budget (D18) is measured with no listener attached, and the cost with tracing on is benchmarked separately.
- A conventions change before stability may change Relayer's telemetry. Such a change is called out in the release notes.

**Alternatives rejected.**
- Relayer-specific span and metric names (`outbox.dispatch`, `relayer.process.duration`): backends would not recognize them, and users would need custom dashboards.
- Using the dispatch span's context as the outbox message's creation context: consumers would correlate with the dispatcher, not with the request that produced the message.
- Always parenting process spans on the creation context: it breaks when processing runs inside another span, and batches cannot have several parents.

## D26. Generate default JSON metadata with the official System.Text.Json generator

Refines [D8](#d8-systemtextjson-source-generation-by-default).

**Context.**
- Relayer discovers `[Message]` types and generates their `JsonSerializerContext` declaration. Roslyn 4.14 runs ordinary generators against the same input compilation, so the SDK's System.Text.Json generator cannot consume that declaration ([Roslyn generator design](https://github.com/dotnet/roslyn/blob/main/docs/features/source-generators.md)). Emitting only `[JsonSerializable]` attributes leaves the context unimplemented.
- JSON contracts depend on properties, constructors, attributes, base types and referenced member types, not just message names and versions. Caching solely by the discovered message set would retain stale metadata after a member edit.

**Decision.**
- Run the official System.Text.Json generator against an augmented compilation containing Relayer's context declaration. Forward only sources for that context; the SDK continues to generate user-declared contexts.
- Keep the compilation dependency so member and reference changes invalidate JSON generation. Accept the compile-time work on edits rather than silently using stale contracts. Do not retain a mutable driver or compilation in process-wide state.
- Pin the compile-time generator dependency centrally to the supported .NET 10 System.Text.Json version and ship it with its license and notices in the analyzer directory. It is not a runtime dependency. Validate SDK and packed-package consumers with reflection disabled and Native AOT when changing that version.
- Forward generator diagnostics, removing a source location only when it refers to the temporary declaration that is not part of the caller's input compilation.

**Consequences.**
- Default registration remains one explicit call, and JSON behavior comes from System.Text.Json rather than a second implementation of its contract rules.
- JSON generation currently reruns when the input compilation changes. A future optimization must preserve invalidation for the complete transitive JSON contract; filtering only message attributes is insufficient.

**Alternatives rejected.**
- Emit a context declaration for the SDK generator to finish: ordinary generators cannot see one another's output on the supported Roslyn baseline.
- Generate JSON contracts ourselves: duplicates System.Text.Json's constructor, converter and member-access rules and risks behavioral divergence.
- Require users to maintain a second list of all message types: adds ceremony to the default registration path and allows the lists to drift.

## D27. Configuration declarations are independent of pipeline execution

**Context.** Issue #6 originally depended on receive and send execution (#4 and #5). Both pipelines need declared routes and consumers to compose, while declaration and named Options validation need neither pipeline. The design also used both `Produce<T>` and `Send<T>` for a destination, despite the single routing API principle.

**Decision.**
- The core owns named, typed transport, route and consumer declarations, available before execution pipelines. Native transport packages wrap `AddTransport<TTransportOptions, TRouteOptions, TConsumerOptions>` in their own simple registration extensions.
- The single route declaration is `Send<T>` for any native destination. Sending versus publishing remains native routing, as in §3.4.
- Each message type has one route across all transports. Cross-broker fan-out is outside the default routing contract.
- Consumer `Handler<THandler>()` registration is source-generated, just like root handler registration. It adds every single-message and batch descriptor to the shared registry before freezing it, and records the handler on that consumer. The untyped `IConsumerBuilder` keeps generated registration independent of native option types; the callback's typed builder retains access to native `Options`.
- Transport option configuration uses named `OptionsBuilder<T>` and `IOptionsMonitor<T>`. Applications use generated binding for Native AOT rather than a generic reflection-based binder inside the core.
- A core hosted validator reports declaration and named-option errors together before later native transport hosted services start. Pipeline, native capability and topology checks remain responsible for their own execution-specific contracts.

**Consequences.**
- Configuration can ship independently, and the receive and send pipelines consume its immutable declaration lists during composition.
- Native option objects retain their own mutability semantics. Reload updates named options, not routing or topology, and cannot silently restart a native client.
- The current core example uses the generic transport-author seam; native registration extensions arrive with their adapters.

## D28. A batch attempt owns one scope and one direct-send buffer

**Context.** Batch handlers can report per-message failures, but a scoped `ISender`
belongs to the whole handler invocation. A direct send cannot be attributed to a
successful or failed batch item without a second, implicit message context.

**Decision.**
- Compose a homogeneous generated batch handler with the consumer's existing size,
  wait, and concurrency declaration. Each batch attempt uses one fresh async DI scope.
- Retry only failed items unless unattributed buffered sends fail the whole attempt.
  Dispose the attempt's scope before final failure work,
  direct-send flush, or publication of settlement decisions.
- Buffer direct sends for the attempt. Flush only a wholly successful attempt;
  discard a wholly failed attempt's buffer. If partial success has buffered direct
  sends, discard the buffer and route every item through the normal failure policy.
  This bounds poison-message attempts through configured retry, redelivery and
  dead-letter handling. Do not guess which item produced a send or settle an item
  successfully after discarding its sends.
- Handlers that need partial success with outgoing messages use explicit durable
  per-message outbox transactions. At-least-once native redelivery still applies
  if later failure work or scope disposal fails.
- Native batch receive and core accumulation are explicit executors selected before
  receives start. Declaring a capability or batch size never supplies an executor.

**Consequences.** Successful deliveries never settle before scope disposal and all
required durable work. Partial failure with buffered sends can retry or dead-letter
otherwise successful items; use per-message outbox transactions for independent
outcomes with outgoing messages. A failed batch call can repeat already durable work on native
redelivery, as expected for at-least-once delivery. The core does not invent native
settlement or per-message ambient sender semantics.

## D29. Durable dispatch claims use expiring fencing tokens

**Context.** The original conditional-claim sketch in §6.4 does not describe recovery after a claimant crashes.
An instance name alone cannot distinguish an old attempt from a newer attempt by that same instance.
The durable store contract needs explicit recovery and ownership rules before database implementations ship.

**Decision.** Supersede D12's row-claim protocol detail while retaining its direct-dispatch and lane-lease design.
- Claim only pending, unparked rows that are due and have no live claim, using an atomic conditional update.
- Give every dispatch attempt a fresh nonempty fencing token and a future expiry. Never reuse tokens.
- Renewal, completion and failed-attempt updates require the matching token and a claim live at the supplied UTC instant.
  Renewal never shortens an expiry. Failure increments attempts, records the error and releases the claim atomically.
- Count the same eligible rows for health, across all lanes. Future schedules, live claims and parked/dead rows are excluded.
- Keep transactional enqueue in the native integration and `IOutboxEnlistment`; expose bounded reads and durable
  dispatch mutations through `IOutboxStore`. The optional health package adapts explicitly named stores.

**Consequences.** Expired claims are recoverable, and stale attempts cannot mutate a newer claim. Database fencing
cannot undo a broker send; crash recovery and sends that outlive their claims can still duplicate delivery.
This preserves the at-least-once contract. Ordered routes additionally require lane ownership; row claims alone
do not guarantee ordering. Schema scripts must persist claim tokens and expiry alongside pending/parked state.

## D30. Share PostgreSQL schema SQL with application-owned EF migrations

**Context.** Schema tooling must be usable before transaction enlistment and dispatch ship. Relayer does not
own an application's DbContext or migration history. Separate EF entity definitions and SQL creation scripts
would introduce two sources of truth for the same durable schema.

**Decision.** The PostgreSQL package embeds one idempotent initial creation script. The CLI prints it and the
EF Core package references the PostgreSQL package to expose it as a SQL migration operation in an application-owned
Npgsql migration. These tables stay outside the application model snapshot. No interception or automatic runtime
migration is registered. Schema evolution uses explicit SQL migration operations; initial creation never silently
upgrades existing definitions. Rollback is application-owned because dropping an outbox destroys durable work.
Lane columns use PostgreSQL integer, matching the durable contract's nonnegative Int32 lane range.

**Consequences.** The CLI, direct deployment and EF migrations create the same columns, constraints and indexes.
The EF integration currently carries the PostgreSQL dependency; future database providers need their own explicit
migration operations. This establishes schema deployment without claiming that database stores, durable sending,
dispatch workers or inbox behavior are implemented.

## D31. EF Core buffers sends for an explicitly owned context and flushes at commit

**Context.** An implicit SaveChanges transaction does not exist when a handler sends its messages.
Several DbContexts can coexist in one DI scope. EF transaction interceptors observe EF's transaction
wrapper; they cannot observe a direct call to a provider's underlying DbTransaction.Commit.
Header prefixes and raw envelope mode also make native headers unsuitable as the only enqueue metadata.

**Decision.** Scoped EF interceptors share an enlistment coordinator. A context selection owns sends
before an implicit save; one active explicit EF transaction is selected automatically, and ambiguous
transactions require explicit selection. The send seam passes an OutboxRecord containing original identity,
stable wire type and creation time. Enlistment copies borrowed storage, then flushes one batched insert
before the EF commit. An implicit save with buffered rows owns a transaction even with no business commands.
Contexts joining the same native transaction share one insert and identical schema/prefix/lane settings.
Buffered rows belong to the native transaction even after a joined context or borrowed EF wrapper is
disposed. Detached participants are excluded from automatic send selection and cannot start another
unit until their pending transaction ends. Retrying EF execution strategies require an explicit
transaction around the complete unit, with a fresh scoped context on each attempt; implicit saves
with buffered rows reject retrying strategies with an actionable error before opening a transaction.
PostgreSQL is implemented first. Its versioned header codec retains all name/value pairs; deterministic
FNV-1a lane hashing uses UTF-8 ordering keys. Tenant metadata is preserved when explicitly supplied.

**Consequences.** No SaveChanges override, entity mapping, header scanning, or process-wide mutable
state is required. Native transaction rollback/disposal abandons buffered sends. Failed explicit units and savepoint rollback
invalidate commit until the whole transaction is rolled back; callers retry complete units with fresh contexts.
Commit goes through IDbContextTransaction, including UseTransaction wrappers. Direct provider commits,
ambient TransactionScope, pooled contexts and concurrent use of a scope are outside this integration.
Schema deployment, dispatch, inbox, leases and health polling remain separate concerns.

## D32. PostgreSQL outbox transactions own commit and expose a compatible command connection

**Context.** A DbTransaction subclass can flush buffered messages before native commit, but NpgsqlCommand's
transaction setter accepts only NpgsqlTransaction. Passing a wrapper directly to Dapper on the original
connection therefore fails. Savepoint rollback also cannot safely discard business changes without rewinding
the corresponding message buffer.

**Decision.** The scoped PostgreSQL enlistment begins a native transaction on a caller-owned open connection,
or opens and owns one connection from an application-owned data source. Its DbTransaction wrapper exposes a
borrowed DbConnection view whose commands unwrap that exact transaction for Npgsql. DbCommand.Connection
returns the underlying NpgsqlConnection so Dapper retains PostgreSQL-specific array parameter handling.
Business commands and the
one outbox batch share the native transaction. Writers reuse the EF integration's validation, owned storage,
lossless header codec, lane hashing and fixed-parameter batch SQL. Options are init-only and explicitly bound
through the Options pattern. Savepoints, nested transactions and concurrent scope use are unsupported.

**Consequences.** Dapper callers use transaction.Connection, never the original NpgsqlConnection with the
wrapper transaction. Native configuration and isolation levels pass through unchanged. Native callbacks cannot
be persisted. Commit failure or cancellation ends the unit and releases its buffer; disposal rolls back any
uncommitted native work. A lost commit acknowledgement remains an unknown outcome requiring application
idempotency. Caller-owned connections remain open, while opened data-source connections are disposed without
disposing the data source. Database outbox durability does not imply native broker transaction enlistment or
implement dispatch. Direct native lifecycle calls and SQL transaction-control commands bypass the wrapper
and are outside this contract.

## D33. Commit-triggered dispatch transfers owned buffers to a named worker

**Context.** Direct dispatch must start without a polling interval and must outlive the committing
scope. Waiting for a broker in a commit callback would add network latency to business transactions.
Reusing scoped buffers in the background would corrupt messages when the next unit of work starts.

**Decision.** PostgreSQL direct dispatch is an explicitly named hosted service. EF context options
select the dispatcher and validate matching schema/prefix settings. Its data source is application-owned
and must target the same database as the DbContext. Only a successful EF commit transfers owned, reusable
message buffers into the worker's bounded channel. The scope receives cleared replacement buffers.
No per-message task or scoped DbContext travels with the queued commit.

The worker filters ordered and future-due rows and groups by transport/destination within the endpoint's
native batch count. One conditional UPDATE claims the group's eligible rows with fresh tokens and a shared
expiry. The worker sends only acquired rows, skipping the group if its deadline passed before the database
response. Native sends retain the stored envelope and native partial-delivery semantics. Successful groups
send immediately once the stored delivery time is due, without requiring native scheduling support, and
use one batch DELETE with a matching live token per row. Native send failures release live claims and
increment attempts. Completion errors after a successful send leave claims until expiry without consuming
send attempts, and incomplete deletions log a warning about duplicate delivery. There is no in-call heartbeat.

**Consequences.** Commit returns independently of broker latency. Normal competing instances cannot
claim the same row, while expired in-flight sends and partial native failures retain D29's at-least-once
boundary. Queue saturation or a stopped worker logs an error and leaves durable rows pending instead of
failing a business transaction that already committed. Shutdown drains queued commits until its deadline.
Recovery polling, ordered lane dispatch and poison-row processing remain separate work; direct dispatch
alone does not recover rows left by overflow, failure or process death. Buffer handoff, sorting and
projection have dedicated allocation benchmarks; native client and database allocations remain excluded.

## D34. Ordered PostgreSQL dispatch drains a locked lane before transfer

**Context.** Expiring leases alone let a successor overtake a slow connected sender. UUIDv7 ordering also
cannot imply transaction commit order, and a database token cannot fence a request already sent to a broker.

**Decision.** Refine D12's ordered dispatch protocol. Members heartbeat in the existing lease table using the
database clock. Live members, sorted by their unique IDs, divide lanes by rank. A member stops renewing
lanes outside its fair share. Successors wait for expiry; shutdown releases leases after native work drains.
Each bounded batch holds its lane lease row lock in a separate database transaction through sequential
native sends, row claims and fenced outcomes. Broker latency never enters the business transaction.
A cancellation deadline of half the lease duration covers the entire batch. Admission stops after one quarter
of the lease duration to reserve time for the final send and commit; healthy slow batches commit their prefix.
A single native operation must still fit the hard deadline. A connected transaction is never
forcibly revoked. Reads use the pending lane/ID index and never skip a scheduled or live-claimed head.
An indexed anti-join excludes later rows of parked keys while allowing other keys to continue.
After committed progress, the worker immediately starts another fair scan; only scans with no progress wait
for the polling interval. A busy lane cannot consume another lane's turn by draining without a scan boundary.
Each scan probes assigned lanes' first non-parked heads in one read-only query. Empty, scheduled and
live-claimed lanes incur no lease writes. This probe is only a hint: the worker re-reads and claims under
the lane transaction lock, so a stale probe cannot authorize a send or skip a head. Membership refreshes
when one third of the lease duration has elapsed on the monotonic clock, including between slow lane
batches, rather than once for every lane. The database clock still decides expiry and ownership.
Mutation commands and their typed parameters belong to the single dispatch loop and are reused across
rows and batches. Per-row claim and completion statements deliberately remain: each send checks the
head's eligibility, and each outcome checks its live token before advancing. Removing these fences or
batching outcomes would require a separate protocol change; command reuse removes their construction
allocations without weakening that boundary. Native execution, materialization and I/O costs remain
outside the managed command-binding benchmark.

**Consequences.** Database failure or uncertain completion rolls back the batch, so accepted sends can replay.
Ordering describes ascending visible UUIDs, not concurrent writers' commit order. When a database connection
fails while a native request is in flight, that request can arrive late: neither cancellation nor the lease can
retract it. Native ordering semantics and the at-least-once contract remain unchanged. Strict fencing of native
requests requires broker support and is not emulated. Idempotent consumers remain necessary.
Poison-row handling, retry limits and key-scoped parking remain tracked by
[#34](https://github.com/thomhurst/Relayer/issues/34).

**Alternatives rejected.** Lease expiry without a transaction guard permits concurrent connected senders.
Stealing live ownership for faster rebalancing violates safe drain. Claim-only scans can skip a live or
scheduled head. Holding a business transaction across broker I/O couples application durability to the broker.
