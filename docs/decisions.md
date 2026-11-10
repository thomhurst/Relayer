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
| [D20](#d20-sagas-are-out-of-scope-for-v1) | Sagas are out of scope for v1 | Accepted; post-v1 implementation specified by [D43](#d43-saga-transitions-share-the-consumer-transaction-and-retain-completed-state) |
| [D21](#d21-repository-conventions) | Repository conventions | Accepted |
| [D22](#d22-the-kafka-transport-wraps-dekaf) | The Kafka transport wraps Dekaf | Accepted; native reuse rule generalized to every transport by [D42](#d42-every-transport-reuses-its-native-clients-features) |
| [D23](#d23-a-generic-transport-spi-owned-by-the-core) | A generic transport SPI owned by the core | Accepted; core-first log scheduling superseded by [D40](#d40-log-transports-reuse-native-processing-runtimes) |
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
| [D35](#d35-direct-dispatch-bounds-endpoint-capability-retention) | Direct dispatch bounds endpoint capability retention | Accepted |
| [D36](#d36-an-explicit-ef-core-consumer-boundary-owns-the-inbox-transaction) | An explicit EF Core consumer boundary owns the inbox transaction | Accepted |
| [D37](#d37-ordered-dispatch-bounds-validated-endpoint-retention) | Ordered dispatch bounds validated endpoint retention | Accepted |
| [D38](#d38-postgresql-maintenance-leases-recover-unordered-rows-and-bound-retention) | PostgreSQL maintenance leases recover unordered rows and bound retention | Accepted |
| [D39](#d39-named-transports-capture-startup-prepared-serializers) | Named transports capture startup-prepared serializers | Accepted |
| [D40](#d40-log-transports-reuse-native-processing-runtimes) | Log transports reuse native processing runtimes | Accepted |
| [D41](#d41-requests-correlate-ordinary-messages-on-explicitly-declared-endpoints) | Requests correlate ordinary messages on explicitly declared endpoints | Accepted |
| [D42](#d42-every-transport-reuses-its-native-clients-features) | Every transport reuses its native client's features | Accepted |
| [D43](#d43-saga-transitions-share-the-consumer-transaction-and-retain-completed-state) | Saga transitions share the consumer transaction and retain completed state | Accepted |
| [D44](#d44-messagepack-uses-native-generated-formatters-directly) | MessagePack uses native generated formatters directly | Accepted |
| [D45](#d45-tenant-store-selection-reuses-native-scoped-factories) | Tenant store selection reuses native scoped factories | Accepted |
| [D46](#d46-sql-server-primitives-use-native-sqlclient-with-an-explicit-aot-boundary) | SQL Server primitives use native SqlClient with an explicit AOT boundary | Accepted |
| [D47](#d47-sql-server-outbox-transactions-reuse-native-bulk-copy) | SQL Server outbox transactions reuse native bulk copy | Accepted |
| [D48](#d48-mysql-enqueue-owns-native-transactions-and-reuses-mysqlbatch) | MySQL enqueue owns native transactions and reuses MySqlBatch | Accepted |
| [D49](#d49-sql-server-commit-dispatch-reuses-native-rpc-batches) | SQL Server commit dispatch reuses native RPC batches | Accepted |
| [D50](#d50-ordered-lanes-follow-a-database-assigned-insert-sequence) | Ordered lanes follow a database-assigned insert sequence | Accepted |
| [D51](#d51-the-ef-core-consumer-boundary-takes-an-isolation-level-and-replays-conflicts) | The EF Core consumer boundary takes an isolation level and replays conflicts | Accepted |
| [D52](#d52-allocations-a-native-api-requires-count-as-native-client-cost) | Allocations a native API requires count as native-client cost | Accepted |
| [D53](#d53-singleton-only-consumers-open-delivery-scopes-on-demand) | Singleton-only consumers open delivery scopes on demand | Accepted |
| [D54](#d54-service-bus-processors-drain-durable-work-before-settlement) | Service Bus processors drain durable work before settlement | Accepted |
| [D55](#d55-mysql-direct-dispatch-reuses-native-batches-and-fenced-row-counts) | MySQL commit-triggered direct dispatch | Accepted |
| [D56](#d56-postgresql-cdc-wakes-existing-unordered-recovery) | PostgreSQL CDC wakes existing unordered recovery | Accepted |
| [D57](#d57-google-pubsub-uses-prepared-native-api-clients) | Google Pub/Sub uses prepared native API clients | Accepted |
| [D58](#d58-mysql-consumers-share-the-adonet-outbox-transaction) | MySQL consumers share the ADO.NET outbox transaction | Accepted |
| [D59](#d59-sql-server-consumers-reuse-the-adonet-outbox-transaction) | SQL Server consumers reuse the ADO.NET outbox transaction | Accepted |
| [D60](#d60-bound-catalog-dispatch-by-native-connection-lifetimes) | Bound catalog dispatch by native connection lifetimes | Accepted |
| [D61](#d61-aspire-reuses-native-resource-wiring-and-application-telemetry) | Aspire reuses native resource wiring and application telemetry | Accepted |
| [D62](#d62-sql-server-maintenance-reuses-direct-dispatch-and-native-transaction-locks) | SQL Server maintenance reuses direct dispatch and native transaction locks | Accepted |
| [D63](#d63-mysql-ordered-dispatch-uses-innodb-lease-rows-and-native-identity) | MySQL ordered dispatch uses InnoDB lease rows and native identity | Accepted |
| [D64](#d64-sql-server-ordered-dispatch-reuses-native-locks-and-sequence-ranges) | SQL Server ordered dispatch reuses native locks and sequence ranges | Accepted |
| [D65](#d65-mysql-maintenance-uses-native-session-admission-and-existing-dispatch) | MySQL maintenance uses native session admission and existing dispatch | Accepted |
| [D66](#d66-durable-local-work-reuses-outbox-rows-and-native-postgresql-transactions) | Durable local work reuses outbox rows and native PostgreSQL transactions | Accepted |

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
- Optional native integrations with unresolved upstream limitations expose explicit build diagnostics;
  see the Avro boundary in D39 and the SQL Server boundary in D46. Their unsupported paths are not in the core AOT smoke.

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

Implementation ownership is clarified by [D40](#d40-log-transports-reuse-native-processing-runtimes): the planned parallel path uses Dekaf's native runtime. The consumption requirements below remain in force.

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
- Service Bus applications may explicitly trust per-entity native `RequiresSession=true` declarations in Validate/None when topology is externally managed. Trust grants ordering capabilities without Manage access; Provision still verifies native settings. Send-only validation does not peek trusted subscriptions, whose existence and completeness remain the application's responsibility.

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

**Implementation note.** D43 specifies the post-v1 runtime and optional EF Core persistence package. It does not bring sagas into v1. Completed state is retained; durable process timeouts compose existing scheduled sends as described in D43.

## D21. Repository conventions

**Decision.**
- `Relayer.slnx` solution file.
- Central Package Management.
- Shared settings in `Directory.Build.props`.
- Nullable reference types on, and warnings treated as errors.
- Layout: `src/`, `tests/`, `benchmarks/`, `docs/`.
- CI on GitHub Actions.
- MinVer derives a shared package and assembly version from `v`-prefixed SemVer tags. Untagged builds
  use preview versions (minimum `0.1`). Full Git history is required for CI and release builds.
- Packages include the repository README and icon, commit-bound SourceLink from the .NET SDK, and
  portable PDB symbol packages. Release builds are deterministic with normalized source paths.
- Tag pushes publish through NuGet trusted publishing only for commits on `main` with successful
  `CI Passed`. See the [release workflow guide](../website/docs/contributing/releases.md) for setup.

## D22. The Kafka transport wraps Dekaf

**Context.** Confluent.Kafka wraps the native librdkafka library. That brings native binaries, interop overhead, logs that bypass `ILogger` (confluent #2386), and an open Native AOT gap (confluent #2146). Those conflict with D4 and D18.

**Decision.**
- `Relayer.Kafka` wraps [Dekaf](https://github.com/thomhurst/Dekaf), a pure C# Kafka client with no native dependencies, Native AOT coverage, and zero-allocation hot paths.
- Dekaf's `ProducerBuilder` and `ConsumerBuilder` are exposed to users unchanged, and its `ConsumeResult` is exposed through `IKafkaConsumeFeature`.
- Where Dekaf already provides a feature (retry and dead-letter topics, transactions, OpenTelemetry instrumentation, partitioned processing), the transport calls it rather than re-implementing it. A Dekaf feature that cannot be used, for example because it would bypass Relayer's uniform failure pipeline or telemetry, is recorded here with the reason, and the missing piece is filed as a Dekaf issue. Relayer does not build a parallel implementation without that record.

**Consequences.**
- No native binaries, and AOT works end to end on Kafka.
- Relayer.Kafka's release cadence follows Dekaf's.
- Consumer handler retries and durable dead-letter copies use the core failure pipeline. Dekaf's hosted retry/dead-letter helpers own dispatch and settlement, so they would bypass the portable classifier and durable-copy contract. The serial consumer foundation instead reuses native fetch batches, next-offset checkpoints, producer acknowledgments and explicit commits. Parallel scheduling and drain-on-revoke are to build on Dekaf's `RunPartitionedAsync` rather than the core scheduler ([#239](https://github.com/thomhurst/Relayer/issues/239)).
- Until Kafka failure copies have a persistable representation for tombstones and null-valued headers, durable redelivery policies fail at startup even when a durable scheduler is registered. The Kafka consumer does not expose that scheduler to manual settlement either. In-process retries and acknowledged dead-letter copies remain available. This explicit limitation avoids accepting a policy that fails only after receiving native metadata.
- Users who need Confluent.Kafka specifically could be served later by a separate `Relayer.Kafka.Confluent` package. That is not planned.

**Alternatives rejected.**
- Confluent.Kafka: native dependency, AOT gap, and logging that bypasses `ILogger`.

## D23. A generic transport SPI owned by the core

The core-first log scheduling rule below is superseded by [D40](#d40-log-transports-reuse-native-processing-runtimes). The remaining SPI decisions stay in force.

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
- The implemented scheduler is single-owner: the transport pumps completion observation and native calls, including while fetching is paused. `IPartitionMessageBinding<TIncoming>` compares native keys and releases delivery leases. Assignment tokens fence completion publication; native adapters still own fencing asynchronous commit queues. Bounded ownership drain never releases buffers beneath late handlers. See the [transport-author contract](../website/docs/transports/partition-scheduler.md).
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
- Outbox pending and dispatch-lag instruments are store-wide, without endpoint dimensions: one transaction can mix transports. Pending records instance-observed durable insert/delete deltas rather than a database snapshot; recovery on another instance can produce negative deltas. Dispatch lag uses the original row timestamp for each native attempt, including retries.
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
transaction around the complete unit, with clean tracked state on each attempt; implicit saves
with buffered rows reject retrying strategies with an actionable error before opening a transaction.
PostgreSQL is implemented first. Its versioned header codec retains all name/value pairs; deterministic
FNV-1a lane hashing uses UTF-8 ordering keys. Tenant metadata is preserved when explicitly supplied; an application-authorized scoped binding supplies and validates it for tenant stores (D45).

**Consequences.** No SaveChanges override, entity mapping, header scanning, or process-wide mutable
state is required. Native transaction rollback/disposal abandons buffered sends. Each context's last
successful SaveChanges (including an empty save) establishes its retained-send boundary, initially zero.
A confirmed native automatic savepoint rollback during that context's SaveChanges discards sends since
that boundary, including sends buffered before SaveChanges began, and permits the explicit unit to
continue. Other participating contexts retain their own buffers. Successful native savepoint release
drops the active checkpoint; failed/canceled-save callbacks preserve recovery only after confirmed
rollback. Relayer does not issue savepoint SQL or emulate database recovery. Manual savepoint rollback,
failed saves without confirmed recovery (including AutoSavepointsEnabled=false), and failed commits
invalidate commit until the whole transaction ends. Callers retry complete units with either a fresh context
or the same context after full rollback (or server abort), disposal of the EF transaction wrapper and any
outbox selections, and clearing/reloading tracked entities. Relayer clears buffered sends and failure/transaction
ownership state when the transaction ends; a new EF transaction selects its context automatically.
Lexical outbox selections still require disposal by the caller.
Commit goes through IDbContextTransaction, including UseTransaction wrappers. Direct provider commits,
ambient TransactionScope, pooled contexts and concurrent use of a scope are outside this integration.
Schema deployment, dispatch, inbox, leases and health polling remain separate concerns.

**Pooling assessment (#356).** EF Core 10.0.14's
[`AddPoolingOptions`](https://github.com/dotnet/efcore/blob/v10.0.14/src/EFCore/Extensions/EntityFrameworkServiceCollectionExtensions.cs#L342-L365)
registers singleton options and invokes their callback with the root provider. Relayer's interceptors
capture the scoped `EntityFrameworkCoreOutbox` also used by `ISender`, including cross-context selection,
shared-transaction participants, buffered sends and scoped dispatch configuration. With scope validation
this resolution fails; without it the root captures a coordinator that subsequent delivery scopes do not use.
Keying buffers by context or transaction does not reconnect that coordinator to the current sender scope.
EF's [`GetResettableServices`](https://github.com/dotnet/efcore/blob/v10.0.14/src/EFCore/DbContext.cs#L917-L973)
resets services in its internal provider, not arbitrary options interceptors; even a reset hook would only
clear state, not supply the next lease's application provider. Pool lease setup likewise does not pass
that provider to interceptors. Supporting pooling would require an explicit per-lease scope-binding API
and corresponding ownership/reset contract, rather than merely moving interceptor fields. Keep both
outbox and consumer boundaries non-pooled so their ownership contract stays uniform. Validate final EF
options as well as the options callback, since EF sets `MaxPoolSize` after invoking that callback.
A singleton root-provider marker also rejects root-bound interceptors inside the callback, including
when DI scope validation is disabled. This guard is necessary because EF validates options extensions
only when building an internal service provider, not when reusing a cached one
([EF 10.0.14 source](https://github.com/dotnet/efcore/blob/v10.0.14/src/EFCore/Internal/ServiceProviderCache.cs)).
The marker retains only its owning root provider; it never captures an application scope.
This restriction concerns DbContext pooling only; native Npgsql connection pooling remains supported.


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
On attempt exhaustion, a single fenced statement copies failure metadata into the dead table and retains
the ordered row as the parked barrier at its original sequence. Unordered exhausted rows are removed.
Management redrive resets the retained row in place; explicit purge deletes the barrier and dead copy
in one transaction. Retention excludes dead rows with active barriers so redrive never loses its payload.
These operations reuse the existing lane runtime; management never sends messages itself.
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
Ordering follows the database-assigned insert sequence among visible rows (D50), not concurrent writers' commit order. When a database connection
fails while a native request is in flight, that request can arrive late: neither cancellation nor the lease can
retract it. Native ordering semantics and the at-least-once contract remain unchanged. Strict fencing of native
requests requires broker support and is not emulated. Idempotent consumers remain necessary.
Poison-row handling, retry limits and key-scoped parking remain tracked by
[#34](https://github.com/thomhurst/Relayer/issues/34).

**Alternatives rejected.** Lease expiry without a transaction guard permits concurrent connected senders.
Stealing live ownership for faster rebalancing violates safe drain. Claim-only scans can skip a live or
scheduled head. Holding a business transaction across broker I/O couples application durability to the broker.

## D35. Direct dispatch bounds endpoint capability retention

**Context.** Dynamic tenant destinations make a worker-lifetime capability dictionary grow with all
historical destinations, even after their durable messages complete. Native limits vary by endpoint,
so a transport-wide capability entry cannot replace endpoint-specific resolution.

**Decision.** Each unordered PostgreSQL direct worker caches at most 256 successful
`(transport name, destination)` lookups in a preallocated dictionary and insertion-order ring.
On a successful miss at capacity, evict the oldest inserted entry. Resolve and validate native
capabilities before eviction, so failures do not displace valid entries. The next use of an evicted
endpoint resolves its native limits again before claiming rows. Hits do not update eviction order.
The cache belongs exclusively to the existing single-reader worker; it needs no locks or reflection.

**Consequences.** Retained destination references and capability entries are bounded by count.
Hot hits keep one dictionary lookup with no recency writes or managed allocations. A miss at capacity
adds one dictionary removal and ring replacement; dictionary storage is reused. Working sets exceeding
256 entries can cause repeated native capability resolution, including any allocations the native
transport performs. This is the bounded-retention policy requested by #138, rather than a change to
native send, batch limits or durable claims. Dedicated benchmarks compare hot sets and eviction churn
against the previous unbounded lookup; their raw results remain outside the repository.

**Alternatives rejected.** Transport-only keys lose endpoint-specific limits. LRU adds maintenance to
every hit. Clearing the entire cache discards unrelated hot entries at once. A public cache setting
adds configuration surface before evidence shows applications need one.

## D36. An explicit EF Core consumer boundary owns the inbox transaction

**Context.** The inbox key must be durable in the same transaction as business data and outgoing rows.
Inspecting handler dependencies would introduce hidden conventions and cannot identify one owner when
several DbContexts share a scope. A preflight existence query alone races with concurrent deliveries.

**Decision.** `ConsumePipelineBuilder.EntityFrameworkCore<TContext>(consumer)` explicitly selects the
scoped context and owns a transaction around the handler and all inner middleware. Install it first.
The inbox is automatic when that context has outbox interceptors; without them it is opt-in per
consumer. Schema and prefix follow the outbox context's scoped configuration. PostgreSQL's unique
`(consumer, message_id)` key and `INSERT ... ON CONFLICT DO NOTHING` serialize concurrent duplicates.
An insert that loses to a committed row skips the handler, records a duplicate metric and permits
normal native settlement. A rollback lets the waiting delivery process the original UUIDv7 identity.

**Consequences.** Handler saves do not commit the unit. Exceptions, cancellation and non-complete
settlement requests roll back business data, inbox and outgoing rows before ordinary failure handling.
Successful handling requests completion if no outcome is pending, then commits before scope disposal
and native settlement. The one-request rule rejects any later settlement request, so disposal cannot
retry or dead-letter work after commit. Native acknowledgement loss
therefore redelivers safely. Retrying EF strategies are supported through the native
`IExecutionStrategy.ExecuteAsync` operation and verification callbacks. Every strategy attempt runs
the complete inner pipeline in a fresh scope, context and transaction; only native transient
classification triggers these retries. Handler failures still flow to Relayer's consumer policy.
Verification queries the exact attempted inbox key and schema using a fresh connection, resolving
an unknown commit without replaying committed work. Without an inbox there is no commit verification
or duplicate protection. The original delivery scope keeps the strategy's context alive and is
restored before ordinary failure handling. Failed-attempt sends and settlement requests are cleared
after scope disposal. Composition selects the original middleware directly for non-retrying contexts that omit an explicit
isolation level (D51), so their per-message work and allocations are unchanged. Retrying contexts add native strategy Task
callbacks and fresh DI scopes, with bounded reusable operation state; no parallel retry policy is
implemented. Pooled contexts, ambient transactions and batch consumers remain rejected. Handlers must not end the owned
transaction or perform native settlement. Inbox retention is provided by PostgreSQL maintenance (D38).

## D37. Ordered dispatch bounds validated endpoint retention

**Context.** An ordered worker can outlive arbitrarily many tenant destinations. Retaining every
historical endpoint leaks destination references and native capability objects after rows complete.
Ordering support and other native limits vary by endpoint, so a transport-only key is unsafe.

**Decision.** Each ordered PostgreSQL worker retains at most 256 successful `(ITransport, destination)`
lookups in a preallocated dictionary and insertion-order ring, shared across its sequential lane loop.
Use the FIFO policy selected for unordered direct dispatch in [#160](https://github.com/thomhurst/Relayer/pull/160):
hits do not update eviction order; a successful miss at capacity replaces the oldest inserted entry.
Resolve native capabilities and validate `OrderedByKey` before eviction. Exceptions and unsupported
endpoints are not retained, so a later attempt can recover without replacing valid entries.

**Consequences.** Retained endpoint count is bounded. Hot hits require one dictionary lookup and no
recency writes or Relayer allocations. Misses at capacity add removal, insertion and ring replacement;
storage is reused. Working sets over 256 endpoints can repeatedly resolve native capabilities, including
any native client allocations. An evicted endpoint is validated again before its next native send,
inside the existing durable claim/failure protocol. Retained capabilities remain snapshots, so topology
changes still require a worker restart. No lane, lease, send or settlement semantics change.
Benchmarks compare hot sets and churn against the previous unbounded lookup; raw evidence stays outside
the repository. This applies the bounded-retention requirement of #159 without adding public options.

**Alternatives rejected.** Transport-only keys lose endpoint-specific ordering support. LRU writes on
every hit. Clearing the whole cache discards unrelated entries together. Sharing the unordered cache's
positive-batch-size validator would change the ordered sender's contract; its sends remain sequential.

## D38. PostgreSQL maintenance leases recover unordered rows and bound retention

**Context.** Commit-triggered direct dispatch can leave durable rows after queue overflow or process death.
Ordered lanes already recover independently. Retention must not delete undelivered messages or run concurrent
cleanup jobs on every replica.

**Decision.** Register maintenance explicitly with immutable named settings. Use separate `sweeper` and `cleanup`
rows in the existing lease table. Database-clock expiries govern idle leadership; hold the lease row's transaction
lock throughout each connected pass, then renew from completion. Release owned leases on graceful shutdown.
Read bounded unordered, due, unclaimed pending rows older than a configured `created_at` threshold. Traverse by
ID up to a fixed high-water mark captured at traversal start, then wrap after an empty pass within that boundary.
This revisits failed or newly eligible older rows even under continuous newer traffic, while failed rows do not
starve later work. Reuse direct dispatch's native grouping,
row fencing and batch deletion. Recovery claim/completion mutations share the leadership transaction, using
one pooled connection per pass. A crash rolls these mutations back and permits replay; direct dispatch keeps
its independent auto-committed claims. No pass holds one pool connection while waiting to borrow another.
Ordered rows remain exclusively with their lane dispatcher.

Delete sent outbox rows on completion, never pending or parked rows by age. Each cleanup pass deletes bounded
batches of dead rows by `failed_at` and inbox records by `processed_at`; either retention can be disabled.
Inbox retention defines a finite deduplication window and must cover the application's replay horizon.

Opt-in PostgreSQL sent history retains only ID, destination and completion time in a separate table.
Reuse PostgreSQL's data-modifying CTE and `DELETE RETURNING` pattern from poison completion: capture only
rows removed by the matching live claim, in the same statement and transaction. Direct, ordered, recovery,
CDC and tenant dispatch share this policy. Disabled workers retain their original completion SQL, chosen at
construction, with no extra per-message work. Enabled completion adds one metadata insert/upsert and index
writes per deleted row, without another round trip or body copy. This is database diagnostic evidence of
completion after native acceptance, not a broker-delivery or consumer-processing guarantee. The existing
leased cleanup deletes bounded oldest-first history batches using a positive finite retention duration.
Lookup reports sent metadata through the existing read-only snapshot only when explicitly enabled
(`RetainSentMessages` or the CLI `--sent-history` switch). Default lookups do not require the history table;
enabled lookups validate its presence with an actionable schema-deployment error. See
[sent-history configuration and limits](../website/docs/reference/postgres-maintenance.md#sent-message-history).

**Consequences.** Recovery remains at-least-once; crashes between native acceptance and completion replay the
stored identity. A lease-row connection failure cannot retract an in-flight native request. Large backlogs drain
across multiple bounded passes, and native calls can delay cleanup. Poison handling and retry backoff remain
separate work. D36 defines the EF Core inbox boundary. No schema evolution or expiry of undelivered messages
is required.

## D39. Named transports capture startup-prepared serializers

**Context.** Named Kafka clusters can use independent registries, credentials and schemas. An application-wide
serializer cannot isolate those contracts. The synchronous buffer-based body interface in D8 cannot await
registry discovery, and Dekaf's synchronous deserializer fetches unknown or evicted schemas synchronously.

**Decision.** Refine D8 with explicit `transport.UseSerializer(factory)` selection. Send routes and consumers
capture the same keyed singleton at startup; transports without an override use the application serializer.
Do not resolve unused default JSON metadata when every transport has an override. Await optional
`IMessageSerializerInitializer` preparation before hosted receivers start, including concurrent startup.

The optional `Relayer.Kafka.SchemaRegistry` package prepares explicit JSON writer contracts with Dekaf's
AOT-safe serializer and native configuration. Accept only the prepared writer schema ID for each message
type. Read that known five-byte prefix and source-generated JSON directly, without synchronous registry
fetches or per-message discovery. Other schema versions require explicit wire types and upcasters.

**Consequences.** Serializer selection adds no lookup to existing send or consume operations. Registry HTTP
errors fail startup. Registry IDs remain local to each serializer and never replace stable CloudEvents
message identity. Dynamic discovery, registry rules/validation, latest-version selection, header/GUID
identities and Protobuf schema registry adapters remain planned. `Relayer.Serialization.Avro` follows the
same fixed-contract startup boundary, using Dekaf's generated POCO writer and cache-only reader after
asynchronous warmup. Unknown IDs never trigger discovery. Segmented bodies use a pooled contiguous copy
required by the native reader; no parallel Avro codec or reflection fallback is introduced. The original Dekaf 1.25.0 publish probe found Apache.Avro/Newtonsoft.Json dynamic paths in native startup schema preparation ([Dekaf #4060](https://github.com/thomhurst/Dekaf/issues/4060)). Dekaf 1.26.0 removes those paths under Native AOT, verified by a warning-clean native writer/reader preparation probe. A separate trimmed JIT publish still fails with IL2026 because the native rule paths remain reachable when dynamic code is supported. Retain `RequiresUnreferencedCode` on registration until that path is supported; this also prevents warning-clean Relayer Native AOT use. Remove only the obsolete `RequiresDynamicCode` annotation. No diagnostics are suppressed. Registry rules remain unsupported by this adapter. No broker send, receive or settlement semantics change.

## D40. Log transports reuse native processing runtimes

Supersedes D23's core-first log scheduling rule and clarifies implementation ownership for D13, following #239. The rest of D23's generic SPI remains in force.

**Context.** D23 put key ordering, completion watermarks, backpressure and ownership drain in the core to avoid rebuilding them for each log transport. Kafka is currently the only production log transport, and Dekaf already provided these mechanisms at the decision-time version 1.25.0 in [`RunPartitionedAsync`](https://github.com/thomhurst/Dekaf/blob/v1.25.0/src/Dekaf/Consumer/PartitionedProcessing.cs) and its [key-ordered dispatcher](https://github.com/thomhurst/Dekaf/blob/v1.25.0/src/Dekaf/Consumer/KeyOrderedPartitionDispatcher.cs). Driving the core scheduler from Kafka, as proposed in #221, duplicates the native runtime and conflicts with D1 and D22.

**Decision.**
- Inspect the pinned native client's source before selecting a log processing model. Reuse its scheduling, completion tracking, backpressure and ownership drain. File missing native capabilities upstream and wrap what exists. A parallel implementation needs a recorded decision naming the native feature and why it cannot be used.
- Kafka's planned parallel adapter wraps Dekaf's `RunPartitionedAsync` and maps configuration onto `PartitionedProcessingOptions`. Use native key ordering, per-partition handler/buffer limits, pause/resume, stop policy/timeout and commit policy/interval. Relayer retains its receive/failure pipeline, durable disposition before native completion, commit tracing and health integration. It does not use the core scheduler for Kafka.
- `PartitionScheduler<TIncoming, TBinding, TPartition, TPosition>` remains a fallback only for log transports whose native client has no equivalent. Its existing single-owner, bounded-storage and ownership-fencing contracts remain unchanged; see the [transport-author contract](../website/docs/transports/partition-scheduler.md).
- No production transport currently uses that fallback. Retain its public SPI and tests while the next log transport is designed, then inspect that client's runtime first. If no planned transport needs it, remove the public scheduler instead of maintaining unused public code. This decision makes no public API removal.

**Consequences.** The current serial Kafka adapter remains the shipped path. #22 (which consolidates #182) owns the native parallel adapter rework; this policy change does not complete it or permit #221's duplicated runtime to merge. Consumer-wide limits, oldest outstanding record health and runtime-aware replay must preserve their contracts; the native gaps and implementation status are recorded in [design §7](design.md#7-kafka-done-properly) and the [Kafka guide](../website/docs/transports/kafka.md). Missing native support is not permission to silently weaken those contracts or rebuild the runtime. D22's native reuse rule still applies; #240 separately tightens its exception wording.

**Alternatives rejected.**
- Require every log transport to use the core scheduler: duplicates clients that already own processing and adds a second ownership protocol.
- Remove the scheduler immediately: preempts the next log transport's native-client assessment; #239 explicitly retains that decision for its design.
- Keep the Kafka duplication until native gaps close: makes an unrecorded exception to D1/D22 and creates a second runtime to retire later.

## D41. Requests correlate ordinary messages on explicitly declared endpoints

**Context.** Request/response (§14, #47) requires application-level correlation and a bounded deadline. Dekaf 1.25.0 exposes producer/consumer headers, not an application RPC runtime. Azure.Messaging.ServiceBus 7.21.0 exposes native `ReplyTo`/`CorrelationId` properties and application properties; these do not own pending application calls. RabbitMQ.Client 7.2.2 provides native reply properties and Direct Reply-To, whose endpoint and acknowledgement semantics differ from ordinary declared consumers. AWS's temporary queue request/response helper is a separate Java client, not AWSSDK.SQS 4.0.100.22. Existing Relayer routes and binary CloudEvents bindings already preserve extension headers; no new broker send, receive, scheduling or settlement runtime is needed.

**Decision.** Explicit `Request<TRequest,TResponse>` registration declares an ordinary request send route and a dedicated single-message reply consumer. Each call supplies a finite positive timeout. Carry `replyto` and `correlationid` extensions in the route's existing binary CloudEvents binding. Use a provider-owned bounded slot table with a fresh identity per reservation; parse correlation from borrowed text/UTF-8 without allocating a lookup key. Request orchestration owns completion and deadline resources, separate from the pooled per-message send/consume pipeline. Release reservations on every terminal outcome; ignore late, duplicate and unmatched correlations without changing normal settlement.

Requests send immediately rather than waiting for a handler send buffer or outbox commit. Reject raw-envelope and outbox-required request routes. Responses use the ordinary scoped sender and its existing durability boundary. A response's configured route must match the incoming reply address. This keeps one route per message type and validates the native destination at startup, without accepting arbitrary destinations from received headers. Native reply properties, temporary queues, Direct Reply-To and multi-destination response routing remain reachable through native clients; this feature does not emulate them or claim their delivery guarantees.

**Consequences.** Request endpoints must be exclusive to the requesting process, with a matching declared response route. State does not survive restart and competing requesters must not share a reply queue. Existing native consumer limits and implementation status still apply. A timeout cancels the native send cooperatively; it cannot retract accepted work. Ordinary direct sends and consumers retain their performance contracts; the additional send-path work is two empty-metadata checks, with no steady-state allocation. Benchmark coverage compares direct send with opt-in request orchestration; its completion, correlation string and deadline allocations are explicit costs of starting a request, not added to ordinary traffic.

## D42. Every transport reuses its native client's features

Generalizes D22's native reuse rule, which was written for Dekaf, to every transport. The rest of D22 remains in force, and D40 remains the specific application of this rule to log processing runtimes.

**Context.** D1 says each transport wraps a native client and keeps native semantics unchanged, but the rule to search for and call a feature the client already ships, and to record why when it cannot be used, was only written down for Dekaf in D22. Azure.Messaging.ServiceBus, AWSSDK.SQS/AWSSDK.SimpleNotificationService and RabbitMQ.Client also ship capabilities that Relayer could otherwise rebuild, such as scheduled delivery, concurrency and prefetch, session and lock renewal, retry policies, dead-lettering and OpenTelemetry instrumentation. Without a general rule, those transports could grow parallel implementations that D22 would forbid for Kafka.

**Decision.**
- Before implementing a capability in any transport or in the core on a transport's behalf, inspect the native client at the version pinned in `Directory.Packages.props`: its source where available, not only its documentation. Also check Relayer's existing code.
- Where the native client already provides the capability, the transport calls it rather than re-implementing it.
- A native feature that cannot be used, for example because it would bypass Relayer's uniform failure pipeline, durable settlement or telemetry, is recorded in this file with the reason, and the missing piece is filed as an issue on that client's repository. Relayer wraps what exists.
- Relayer does not build a parallel implementation without that record.

**Consequences.**
- Every transport follows the same reuse rule, so reviewers apply one standard across Kafka, Azure Service Bus, SQS/SNS and RabbitMQ.
- Transport behaviour tracks native client improvements, and gaps are fixed upstream where possible instead of being hidden in Relayer.
- Existing transport code that already duplicates a native feature without a record is a defect to fix or record, not a precedent.

**Alternatives rejected.**
- Keep the rule Kafka-specific: leaves the other transports free to duplicate their native clients, which contradicts D1.
- Repeat the rule in each transport's decision: multiplies wording that drifts; one general rule with transport-specific records is easier to keep consistent.

## D43. Saga transitions share the consumer transaction and retain completed state

Extends D20's post-v1 scope; it does not change the v1 boundary.

**Context.** #248 requires explicit process state, atomic outgoing work and optimistic concurrency. EF Core 10.0.14 already checks concurrency tokens, and Npgsql.EntityFrameworkCore.PostgreSQL 10.0.3 maps a uint row version to PostgreSQL xmin. Native primary-key uniqueness detects concurrent creation. D31/D36 already supply scoped sender enlistment and the consumer's atomic inbox/outbox transaction. The failure pipeline already rolls back a failed attempt and retries in a fresh scope. None of these need a second implementation.

**Decision.** Keep typed execution in `Relayer.Sagas` and persistence in optional `Relayer.Sagas.EntityFrameworkCore`. State queries and native EF model configuration remain application code. Startup validates an explicit non-generated correlation key, native xmin row version and the selected outbox context. Save state, inbox identity and outgoing messages in the existing consumer transaction, and leave settlement and complete-attempt retries to the existing pipelines. A send-only transition still issues a native version-checked update. Keep completed rows as tombstones so later starting messages cannot recreate a completed process. Deleting retained state deliberately ends that protection. Diagram metadata remains independent of execution. Durable process timeouts (#249) use ordinary typed messages with application-owned process and wait identities, sent through ISender with SendOptions.DeliverAt inside this same boundary. Existing PostgreSQL due-row dispatch owns delivery; the consumer inbox owns duplicate message IDs. Replacing or clearing the wait identity logically invalidates old timeouts; completed tombstones skip transitions, and missing state keeps the existing explicit failure. No new scheduler, cancellation store, timeout registration API or retry loop is introduced. See the [timeout guide](../website/docs/guides/saga-timeouts.md).

**Execution state.** Reuse the continuation and ExecutionContext restoration pattern from `ConsumePipeline.CoreOperation` and the provider-owned, peak-retaining cross-thread pool pattern from `SendRoute`. A plain async ValueTask requires a state-machine object on suspension; the runtime's pooled async builders use [thread-local and per-core caches](https://github.com/dotnet/runtime/blob/v10.0.14/src/libraries/System.Private.CoreLib/src/System/Runtime/CompilerServices/PoolingAsyncValueTaskMethodBuilderT.cs#L256-L283) and do not guarantee reuse after worker changes. The saga executor therefore retains its typed load/transition/save operation until the caller consumes GetResult, then clears completion references and returns it to its owning pool. Generated handler registration adds the typed singleton explicitly. There are no process-wide mutable pools, background threads, timers, per-message callback allocations or additional retry loops. The EF save hook returns the native task directly.

Calling CoreOperation recursively would create another consumer scope and repeat the consume/settlement lifecycle. SendRoute owns send buffers and transport dispatch rather than typed state loading. Neither can execute a saga transition directly. The small saga operation uses their established patterns within the existing consumer boundary; it does not replace a native locking, scheduling, settlement or transaction feature. Benchmarks compare native EF updates with saga updates, and use a reusable synchronous/cross-thread read fixture to attribute execution allocations independently of native EF allocations. Lifecycle tests cover every suspension stage, exceptions, cancellation, execution-context isolation and reservation until GetResult. Local benchmark timings remain diagnostic under D18.

**Consequences.** The provider-independent runtime and generated dispatch are Native AOT tested. EF persistence inherits the provider's trimming, compiled-model and query restrictions; this decision does not claim to make EF Core AOT compatible. Ordinary non-saga send/receive paths retain their existing contracts. The additional execution complexity serves the zero-allocation requirement without introducing a separate saga transaction, inbox, outbox, retry or lock provider. See the [state-transition guide](../website/docs/guides/sagas.md) for supported registration and calls.

## D44. MessagePack uses native generated formatters directly

**Context.** MessagePack-CSharp 3.1.12 provides `MessagePackSerializer.Serialize`/`Deserialize`, native LZ4 compression, `BuiltinResolver`, generated formatters and buffer readers/writers. Strict Native AOT publishing of the high-level APIs retains their global default resolver and reflective formatter paths even when generated options are supplied. `BuiltinResolver` also retains reflective `Type` formatter construction. This conflicts with D8's AOT contract; see [upstream trimming issue #1666](https://github.com/MessagePack-CSharp/MessagePack-CSharp/issues/1666).

**Decision.** The optional MessagePack adapter calls native `IMessagePackFormatter<T>` methods with `MessagePackWriter` and `MessagePackReader`, passing explicit native options. The application supplies generated resolvers and the concrete primitive formatters it needs. There is no default resolver, runtime formatter discovery, or Relayer formatter registry. Preserve native old-spec and security settings. Reject compression options during construction rather than duplicating MessagePack's LZ4 implementation or silently ignoring the option. Reject nil bodies and trailing values, consistent with the message contract. Wrap formatter errors in the native serialization exception for a uniform malformed-body contract.

**Consequences.** Uncompressed native wire bytes and segmented buffers work under Native AOT without warning suppressions. Applications using native untrusted-data security must enable `MessagePack.Security.UseNonReflectiveObjectComparer` with trimming, retaining collision resistance while rejecting unsupported object keys. Custom formatters and complex contract graphs need application validation. Full high-level serializer/compression support waits for warning-clean upstream APIs; no parallel compression implementation is introduced.

## D45. Tenant store selection reuses native scoped factories

Replaces the proposed library-owned `IOutboxStoreResolver` facade in design §6.6. The remaining tenant
propagation, durable metadata and bounded dispatcher requirements are unchanged.

**Context.** EF Core 10.0.14 already creates options through an `IServiceProvider` callback with scoped
options and context lifetimes. Npgsql's 10.0.3 EF provider accepts an application-owned `NpgsqlDataSource`,
and the native connection string supports database and search-path selection. Relayer's existing
interceptors enlist that context's connection and transaction. Its PostgreSQL transaction overloads
also accept native connections or data sources. A second resolver contract would wrap the same native
configuration without adding a durable guarantee.

**Decision.** Applications select authorized tenant stores through their normal scoped factories and
pass native configuration through unchanged. Relayer does not add a competing resolver or native
configuration model. Validate manual scoped outbox units and explicitly composed tenant endpoints
against multiple PostgreSQL databases and schemas, including overlapping inbox IDs and rollback.
Application-owned data sources retain their native ownership and pool semantics.

**Consequences.** With `AddRelayerTenancy`, single-message `EntityFrameworkCore<TContext>` consumers
compose without resolving a startup context. Applications authorize and select stores through native
scoped factories after header restoration. The boundary validates provider, context pooling and
outbox options before transactions in delivery/retry scopes; native execution strategies remain
authoritative. The existing immutable outbox configuration is attached through EF's native options
extension facility, so delivery validation does not enumerate interceptor sequences or retain a
worker-lifetime tenant cache. Providers without tenancy retain startup validation. Saga state model
validation still requires a startup context.

For durable tenant/store/header binding, the same native scoped factory supplies an
`OutboxTenantBinding` alongside the existing store options after authorizing the tenant and selecting
the native resource. It is an application assertion about store ownership, not another store resolver
or an authentication mechanism. Tenant-enabled PostgreSQL and EF outbox registrations require it.
The immutable binding validates the current scoped tenant, exactly one matching outgoing tenant header,
and any supplied durable metadata; it fills missing durable metadata with the authorized ID. Rejected
enlistments invalidate the unit. Transaction commit revalidates the scope, while EF also validates
before transaction start and SaveChanges. Native transaction and retry semantics remain authoritative;
applications create a fresh authorized scope per retry. Tenant enumeration and a shared dispatch
connection budget remain separate work.

## D46. SQL Server primitives use native SqlClient with an explicit AOT boundary

**Context.** #288 requires Microsoft.Data.SqlClient-backed durable store primitives. SqlClient 7.1.1 supplies
native pooling, cancellation, typed SQL parameters and conditional autocommit statements. A strict .NET 10
Native AOT publish of its open/read/mutation paths fails with IL2026, IL2057, IL2067 and IL2070 in native
diagnostics, authentication and configuration code. This remains tracked by
[SqlClient #1947](https://github.com/dotnet/SqlClient/issues/1947); the authentication subset has its own
[pending backport #4349](https://github.com/dotnet/SqlClient/issues/4349). The pinned release has no complete
warning-clean path. SqlClient's object-valued parameter API also boxes scalar values on assignment.

**Decision.** Apply the optional-native-integration boundary used in D39: SQL Server schema tooling stays
AOT safe, but constructing or registering its durable store exposes `RequiresUnreferencedCode` and
`RequiresDynamicCode` diagnostics. SQL Server runtime trimming and Native AOT remain unsupported until the
native client supplies a warning-clean path. Keep the core AOT smoke independent of this optional provider;
do not suppress native warnings, weaken its CI gate, or introduce another SQL client/authentication stack.
All other packages keep their existing contracts. Reuse the existing binary header codec through the core.
Store IDs and fencing tokens as RFC-order binary(16), because SQL Server uniqueidentifier ordering does not
match UUIDv7 ordering. Use datetimeoffset(7) for UTC-instant eligibility and atomic conditional statements for
fencing. Reuse per-store exactly typed scalar boxes and UUID/payload buffers at the native API boundary.
Reject ambient transactions so dispatcher mutations complete durably in native autocommit.

**Consequences.** SQL Server reads, claims and health queries are usable independently of future enqueue,
inbox, workers and maintenance integrations. Native driver allocations and I/O remain visible in performance
coverage; Relayer does not install its own pool, retry engine or reflection path. The SQL Server AOT exception
is explicit at the API and in the site, and does not change ordinary direct send/receive guarantees.
Follow-up [#309](https://github.com/thomhurst/Relayer/issues/309) tracks warning-clean native runtime support.

## D47. SQL Server outbox transactions reuse native bulk copy

**Context.** #290 adds SQL Server ADO.NET/Dapper enlistment to D46's durable primitives.
SqlClient 7.1.1 supplies local transactions and `SqlBulkCopy` with an external `SqlTransaction`,
streaming `DbDataReader` input, constraint checks and insert triggers. Like Npgsql in D32,
`SqlCommand.Transaction` accepts only its provider transaction, so Dapper cannot pass a wrapper to
commands created on the original connection.

**Decision.** Apply D32's transaction and borrowed command-connection lifecycle to SQL Server.
Copy original envelopes using the core's lossless binary header codec, RFC-order UUID bytes and
established UTF-8 lane hash. Commit streams reusable rows into native `SqlBulkCopy` with
`CheckConstraints | FireTriggers` on the business transaction, then commits natively. Reuse native bulk
copy instead of row SQL, table-valued schema types, a private batch engine or a custom pool.
A connection-string overload owns its opened pooled connection; the open-connection overload leaves
the caller's connection open. Immutable options bind explicitly and validate on startup. Savepoints,
nesting, ambient transactions and concurrent scope use are rejected or unsupported. D46's explicit
SqlClient trimming/AOT restriction also applies to enlistment construction and registration.

**Consequences.** Business writes and sends are atomic. Failure, cancellation and disposal release the
buffer and dispose the uncommitted native transaction. A lost commit acknowledgement remains an unknown
outcome. Cancellation remains native: some server-side bulk-copy waits may require the server response or
native timeout before returning ([SqlClient #4965](https://github.com/dotnet/SqlClient/issues/4965)).
No parallel cancellation/retry engine is added. Completed Task read results and byte/character storage are reused after warmup at the streaming boundary. Benchmarks expose
native allocations separately from owned-buffer copying. Enlistment does not add SQL Server EF Core,
inbox, workers, scheduling or maintenance; future scheduling through ISender fails until a matching
worker exists. See the [SQL Server guide](../website/docs/reference/sqlserver-store.md).

## D48. MySQL enqueue owns native transactions and reuses MySqlBatch

**Context.** MySqlConnector commands accept only native MySqlTransaction instances. Transactional
business writes and buffered sends need one explicit commit boundary, with the D32 connection-view
pattern for ordinary ADO.NET and Dapper calls. MySqlConnector 2.6.2 already supplies MySqlBatch,
including packet construction, execution and cancellation. MySqlBulkCopy requires local-infile
configuration and a reader or row source, which is unnecessary for ordinary transactional enqueue.

**Decision.** MySqlOutbox returns a DbTransaction wrapper over a native transaction. Commands from
transaction.Connection unwrap that same transaction. Commit flushes owned messages through native
MySqlBatch before native commit. Rollback, failed flush and disposal discard the complete unit.
The data-source overload owns only its opened connection; caller-supplied open connections remain
application-owned. Core shares the existing owned message buffer, metadata validation, header codec
and stable lane hash with PostgreSQL. MySQL parameters reuse network-order UUID bytes, UTC timestamp
buffers and reusable MemoryStream array slices without changing native connection settings.

**Consequences.** No parallel batching or bulk-loading protocol is introduced. MySqlBatch is marked
experimental upstream and does not guarantee one round trip on MySQL; packet and execution behavior
remain native. Savepoints, nested transactions, overlapping scope operations and ambient transactions
are rejected or unsupported. Participating tables must use InnoDB. Callers must not execute DDL,
transaction-control SQL or other implicit-commit statements: MySQL can commit those independently of
the wrapper, and Relayer cannot restore atomicity afterward. Lost COMMIT acknowledgements retain the
native unknown-outcome semantics. Scheduling support remains false until matching workers exist;
transactional enqueue alone never promises delivery. See the [MySQL store reference](https://relayer.tomlonghurst.dev/docs/reference/mysql-store).

## D49. SQL Server commit dispatch reuses native RPC batches

**Context.** #329 adds independently useful unordered dispatch after D47's native commit. SqlClient 7.1.1's
[`SqlBatch`](https://github.com/dotnet/SqlClient/blob/v7.1.1/src/Microsoft.Data.SqlClient/src/Microsoft/Data/SqlClient/SqlBatch.cs)
groups commands through native RPC batches, including cancellation and sequential result reading.
[`SqlDependency`](https://github.com/dotnet/SqlClient/blob/v7.1.1/src/Microsoft.Data.SqlClient/src/Microsoft/Data/SqlClient/SqlDependency.cs)
uses process-wide listener state and Service Broker query notifications. Service Broker's native conversation
queues require explicit database infrastructure; they do not represent this process's already-owned committed buffers.

**Decision.** Reuse the PostgreSQL direct-dispatch lifecycle: after durable commit, transfer owned buffers to a
bounded local queue, group destinations using the shared bounded capability cache, and invoke the ordinary
named native transports. Use SqlBatch for independently fenced autocommit claims and completion, preserving
binary UUID order and native pooling. Read explicit per-statement row counts so trigger row counts cannot
inflate successful completion. Do not add a cross-process listener, notification runtime or recovery poll.
Service Broker remains an optional mechanism to assess for recovery work, not a prerequisite for local commit dispatch.

Require an explicit native Initial Catalog for the dispatcher. Compare the writer's actual database and data source
before opening a transaction, without opening a second pooled connection (which could exhaust Max Pool Size = 1).
Use exact database spelling so case-sensitive databases cannot be confused. Connection strings and all native
pool/authentication settings pass through unchanged. The ordinary transaction integration still permits login defaults
when no dispatcher is selected. Configuration mismatches fail explicitly rather than stranding rows in another store.

**Consequences.** SQL Server unordered commit dispatch is opt-in; ordered dispatch, inbox, recovery, poison rows and
retention remain separate work. Full queues, shutdown, future rows and unknown send outcomes retain durable rows.
Native failures update attempts under the live token; successful sends followed by failed or expired completion do
not consume another send attempt. D46's unsupported trimming/AOT boundary remains explicit. Buffer-transfer
benchmarks isolate Relayer's reusable copy/swap/group work; database and native transport costs remain separate.

## D50. Ordered lanes follow a database-assigned insert sequence

**Status.** Accepted. Implemented for PostgreSQL, MySQL (D63) and SQL Server (D64).

**Context.** D34 reads each lane in ascending `id` order. The `id` is a UUIDv7 created on the application
host when the message is sent, while its row is inserted at flush, immediately before commit (D31, D32).
Two instances whose clocks differ can therefore give the later of two strictly sequential commits the
smaller `id`. When both rows are pending in one scan, the lane sends them in the wrong order for the same
ordering key. A handler that sends early and commits late has the same effect on one instance. D34 excludes
only concurrent writers from its ordering guarantee; these transactions are not concurrent.

**Decision.** Refine D34's read order while retaining its lane leases, locks, deadlines and fencing.
- The outbox table gains `seq`, a database-assigned identity value set when the row is inserted.
- Ordered lanes read and index pending rows by `(lane, seq)`. The parked-key anti-join compares `seq`. Unordered pending rows keep a partial UUID index and their existing read/recovery cursors.
- One transaction's rows are inserted in send order, so their `seq` values keep that order. PostgreSQL uses `UNNEST WITH ORDINALITY ORDER BY position`; shared EF contexts merge their reusable rows by a scope-local enqueue position before insertion.
- Ordered PostgreSQL dispatch carries `seq` as the reserved `relayer.outbox.sequence` decimal header, replacing producer-supplied copies. `MessageContext.OutboxSequence` parses it lazily; missing or malformed values are absent. Unordered dispatch does not add it. Consumers compare it only within one ordering key, source table and tenant; it is not a business version for concurrent writers.
- `id` remains the stable message identity for the inbox, native message IDs and fenced claims.
- PostgreSQL ordered redrive requires the original parked row and updates it in place. If it is missing
  or no longer parked, fail the entire management transaction with the row ID. The final failure transition,
  redrive and purge lock/update their durable rows transactionally, so this indicates inconsistent state,
  not an ordinary concurrent management race. Silently skipping it hides a broken ordering invariant;
  recreating it would assign a new sequence and could reorder the key. Operators must restore the original
  durable row and sequence or explicitly purge the dead copy to discard it.
- Other database stores adopt the same rule with their native identity or sequence feature.

**Consequences.** Sequential transactions are dispatched in commit order whatever the host clocks say.
Transactions whose business writes conflict already commit one after the other before they flush, so
their `seq` values also follow commit order. A sequence is non-transactional: it takes no lock that a
business transaction can wait on, and it cannot raise a serialization failure. Transactions that are
genuinely concurrent still have no defined order, and a row committed late can still be dispatched after
a higher `seq`; idempotent, version-guarded consumers remain necessary. Existing tables need an explicit
SQL migration (D30). The upgrade preserves existing rows' former UUID order because historical insertion order is unavailable; it must run with writers and dispatchers stopped, in one application-owned transaction where transactional DDL is supported. MySQL DDL commits implicitly; its stopped, multi-step upgrade requires a backup before deployment. Identity values can have gaps; keep the sequence's default cache of one and do not reset it.

**Alternatives rejected.**
- A per-key advisory lock held from flush to commit gives strict commit order, but makes same-key writers queue
  on a lock Relayer introduced.
- Ordering by `created_at` or a database timestamp has the same race as `id` at lower resolution.
- Dispatching only rows older than the oldest in-flight transaction removes late arrivals but orders by
  transaction start, which is wrong for writers that waited on each other.

## D51. The EF Core consumer boundary takes an isolation level and replays conflicts

**Status.** Accepted. Implemented for PostgreSQL by [#354](https://github.com/thomhurst/Relayer/issues/354).

**Context.** D36's consumer boundary owns the transaction around the handler and opens it at the provider
default. The handler must not end or replace it, so an application that needs Repeatable Read or
Serializable for a multi-row invariant cannot use the boundary. Under those levels a conflict is an
expected outcome, not a poison message, and PostgreSQL reports a duplicate inbox key committed after the
snapshot as a serialization failure rather than as zero inserted rows.

**Decision.** Refine D36.
- The boundary accepts an optional isolation level and passes it to the native transaction. The default
  stays the provider default.
- The database package classifies native serialization failures and deadlocks from the handler or the
  commit as conflicts. A conflict replays the complete handler in a fresh scope within a bounded budget
  and does not count toward dead-lettering. Exhaustion follows ordinary failure handling.
- The core stays free of database dependencies; classification lives beside the provider.

**Consequences.** Handlers can rely on database isolation instead of hand-written locks. A replay after an
inbox conflict sees the committed key and skips the handler. Explicit isolation without an application
retrying strategy selects EF Core 10.0.14's native `ExecutionStrategy`, with three zero-delay retries
and PostgreSQL `40001`/`40P01` classification beside the provider. EF owns exception unwrapping,
budget enforcement and exhaustion; D36 supplies complete fresh-scope callbacks. No parallel retry
loop is introduced. These conflicts guarantee rollback, so no unknown-commit verification is needed.
Application-configured retrying strategies remain authoritative for classification, count and delays,
including conflicts; no second retry budget wraps them. Omitting isolation and native retries retains
the original path. Isolation is selected once at composition and passed to the native overload;
the default gains no per-message branch, scope or allocation.

**Alternatives rejected.**
- Letting the handler open its own transaction breaks the atomic inbox, business and outbox commit.
- Sending conflicts through broker redelivery spends the delivery count on expected contention.

## D52. Allocations a native API requires count as native-client cost

**Context.** D18 budgets zero allocations per message from Relayer's own pipeline and excludes the serializer
and the native client. Three transports stalled on allocations that sit exactly on that line: the Service Bus
processor callback must return a `Task` ([#24](https://github.com/thomhurst/Relayer/issues/24)), the AWS SDK
takes the SQS body and attributes as `string` ([#223](https://github.com/thomhurst/Relayer/issues/223)), and
RabbitMQ.Client accepts header values only as exact-length arrays or strings
([#273](https://github.com/thomhurst/Relayer/issues/273)). Relayer's code performs each allocation, but any
caller of that client pays it. D1 wraps native clients; a budget that blocks a transport because of its
client's signature contradicts that. Per-message DI scopes for scoped handlers raised the same question.

**Decision.** Refine D18's exclusion; its 5% throughput and startup budgets are unchanged.
- An allocation counts as native-client cost when the native API's signature requires that object and a
  raw-client benchmark doing the same work allocates at least as much. The measured comparison against the
  raw client, not absolute zero, is the test at the native boundary.
- Each such allocation is named in the transport's site page and pinned by a benchmark or allocation test, so
  it cannot grow unnoticed.
- Where the client could plausibly accept a non-allocating form, file the request upstream and remove the
  conversion when it ships. Do not build a replacement for the client call (D42).
- Retained memory is bounded. A cache that removes a recurring boundary allocation has a fixed capacity and
  allocates on a miss; unbounded retention is never an acceptable way to reach zero.
- A scoped handler's DI scope, its service cache and the handler instance are application lifetime cost, not
  pipeline cost. Singleton handlers keep the zero-allocation path.
- Everything else Relayer adds per message stays at zero, and still needs maintainer approval to change.

**Consequences.** Service Bus receive, SQS send and RabbitMQ send can ship on their native clients. Budgets are
stated per transport as "Relayer adds N B over the raw client" with the native-required objects listed.
Reviewers check the raw-client comparison and the pin instead of asking for a maintainer exception each time.
The RabbitMQ header cache is temporary: the client accepted a request for memory-backed header values
(rabbitmq/rabbitmq-dotnet-client#2144).

**Alternatives rejected.**
- Strict absolute zero: blocks every client whose API takes strings or returns `Task`.
- Per-transport exceptions granted one at a time: the same question, re-argued for each adapter.
- Bypassing the client (direct HTTP or AMQP framing) to avoid its allocations: replaces what D1 wraps.

## D53. Singleton-only consumers open delivery scopes on demand

**Context.** D18 requires zero allocations from Relayer's own pipeline. Microsoft DI scopes cannot be pooled, and
each fresh scope allocates even when a singleton handler never uses it. The failure pipeline also resolved the
scoped `ISender` for every attempt, which grew the scope's service cache and, because pooled senders were
disposable, its disposal list.

**Decision.** A failure-pipeline consumer whose handlers are all singletons resolves them from a long-lived
composition scope and opens the attempt's scope on the first `MessageContext.Services` read. That read also attaches
the attempt's scoped sender, so buffering and failed-attempt semantics are unchanged. Lifetimes come from the
final registration in `IRelayerBuilder.Services`. Scoped and transient handlers, tenant propagation, batch consumers,
the explicit failure-delegate path and pipelines composed outside `AddRelayer` keep one eager scope per attempt.
Pooled senders are not disposable: `EndAttempt` returns them, so scopes record nothing for them.

**Consequences.** A singleton handler that does not read `Services` adds no DI allocation. Default scoped handlers
keep their scope, service cache and handler instance, classified as application lifetime cost by D52. A
sender resolved from an ordinary application scope is no longer returned to the pool, so that scope may allocate one
sender, and an attempt that finds the pool empty allocates one too.

**Alternatives rejected.** Reusing scopes would leak scoped state between deliveries. Finding the current attempt
through `AsyncLocal` allocates on every write, and a thread-static is unsafe once middleware suspends. Inferring
lifetimes by resolving handlers twice at startup would construct application services as a side effect.

## D54. Service Bus processors drain durable work before settlement

**Context.** Native processors own receive concurrency, sessions, retries of transport operations and
lock renewal. Their stop operation cancels the native callback token before awaiting callbacks.
Passing that token into durable handling prevents an otherwise graceful stop from completing settlement.
A topic/subscription delivery also cannot be retried by scheduling a copy to its topic without delivering
that copy to unrelated subscriptions.

**Decision.** Compose one core pipeline per native processor declaration. Pass the unchanged native
options, require PeekLock and AutoCompleteMessages=false, and validate portable concurrency against
the native concurrency at startup. Non-session and session bindings retain their original callback
objects. Core settlement runs inside the process span after durable work and asynchronous scope disposal.
A consumer-owned cancellation token keeps that work alive during native stop; the host drain deadline
cancels it. A cancelled drain requires a new host. The native callback token remains available unchanged.
Successful manual native settlement is final: a later handler exception propagates without another
settlement or retry, and portable and native settlement cannot be combined in one attempt.

**Consequences.** Immediate abandon and dead-letter operations retain failure metadata through native
property modifications. Positive delayed redelivery is rejected at composition; applications can use
in-process RetryDelays. No retry copy is broadcast through a topic. The optional
Relayer.AzureServiceBus.HealthChecks companion package keeps health dependencies out of the transport.
Its receiver slots track the oldest active callback only when explicitly enabled, and connectivity
uses read-only peek or a fresh send-link attachment on the existing client, never a probe message.
Fresh send links are necessary because the SDK caches message-batch limits after the first attachment.
The processor callback type is `Func<ProcessMessageEventArgs, Task>`, and a `Task` is single-use, so a suspended
callback needs a new one. `ReceivePipeline.ProcessSettledAsync` returns a plain `ValueTask` because settlement has
already run, and `AsTask` then allocates the smallest available `Task`; health tracking stays in a pooled frame.
D52 classifies the required suspended callback Task as native-client cost, pinned against a raw SDK callback.

**Alternatives rejected.** Mutating native options hides configuration mistakes. Auto-complete and
ReceiveAndDelete can settle before work is durable. Treating the native stop token as the drain deadline
abandons successfully completed work at shutdown. Scheduling subscription retries through their topic
changes native routing. Adding health dependencies to every transport user violates optional integration.

## D55. MySQL direct dispatch reuses native batches and fenced row counts

**Context.** MySQL enqueue (D48) commits durable rows but has no commit-triggered sender.
MySqlConnector 2.6.2 already provides packet construction, execution and cancellation through
[MySqlBatch](https://github.com/mysql-net/MySqlConnector/blob/2.6.2/src/MySqlConnector/MySqlBatch.cs).
Its per-command `RecordsAffected` is hardcoded to zero, tracked in
[MySqlConnector #1696](https://github.com/mysql-net/MySqlConnector/issues/1696). MySQL's Event
Scheduler executes SQL at scheduled times; it cannot notify application-owned native transports
of a local transaction commit.

**Decision.** Apply D33/D35's bounded owned-buffer handoff and capability cache to ADO.NET
MySQL commits. Reuse native conditional InnoDB updates/deletes, `MySqlBatch` and `SELECT ROW_COUNT()`
for each fenced outcome. Keep UUID and timestamp parameter storage reusable, as in D48. Do not
implement a second packet protocol or replace native transport sends. The writer and worker's
explicit database, server, port and table prefix must agree; validate before the transaction.

**Consequences.** MySQL batching remains experimental upstream and does not promise one round
trip. Partial claim batches may leave expiring claims after an error; no send begins without all
outcomes being read. Completion failure never increments broker failure attempts. Queue overflow,
shutdown, expiry and post-send database errors leave durable work with the same at-least-once
boundary as D33. Ordered dispatch, inbox and recovery remain separate deliverables; no eventual
delivery guarantee is inferred from commit notification alone. See the
[MySQL reference](../website/docs/reference/mysql-store.md#commit-triggered-unordered-dispatch).

## D56. PostgreSQL CDC wakes existing unordered recovery

**Status:** Accepted.

**Context.** Npgsql 10.0.3 already provides `LogicalReplicationConnection`, `pgoutput` decoding, persistent
slots and WAL feedback. Relayer already owns durable reads, fenced claims, native transport grouping and
completion in PostgreSQL recovery. Replicating payload buffers or building another send/checkpoint runtime
would duplicate those mechanisms. Logical replication commit events do not notify future due times or
claim expiry. The existing schema differs from Debezium's default column names.

**Decision.** Register an explicit optional native replication connection factory and pass native slot and
pgoutput options unchanged. Require non-streaming V1 committed transactions and an application-provisioned
persistent slot with INSERT-only, unfiltered outbox publications. Capture triggers bounded unordered
recovery through a fixed ID boundary, including an initial catch-up on each connection. PostgreSQL owns
slot exclusivity; existing row fencing arbitrates with direct dispatch and the maintenance sweeper without
acquiring its idle leadership lease. Feedback follows committed database mutations, never a cancelled or
failed database transaction. Native send failures remain durable and may be checkpointed; maintenance
handles retries, expired claims, due schedules and retention. Ordered rows remain with existing lane leases.

**Consequences.** CDC avoids waiting for a polling interval for new commits without changing native sends,
table layout, ordering or at-least-once recovery. A crash after broker acceptance can duplicate delivery.
The application provisions and monitors WAL retention and must use the writer's database for both
connections. Native transient failures reconnect; other runtime failures fault the worker and stop the
host under its default BackgroundService exception policy. Dispatch runs inline, so broker stalls delay
later capture and can grow retained WAL. Operations must monitor retention and repair permanent failures.
SQL Server CDC remains planned. Debezium can use explicit column mappings and connector-specific binary
header/payload handling, with a separate slot and one delivery owner; it does not inherit Relayer's lane,
scheduling or settlement protocol. See the [CDC reference](../website/docs/reference/postgres-cdc.md).

## D57. Google Pub/Sub uses prepared native API clients

**Context.** Google.Cloud.PubSub.V1 3.39.0 already provides PublisherClientImpl batching, ordering and
retry behavior, and SubscriberClientImpl streaming recovery, flow control, lease extension and callback
Ack/Nack settlement. Its public constructors accept prepared native API clients. The convenience builders
retain Google authentication/JSON and reflective gRPC transport selection that fail strict AOT publishing,
even with GrpcNetClientAdapter selected. This limitation is tracked in
[google-cloud-dotnet #13328](https://github.com/googleapis/google-cloud-dotnet/issues/13328).

**Decision.** Require caller-owned PublisherServiceApiClient and SubscriberServiceApiClient instances.
Pass native publisher/subscriber settings unchanged through their native Clone operations; create native
high-level runtimes through their public constructors. Expose API clients and topic publishers through a
keyed typed service and the native delivery feature. Applications own channel and authentication setup.
Do not wrap authentication, add another reconnect/flow-control engine, suppress AOT warnings or weaken D4.
The strict AOT smoke uses managed gRPC public constructors and generated JSON against the emulator;
production authentication paths need separate application validation.

The native subscriber intentionally faults on an initial streaming-write `IOException`, including a
replacement stream opened after an established connection drops (see
[google-cloud-dotnet #16088](https://github.com/googleapis/google-cloud-dotnet/issues/16088)). Preserve
that fatal lifecycle: applications recreate the host against the existing durable subscription instead
of adding an adapter retry engine. Outage conformance may explicitly recreate its DI application only
after recognizing the exact native subscriber failure caused by the injected TCP reset. Unrelated
shutdown errors remain failures, and every acknowledged message must still arrive on the same endpoint.

Nack cannot change broker attributes, and native DeadLetterPolicy cannot supply on-demand dead-lettering
with Relayer error history. Keep these capabilities unavailable to the core durable failure policy. Reuse
the existing core retry and acknowledged dead-letter-copy path, with explicit destination topics. Native
Nack remains the SDK's unchanged response to callback failures. PublisherClient.PublishAsync has no
cancellation parameter: hold borrowed storage through completion, then observe cancellation. Never
simulate scheduling, transactions or settlement guarantees the broker does not provide.

The service limits attribute values to 1,024 UTF-8 bytes. Frame larger logical Relayer error histories
losslessly across reserved relayer.failure.history.part.N attributes with a relayer.failure.history.parts
count, and reconstruct only on an error-history lookup. This is Relayer metadata serialization, not a
second broker delivery mechanism. Check the native 100-attribute and key/value limits on failure copies;
if complete metadata cannot fit, fail without acknowledging or silently truncating the original.

**Consequences.** Native send batching and streaming concurrency remain native; declaration Concurrency
must equal the native maximum outstanding element count. Pub/Sub CloudEvents use the native ce- attribute
binding. Native callback replies queue acknowledgements after durable work; ack spans describe that
handoff, not confirmation of an acknowledgement RPC. Native-required strings and callback Tasks remain
visible in native comparisons; Relayer pools its own delivery and send state. See the
[transport guide](../website/docs/transports/google-pubsub.md) for lifecycle, capability and validation limits.

## D58. MySQL consumers share the ADO.NET outbox transaction

**Context.** MySqlConnector 2.6.2 already owns native begin/commit/rollback, cancellation and
rollback-on-disposal. D48's scoped outbox and command connection already enlist business commands
and buffered sends in one native transaction. The existing consumer transaction middleware seam
and settlement pipeline order durability and scope disposal before acknowledgement. PostgreSQL's
EF Core boundary (D36) provides the behavioral reference, but cannot run on a MySQL ADO.NET scope.

**Decision.** Add an explicit MySQL single-message consumer boundary using those existing pieces.
Deduplication remains opt-in. InnoDB's primary key on exact consumer bytes and network-order UUIDv7
identity serializes duplicates; native `INSERT IGNORE` reports an insert independently of
`CLIENT_FOUND_ROWS`. Strict UTF-8 consumer validation bounds inputs to the schema so truncation is
not an alternative successful result. Do not add an application lock, native retry loop or commit
verification mechanism. Native failures propagate to complete-attempt consumer failure handling;
redelivery of an unknown commit checks the same durable inbox key.

Expose the scoped outbox's current command transaction. Reject handler commit/rollback while the
consumer owns it, detect premature disposal, and use the existing buffered outbox flush for commit.
Request completion before commit so disposal cannot reject already durable work. Noncomplete
outcomes and exceptions dispose the transaction and roll back before normal failure handling.

**Consequences.** The integration supports ADO.NET/Dapper business writes on InnoDB and optional
unordered direct dispatch. Native implicit commits, DDL and nontransactional business tables remain
unsupported, as in D48. EF Core, saga persistence, ordered workers and inbox retention remain
separate work. Existing scoped enlistment/transaction allocations remain visible in comparisons;
no inbox buffers are allocated on the enqueue-only path. See the
[MySQL guide](../website/docs/reference/mysql-store.md#atomic-consumer-inbox).

## D59. SQL Server consumers reuse the ADO.NET outbox transaction

**Status.** Accepted.

**Context.** #332 needs atomic inbox identity, handler business writes and outgoing rows. The existing
EF consumer is specific to Npgsql and cannot enlist SQL Server business writes. SqlClient 7.1.1's
[SqlTransaction](https://github.com/dotnet/SqlClient/blob/v7.1.1/src/Microsoft.Data.SqlClient/src/Microsoft/Data/SqlClient/SqlTransaction.cs)
already owns native commit, rollback and disposal, and D47 wraps it for ADO.NET/Dapper enlistment.
SQL Server primary keys and `UPDLOCK, HOLDLOCK` supply uniqueness and serializable key-range locking.
No distributed lock, duplicate exception loop, pool or retry engine is needed.

**Decision.** An explicit outer `.SqlServer(consumer, connectionString, inbox: true)` boundary owns
the existing scoped outbox transaction. Expose its borrowed `CurrentTransaction` for business commands.
Insert the `(consumer, message_id)` inbox key before handling through a conditional insert protected
by native update/range locks. A competing delivery waits for commit or rollback; a committed key skips
the handler. Consumer names are binary case-sensitive, bounded to 256 UTF-16 code units and reject
trailing spaces so SQL padding cannot alias accepted names. Inbox UUIDs use native `uniqueidentifier`
for equality; ordered outbox IDs retain D46's binary representation.

Commit/rollback by handlers is rejected; premature disposal rolls back and fails the delivery. Ordinary
failure handling receives native failures after transaction disposal. Retry/dead-letter requests roll back;
otherwise request immutable completion before committing, as in D36. Count duplicates only after commit.

**Consequences.** The opt-in inbox supports ADO.NET/Dapper single-message consumers with the existing
SQL Server outbox registration, schema/options and direct dispatcher. EF Core, sagas, batch consumers,
provider retry strategies and inbox retention workers remain separate work. The connection string fixes
the database per pipeline; outbox options fix the schema/prefix at composition. A mismatching reloaded
scope fails before opening a transaction; restart the pipeline to select a new schema/prefix. D46's trimming/AOT restriction
continues to apply. See the [SQL Server guide](../website/docs/reference/sqlserver-store.md#atomic-consumer-inbox).

## D60. Bound catalog dispatch by native connection lifetimes

**Context.** Npgsql 10.0.3 `PoolingDataSource` sets `MaxConnections` from that data source's
`MaxPoolSize`; returning a connection retains its connector in that pool. `NpgsqlDataSource`
`OpenConnectionAsync` and `CreateCommand` use native pooling/connection ownership. There is no
aggregate pool limit over independent application-owned sources. Relayer already has fenced direct
claims, transactional unordered recovery/retention and ordered membership/lane leases. Replacing
those with tenant-specific implementations would duplicate their durability and ordering semantics.

**Decision.** Add a PostgreSQL `ITenantSource` snapshot of authorized native data sources and explicit
store options, with one fixed-concurrency scheduler invoking those existing operations serially per
store. The scheduler's admission limit bounds all catalog connections, including leases and release.
Do not start hosted workers per tenant, mutate native configuration, build a pool, or wrap native
connections. Require application-owned native `Pooling=false` dispatcher sources so idle per-source
pools cannot accumulate physical connections beyond that limit. Writers may use separate native
pooled sources; existing fixed-store registrations retain their configuration and behavior.

**Consequences.** The physical bound applies to catalog dispatch, not business transactions or other
workers. Unpooled native connection setup is visible in the catalog benchmark; this opt-in feature
makes no throughput claim against pooled fixed-store dispatch. Applications that require pooled
dispatch can continue using fixed-store registrations with explicitly managed per-source budgets.
Catalog refreshes and turns are finite, bounded by cancellation deadlines. Per-store reusable buffers
and queues live only while cataloged; removing a store retires queues and releases fenced leases
under the same admission limit. Native expiry recovers interrupted release. Source ownership,
authorization, provisioning and immutable snapshot publication remain application responsibilities.

## D61. Aspire reuses native resource wiring and application telemetry

**Context.** Aspire 13.6 supplies Kafka and Service Bus resources, `WithReference`
connection injection, readiness probes and OTLP dashboard configuration. Relayer already
supplies native transport option builders, keyed health adapters, durable outbox stores
and its `Relayer` ActivitySource/Meter. The original package table proposed an
`Aspire.Hosting` dependency without an application-side connection contract.

**Decision.** Keep AppHost resource declaration on Aspire's native APIs. `Relayer.Aspire`
is an optional, transport-neutral application-side package: `AddRelayerInstrumentation`
subscribes the application's existing OpenTelemetry providers. Typed `UseAspireConnection`
methods live in each existing transport package and configure its named options from
`ConnectionStrings`. Applications therefore acquire only their chosen native clients;
`Relayer.Aspire` depends on core and OpenTelemetry hosting, with no transport or AppHost
orchestration dependencies. Native configuration callbacks stay reachable.
Health registration and HTTP readiness probes use existing APIs directly. Outbox resource
connections go to the existing database adapters; schema, transaction enlistment and workers
remain explicit. No native resource, exporter, client, health runtime or transaction is reimplemented.

**Consequences.** Configuration is captured during registration and connection changes
require restart. Applications choose sampling/exporters (including those from ServiceDefaults),
authentication and health policy. The sample uses native Kafka and Service Bus emulator resources;
other transports can use their existing configuration directly. See the
[Aspire guide](../website/docs/guides/aspire.md).

## D62. SQL Server maintenance reuses direct dispatch and native transaction locks

**Context.** D47/D49's durable SQL Server rows survive missed commit notifications and crashes.
SqlClient 7.1.1's [SqlDependency source](https://github.com/dotnet/SqlClient/blob/v7.1.1/src/Microsoft.Data.SqlClient/src/Microsoft/Data/SqlClient/SqlDependency.cs)
provides Service Broker query notifications, including process-wide listener state. Notifications do not
replace retry, scheduled-row recovery or bounded retention. We inspected that implementation and SQL Server's
[Service Broker facilities](https://learn.microsoft.com/en-us/sql/database-engine/configure-windows/sql-server-service-broker)
and [sp_getapplock](https://learn.microsoft.com/en-us/sql/relational-databases/system-stored-procedures/sp-getapplock-transact-sql).

**Decision.** Apply D38's bounded high-water traversal and separate durable idle leases to SQL Server.
Use native transaction-owned application locks with zero wait to arbitrate connected leadership, and hold
that transaction through dispatch and retention. Reuse the direct dispatcher, capability cache, native batches
and fenced mutations on that same connection, avoiding a second sender or waiting on a second pool slot.
Database clock expiry governs idle leadership. Row fencing still arbitrates against direct dispatch.
No new notification or wake mechanism is introduced; Service Broker remains application-owned.

Transfer malformed header rows to a raw dead copy under a live fenced claim. Count recovery configuration
failures and native send failures; an atomic update/dead-copy/delete transaction dead-letters exhausted
unordered rows and parks exhausted ordered rows. Retention preserves dead copies of parked barriers.
The maximum send-attempt setting must match all dispatchers for a store. Completion failures after native
success do not consume attempts. Future scheduled ISender registration remains separate; D64 adds ordered lane workers.

**Consequences.** Recovery is at least once; rollback after native acceptance replays the original identity.
A connected pass can delay its own cleanup, while another replica immediately skips its active leadership.
Explicit idempotent schema deployment creates missing dead and lease tables without implicit migrations.

## D63. MySQL ordered dispatch uses InnoDB lease rows and native identity

**Context.** MySqlConnector 2.6.2 supplies native transactions, cancellation and sequential
`MySqlBatch` commands. MySQL 8.4 supplies `AUTO_INCREMENT`, conditional updates and InnoDB row
locks. Event Scheduler runs SQL in the server; it cannot execute application transport sends
or track their completion.

**Decision.** Reuse D34's polling, membership, bounded admission and lease-row transaction lock
across native sends, using MySQL database time and conditional statements with explicit ownership
checks. Native identity follows the existing batch flush's send order (D48). Core shares the
capability cache and sequence-header projection. No parallel notification engine is introduced.

**Consequences.** Live lanes are fenced across workers, including slow or cancelling sends.
Native statement-level auto-increment locking remains unchanged; no per-key writer lock is added.
The explicit upgrade preserves former UUID order for old rows and documents implicit DDL commits.
Exhausted rows park in place, preserving their sequence and key barrier. Unordered recovery and
maintenance are provided by D65; parked-row repair remains application-owned SQL. Ordered polling
does not enable writer scheduling until registration can validate a matching scheduling worker.


## D64. SQL Server ordered dispatch reuses native locks and sequence ranges

**Status.** Accepted.

**Context.** #330 adds ordered dispatch to the existing SQL Server relational outbox. The pinned
[SqlClient 7.1.1 bulk-copy source](https://github.com/dotnet/SqlClient/blob/v7.1.1/src/Microsoft.Data.SqlClient/src/Microsoft/Data/SqlClient/SqlBulkCopy.cs)
provides native streaming and transaction enlistment, but its input order alone is not a database sequence
assignment contract. SQL Server supplies [native sequence range allocation](https://learn.microsoft.com/en-us/sql/relational-databases/system-stored-procedures/sp-sequence-get-range-transact-sql)
and conditional SQL updates with transaction/key-range locks. These cover the required primitives.
[Service Broker conversation-group locks](https://learn.microsoft.com/en-us/sql/database-engine/service-broker/conversation-group-locks)
serialize readers of Service Broker queues; they do not lease this existing relational outbox. Adopting those
queues would replace its persistence/write path. SqlClient's
[SqlDependency source](https://github.com/dotnet/SqlClient/blob/v7.1.1/src/Microsoft.Data.SqlClient/src/Microsoft/Data/SqlClient/SqlDependency.cs)
uses process-wide listener state and query notifications; this violates D17 and cannot supply due-time or
expired-lease recovery alone.

**Decision.** Apply D34's bounded sequential lane lifecycle using native conditional SQL and
`UPDLOCK, HOLDLOCK` on the lease key, retained through the send transaction. Database time controls
membership, ownership, schedules and fenced message mutations. Do not create a second lock service,
notification runtime, or custom database retry mechanism. Fair membership and send deadlines follow the
existing PostgreSQL worker, and both providers reuse the bounded endpoint capability cache and insert-sequence
header projection. Poll only bounded assigned-lane heads; Service Broker/query notifications are not installed.

Use one native `sp_sequence_get_range` reservation immediately before an ordered `SqlBulkCopy` flush;
map the returned range to buffered send positions. Mixed batches retain their relative send order as well.
This uses the database's own sequence allocator instead of relying on bulk insertion order or sorting UUIDs.
Unordered-only bulk copies keep server-default sequence assignment. UUIDs remain message/claim identities.
The explicit upgrade preserves legacy UUID order and resumes the native sequence above migrated rows.
Never restart it during ordinary operation; gaps and unordered concurrent writers are allowed under D50.

**Consequences.** Connected owners cannot knowingly overlap a lane, even if its lease expires during a
still-locked send transaction. Broken connections and uncertain native sends can still duplicate delivery;
no broker ordering or settlement is emulated. Exhausted ordered rows park in place with their original
sequence/error and a key-scoped barrier. SQL Server dead-letter management remains separate work;
unordered recovery and retention follow D62. Ordered membership uses a separate `ordered_leases` table
so existing maintenance `leases` rows keep their native uniqueidentifier ownership schema. D46's SqlClient AOT/trimming
boundary remains explicit. Ordered flushes add one native range RPC per transaction; lane reads/claims
and header preparation have dedicated managed and real-database benchmark coverage.

## D65. MySQL maintenance uses native session admission and existing dispatch

**Context.** Durable MySQL rows outlive missed commit notifications and crashed senders. MySqlConnector
2.6.2 supplies native transactions, commands, batches and pooled connections, but no application
outbox recovery runtime. MySQL Event Scheduler executes SQL, not application transport sends.

**Decision.** Use separate database-time sweeper/cleanup leases, with zero-wait native `GET_LOCK`
session admission scoped to database/table prefix. Hold admission across one bounded pass and
explicitly release it before returning the pooled connection. Connected passes cannot overlap
after idle leadership expiry. The database-time row preserves idle leadership between passes;
the session lock is authoritative while a pass is connected. Release uses the native command timeout
and logs cleanup failures without replacing a pass error; the native client discards broken sessions.
Row claims remain independently fenced autocommit mutations;
leadership does not replace row ownership. Session loss and broker uncertainty retain the existing
at-least-once boundary. Support one writable primary, not independent multi-primary servers.

Recover due unordered rows through D55's dispatcher and a fixed upper UUID boundary, revisiting
retries despite continuous inserts. Preserve ordered lanes and parked barriers. Quarantine malformed
rows using a raw dead copy under a live claim. InnoDB transactions atomically record failed attempts,
copy exhausted rows to dead storage, then delete unordered rows or park ordered rows. Failed
completion after broker acceptance never consumes attempts. Bound indexed dead/inbox retention
independently; preserve parked rows and their dead copies. Ordered decoding isolates malformed
rows through the existing lane failure/parking path, retaining the original sequence and raw payload.

**Consequences.** Maintenance is explicit, named, immutable configuration and needs at least two
pooled connections. No Event Scheduler privilege or server change is required. Native transports
still own broker retry, ordering and settlement. Deployment adds dead/lease tables without changing
existing definitions. Registration alone does not advertise new writer scheduling capabilities.

## D66. Durable local work reuses outbox rows and native PostgreSQL transactions

**Context.** #361 proposes process-local consumption of durable work without a broker round trip.
The existing PostgreSQL outbox already owns transactional enqueue, fenced row claims, ordered lane leases,
scheduling, recovery and poison-row parking. A local handler invoked from `ITransport.SendAsync` alone
cannot atomically commit its business work and the dispatcher's later deletion on another connection.

Native-first assessment: Npgsql [10.0.3 transaction source](https://github.com/npgsql/npgsql/blob/v10.0.3/src/Npgsql/NpgsqlTransaction.cs)
provides the active connection, commit, rollback and disposal. PostgreSQL 18
[notification source](https://github.com/postgres/postgres/blob/REL_18_0/src/backend/commands/async.c)
explicitly discards the notification queue at reboot; LISTEN/NOTIFY is a wake-up mechanism, not a durable
work queue. Native row locks, conditional DELETE and transactions already supply the storage primitives.
No database extension is required; introducing an extension-owned queue would add deployment requirements
and a second durable store alongside rows that already enlist in the business transaction.

**Decision.** Permit a narrow D1/D42 exception for the planned PostgreSQL local transport: reuse the outbox
as its durable queue, rather than emulating an existing broker's features. Do not build another table,
claim protocol, retry scheduler, ordering mechanism or dead-letter store. Reuse D50 lanes and the existing
poison transitions. Notifications may only accelerate discovery; durable rows remain authoritative.

First expose `PostgreSqlOutboxStore.CompleteAsync(claim, transaction, cancellationToken)` for independently
useful application-owned workers. It executes the existing fenced deletion against the supplied native
transaction's database and this store's schema/prefix. Expiry uses `clock_timestamp()` at deletion, not a
caller timestamp or the start of a potentially long transaction. The caller commits or rolls back; false
means it must roll back all associated business work. No transaction is retained or rebound to the store.

**Consequences.** #458 supplies the primitive; the transport and consume-boundary integration remain planned
in #459. They must carry row identity and claim token into each attempt and roll back on a lost fence.
Native row locks serialize deletion with competing claims. Rollback restores both business work and the row;
a successful deletion remains provisional until commit. This permits atomic completion only for work in
that same transaction, not exactly-once external effects. Existing broker dispatch and settlement are unchanged.
