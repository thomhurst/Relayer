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
| [D12](#d12-outbox-dispatch-direct-for-unordered-lane-leases-for-ordered) | Outbox dispatch: direct for unordered, lane leases for ordered | Accepted |
| [D13](#d13-kafka-gets-its-own-consumption-model) | Kafka gets its own consumption model | Accepted |
| [D14](#d14-transports-declare-capabilities-validated-at-startup) | Transports declare capabilities, validated at startup | Accepted |
| [D15](#d15-opentelemetry-in-the-core) | OpenTelemetry in the core | Accepted |
| [D16](#d16-topology-is-declared-default-mode-is-validate) | Topology is declared; default mode is Validate | Accepted |
| [D17](#d17-testing-is-a-first-class-product) | Testing is a first-class product | Accepted |
| [D18](#d18-performance-budget-enforced-in-ci) | Performance budget enforced in CI | Accepted |
| [D19](#d19-dependency-policy) | Dependency policy | Accepted |
| [D20](#d20-sagas-are-out-of-scope-for-v1) | Sagas are out of scope for v1 | Accepted |
| [D21](#d21-repository-conventions) | Repository conventions | Accepted |

---

## D1. Wrap native clients; do not abstract brokers

**Context.** Existing libraries model every broker as a queue. That causes real bugs: for example, Kafka being "force fit" into a RabbitMQ shape (Wolverine #3134) and offsets only being committed after a restart (Brighter #2269). Users also lose access to native features.

**Decision.**
- Each transport wraps the official native client. Native send, receive, ordering, settlement, and dead-lettering semantics are kept unchanged.
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
- librdkafka logs are routed to `ILogger`.

## D14. Transports declare capabilities, validated at startup

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
