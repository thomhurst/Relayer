# Relayer Development Guide

Relayer is a .NET messaging library that wraps the native client for each broker (Kafka through [Dekaf](https://github.com/thomhurst/Dekaf), Azure Service Bus, Amazon SQS/SNS, RabbitMQ). It exists to remove the ceremony and boilerplate around those clients: resiliency, outbox/inbox, telemetry and trace propagation, serialization, topology and testing. It is not a new broker abstraction, and it never changes how the underlying client sends, receives, orders or acknowledges messages. [docs/design.md](docs/design.md) says how it works and [docs/decisions.md](docs/decisions.md) says why; follow both, and record a new decision there instead of silently diverging from one.

## API design

The goal is code that is simple, intuitive and easy to write, without giving up power or performance ([design §1](docs/design.md#1-principles)).

- Write the user's call site first, in the style of [design §3](docs/design.md#3-developer-experience), then implement it. The common case takes a few lines and is correct by default: at-least-once delivery, poison messages dead-lettered rather than crashing or stalling a consumer, and settlement only after the work is durable.
- Wrap, don't replace (D1). Use native send, receive, settlement, ordering, partitioning, sessions and dead-lettering as-is. Never emulate a feature the broker already has, and reuse features the native client already ships where they fit (D22).
- Keep power reachable without cluttering the common path. Portable concerns are uniform across transports; transport-specific features are typed, discoverable extensions such as `context.Kafka()` or `o.ServiceBus(...)`, never a lowest-common-denominator option.
- Pass native configuration through unchanged (Dekaf's builders, `ServiceBusClientOptions`, `ConnectionFactory`, `AmazonSQSConfig`); never re-model it. Expose native objects through the feature collection (D6).
- One obvious way to do each thing. Do not add parallel APIs for the same job; for example, publish versus send is a routing concern, not a separate API ([design §3.4](docs/design.md#34-sending-from-anywhere)).
- Explicit, not magic (D4): no assembly scanning or hidden conventions. Registration is in user code or in source-generated code the user can read.
- Fail early with an actionable message: analyzers at compile time, capability and topology validation at startup (D16, D24). Never degrade behavior silently.
- When API simplicity and performance pull apart, keep the public API simple and move the complexity into internals or the source generator.

## Performance requirements

Performance is a feature ([design §12](docs/design.md), decision D18). The budget, measured against the raw native client in the same benchmark:

- At most 5% throughput overhead on the receive path and the direct send path (no outbox, default JSON serializer).
- Zero allocations per message from Relayer's own pipeline in steady state. Allocations by the serializer and the native client are excluded.
- Under 50 ms to build the pipeline for 100 handlers, and startup works under Native AOT.

Rules for code on the per-message path (pipeline, middleware, dispatch, serialization, transport adapters, outbox write and dispatch):

- No reflection, `Type` lookups, `MethodInfo.Invoke`, `dynamic` or runtime code generation. Dispatch is source-generated; the pipeline is composed once at startup.
- Keep per-message work synchronous where possible with `ValueTask`, and allocate 0 B per message. Pool context objects and buffers, read headers lazily, and amortize necessary work per batch, consumer or connection.
- Avoid LINQ, capturing lambdas, uncached delegates, boxing, string formatting, allocating iterators, per-message `Task.Run` or thread-pool hops, locks, and exceptions as control flow. Prefer spans, `IBufferWriter<byte>`/`ReadOnlySequence<byte>`, static callbacks with explicit state, channels, and `Interlocked`.
- Do not trade throughput, latency, CPU or allocations against each other without explicit maintainer approval, including in refactoring or review feedback.
- Add or extend a BenchmarkDotNet benchmark in `benchmarks/Relayer.Benchmarks` (with `[MemoryDiagnoser]`) only when a change can materially move steady-state per-message cost: it adds or removes an allocation, lock, await, copy, loop or data-structure operation on that path, or changes an algorithm, buffer strategy or threading. For anything else (bug fixes that add a constant-time branch, error and shutdown paths, configuration, builders, docs, tests), state the per-message cost in one line in the PR (for example "one bool check per message, 0 B") instead. Reviewers must not ask for a benchmark in those cases, and a finding that asks for one is non-blocking.
- Local benchmark numbers are diagnostic only. Do not commit benchmark outputs, logs, reports or evidence Markdown.

## Library conventions and contracts

- .NET 10 and later (D2). Nullable reference types on, warnings are errors, Central Package Management in `Directory.Packages.props`, shared settings in `Directory.Build.props` (D21).
- Native AOT and trimming safe from day one. Serialization uses System.Text.Json source generation by default (D8).
- The core depends only on `Microsoft.Extensions.*` abstractions and the BCL. A transport package depends only on its native client and supports only its latest major version (D19).
- Public APIs expose interfaces and options; implementations are internal or sealed. Use init-only options and fluent builders following existing code. Options also bind from `IConfiguration` through the Options pattern.
- Messages are identified on the wire by their stable `[Message("name", Version = n)]` name, never the CLR type name, with CloudEvents attributes in native headers (D7).
- All awaits in `src/` use `ConfigureAwait(false)`; tests do not require it. Do not block on tasks with `.Result` or `.Wait()`.
- Telemetry uses one `ActivitySource` and one `Meter`, both named `Relayer`, following the OpenTelemetry messaging semantic conventions (D15, D25): only convention-defined operation types, span names, attributes and metrics, with Relayer additions under the `relayer.` prefix ([design §8](docs/design.md#8-observability)). Logging uses `LoggerMessage` source generation.
- No mutable process-wide statics, so test suites run in parallel in one process (D17).

## Build and test

`global.json` selects the SDK and Microsoft.Testing.Platform (MTP).

```powershell
dotnet build -c Release
dotnet test -c Release
dotnet run -c Release --project benchmarks/Relayer.Benchmarks
```

- Tests use TUnit. Filter with `--treenode-filter "/*/*/ClassName/TestName"` (`/<Assembly>/<Namespace>/<Class>/<Test>` segments; wildcards and `(A|B)` alternation work within a segment). With MTP, pass test options directly, without an extra `--` or VSTest's `--filter`.
- Features require unit tests; transport behavior changes require tests against the real broker or its emulator where one exists, and every transport must pass the transport conformance suite ([design §15](docs/design.md#15-roadmap)).
- Investigate intermittent test failures instead of rerunning CI to obtain green. Use deterministic synchronization for timing-dependent tests.
- Review final changes for reuse, quality, and efficiency before opening a PR (`/simplify` when available). Preserve the performance requirements above.

## Pull requests

- Open every agent-created PR as a draft (`gh pr create --draft`) and follow the [draft review lifecycle](.agents/skills/issue-pr-loop/SKILL.md#draft-review-lifecycle), inside or outside the issue/PR loop: reviewers approve the draft's head before it is marked ready, and build/test CI runs only on ready PRs. Once both reviewers approve the current head, mark it ready and arm auto-merge in one step with `scripts/Enable-AutoMerge.ps1 -Pr <N> -MarkReady` (never a bare `gh pr ready`, never `gh pr merge --auto` directly); GitHub squash-merges that head when the required checks pass. Any later push, including a rebase, disarms auto-merge and needs a new review cycle.
- Every review cycle, check both review threads (`pulls/<N>/reviews`, `pulls/<N>/comments`, GraphQL `reviewThreads`) and PR conversation comments (`issues/<N>/comments`). Reviewers differ: Claude often posts its review as a conversation comment, while other agents use review threads. A finding on either surface needs a disposition before merge.
- Resolve each PR review thread, whether a human or a bot opened it, as soon as you have dispositioned it: the fix is pushed to the PR head and your reply names the commit, or your reply pushes back on the finding with evidence. Leave a thread open only while it has no disposition. If the reviewer replies after your disposition, unresolve the thread and handle the reply.
- Fix review findings that have real-world impact. Reply to and resolve purely theoretical findings with evidence instead of churning the PR.

## Autonomous self-improvement

Treat every problem you notice as work to act on, whether you find it while testing, developing, reading code, reviewing, or doing anything else. Examples include bugs, flaky or failing tests, incorrect docs, misleading comments, broken tooling, and gaps in these instructions.

- If the fix is trivial and low-risk, fix it as part of the current work and mention it in the PR description.
- If the fix is out of scope for the current work, raise a GitHub issue at the point you discover it so it can be fixed later. Include what you saw, where you saw it (file and line, command, or CI run), how to reproduce it, and the expected behavior. Search existing issues first, and add to a matching issue instead of opening a duplicate.
- A problem in a wrapped client (for example Dekaf) belongs in that client's repository: file it there with a minimal reproduction, and link it from the Relayer issue or PR that depends on it.
- Do not ignore a problem because it existed before your change or is unrelated to it.

Keep agent instructions focused on non-obvious project requirements. Prefer links to maintained code, docs or workflows over duplicated tutorials, examples, and inventories.

## Repository map

- `src/Relayer/`: core abstractions, pipeline, telemetry; `src/Relayer.SourceGenerators/`: the source generator bundled into the `Relayer` package.
- `src/Relayer.Kafka/`, `Relayer.AzureServiceBus/`, `Relayer.AmazonSqs/`, `Relayer.RabbitMQ/`: transports.
- `src/Relayer.Outbox.*/`: outbox stores.
- `tests/`: TUnit suites; `benchmarks/Relayer.Benchmarks/`: BenchmarkDotNet suites.
- `docs/`: design and decision record.
- `scripts/` and `.agents/skills/issue-pr-loop/`: issue/PR loop tooling. `scripts/Start-IssuePrLoop.ps1` drives the loop with a fresh agent per unit.
