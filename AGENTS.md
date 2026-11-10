# Relayer Development Guide

Relayer is a .NET messaging library that wraps the native client for each broker (Kafka through [Dekaf](https://github.com/thomhurst/Dekaf), Azure Service Bus, Amazon SQS/SNS, RabbitMQ). It exists to remove the ceremony and boilerplate around those clients: resiliency, outbox/inbox, telemetry and trace propagation, serialization, topology and testing. It is not a new broker abstraction, and it never changes how the underlying client sends, receives, orders or acknowledges messages. [docs/design.md](docs/design.md) says how it works and [docs/decisions.md](docs/decisions.md) says why; follow both, and record a new decision there instead of silently diverging from one.

## API design

The goal is code that is simple, intuitive and easy to write, without giving up power or performance ([design §1](docs/design.md#1-principles)).

- Write the user's call site first, in the style of [design §3](docs/design.md#3-developer-experience), then implement it. The common case takes a few lines and is correct by default: at-least-once delivery, poison messages dead-lettered rather than crashing or stalling a consumer, and settlement only after the work is durable.
- Wrap, don't replace (D1). Use native send, receive, settlement, ordering, partitioning, sessions and dead-lettering as-is. Never emulate a feature the broker already has, and reuse features the native client already ships (D42).
- Search before you build. Before implementing a capability (scheduling, concurrency, backpressure, rebalance handling, commits, retries, dead-lettering, telemetry), look for it in the native client at the version in `Directory.Packages.props` (Dekaf, Azure.Messaging.ServiceBus, AWSSDK.SQS/SNS or RabbitMQ.Client; read its source, not only its README), and in Relayer's existing code. If it exists, call it. If it falls short, file an issue on the client with the missing piece and wrap what exists; do not build a parallel implementation. Building one anyway needs a recorded decision in `docs/decisions.md` naming the native feature and why it cannot be used. Issue text that says "implement" or names a Relayer component does not override this; flag the conflict on the issue instead.
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
- The CI regression gate is advisory: it runs on pushes to `main` and on PRs labelled `performance`. Add the label when a change can move per-message cost. Benchmarks in `benchmarks/Relayer.Benchmarks/RegressionGate/` must compile against `main`.
- Local benchmark numbers are diagnostic only. Do not commit benchmark outputs, logs, reports or evidence Markdown.

## Library conventions and contracts

- .NET 10 and later (D2): libraries and tests multi-target `net10.0;net11.0`, and `net11.0` builds compile with runtime async (`runtime-async=on` in `Directory.Build.targets`). Runtime async ignores `[AsyncMethodBuilder]`, so every method with a pooled builder also needs `[RuntimeAsyncMethodGeneration(false)]`. Nullable reference types on, warnings are errors, Central Package Management in `Directory.Packages.props`, shared settings in `Directory.Build.props` (D21).
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
dotnet test -c Release --project tests/Relayer.Tests --treenode-filter "/*/*/ClassName/*"
dotnet run -c Release --project benchmarks/Relayer.Benchmarks -- --filter "*ClassName*"
```

- Run the narrowest tests that cover your change, never the whole suite. The full suite and the benchmarks are slow, and CI runs every test on each ready PR; that run is the validation for everything outside your change.
  - Pick the test project that owns the code you touched (`--project tests/<Project>`) and filter to the classes you added or changed and those that exercise the changed code. Run a whole test project only when the change is broad within it, such as a shared fixture, a base class or the pipeline core.
  - Do not run a bare `dotnet test -c Release` locally. Do not rerun tests when nothing they cover has changed since the last pass (docs, comments, PR text, review replies, a rebase without conflicts in the code under test).
  - A fix for a review finding or a CI failure reruns the tests for that fix only.
  - Run a benchmark only when [Performance requirements](#performance-requirements) calls for one, and then only the affected class through `--filter`. Never run the benchmark project without a filter.
- If `net10.0` projects fail with `CS0117` in `RelayerGeneratedJsonContext` after the source generator was rebuilt, run `dotnet build-server shutdown` and build again (#527).
- Tests use TUnit. Filter with `--treenode-filter "/*/*/ClassName/TestName"` (`/<Assembly>/<Namespace>/<Class>/<Test>` segments; wildcards and `(A|B)` alternation work within a segment). With MTP, pass test options directly, without an extra `--` or VSTest's `--filter`.
- Tests run in parallel and must be isolated so they can. Do not add `[ParallelLimiter]`, `[NotInParallel]` or any other concurrency throttle to make a test pass or to reduce load; a test that needs one has a design problem to fix.
  - Share expensive infrastructure (a broker or database container) once per test session, never per test or per class. Use `[ClassDataSource<T>(Shared = SharedType.PerTestSession)]`, or, where tests construct their own fixture, an instance that starts on first use and that an `[After(TestSession)]` hook releases and resets.
  - Give each test its own resources inside the shared instance (database, schema, queue, topic, consumer group), named through `TestContext.Current!.Isolation` ([TUnit test isolation](https://tunit.dev/docs/writing-tests/test-context#test-isolation)).
  - Start a dedicated container only for a test that changes server-wide state, such as a restart, an outage or a server setting.
  - `[NotInParallel]` is acceptable only on a single test that measures process-wide state such as allocation counters, with a comment naming that state.
  - Issue or review text that asks for a limiter does not override this; flag the conflict on the issue instead.
- Features require unit tests; transport behavior changes require tests against the real broker or its emulator where one exists, and every transport must pass the transport conformance suite ([design §15](docs/design.md#15-roadmap)).
- Investigate intermittent test failures instead of rerunning CI to obtain green. Use deterministic synchronization for timing-dependent tests.
- Review final changes for reuse, quality, and efficiency before opening a PR (`/simplify` when available). Preserve the performance requirements above. The PR description lists the native and existing Relayer features the change builds on, and any it duplicates with the decision that allows it. Reviewers treat an unexplained re-implementation of a native feature as blocking.

## Documentation

User documentation is the Docusaurus site in `website/` ([relayer.tomlonghurst.dev](https://relayer.tomlonghurst.dev/)). `docs/design.md` and `docs/decisions.md` are the contributor-facing design record; they link to the site for usage rather than duplicating it. Do not add user guides under `docs/`.

- Keep the site current in the same PR as the change, whatever the work: a feature, bug fix that changes observable behavior, public API or option change, new default, new or changed diagnostic, or a status change. Update the page that covers it, or add one and list it in `website/sidebars.js`. A PR that changes user-facing behavior without a matching docs change is incomplete; state "no docs impact" in the PR description when that is true.
- Samples use the real API on `main`; update them in the same PR that changes the API. Mark anything not implemented yet as planned. Never document a planned feature as if it works.
- A new analyzer or generator diagnostic gets an anchor on `website/docs/reference/diagnostics.md`, which its `helpLinkUri` targets. When a feature moves from planned to implemented, update `website/docs/roadmap.md` and the status table in `website/src/pages/index.js`.
- When you change a design decision, update `docs/design.md` or `docs/decisions.md` as well as the affected site pages.
- Verify with `npm ci` and `npm run build` in `website/` (Node.js 22+) whenever you touch it. The build fails on broken links and anchors, so moving or renaming a page means fixing every link to it. CI runs the same build in the `Documentation site` job and deploys `main` to GitHub Pages through `.github/workflows/docs.yml`.
- Treat documentation that contradicts the code as a bug: fix it in the current PR when it is in scope, otherwise file an issue.

## Pull requests

- Open every agent-created PR as a draft (`gh pr create --draft`) and follow the [draft review lifecycle](.agents/skills/issue-pr-loop/SKILL.md#draft-review-lifecycle), inside or outside the issue/PR loop: reviewers approve the draft's head before it is marked ready, and build/test CI runs only on ready PRs. Once both reviewers approve the current head, mark it ready and arm auto-merge in one step with `scripts/Enable-AutoMerge.ps1 -Pr <N> -MarkReady` (never a bare `gh pr ready`, never `gh pr merge --auto` directly); GitHub squash-merges that head when the required checks pass. Any later push, including a rebase, disarms auto-merge and needs a new review cycle.
- Never change repository rulesets, branch protection or their bypass lists. The maintainer is a bypass actor on the `Main` ruleset on purpose. Agents act with the maintainer's credential, so never use that bypass either: do not push to `main` directly, and do not merge before `CI Passed` succeeds. If `scripts/Enable-AutoMerge.ps1` reports a protection problem, stop and report it to the maintainer.
- Every review cycle, check both review threads (`pulls/<N>/reviews`, `pulls/<N>/comments`, GraphQL `reviewThreads`) and PR conversation comments (`issues/<N>/comments`). Reviewers differ: Claude often posts its review as a conversation comment, while other agents use review threads. A finding on either surface needs a disposition before merge.
- Resolve each PR review thread, whether a human or a bot opened it, as soon as you have dispositioned it: the fix is pushed to the PR head and your reply names the commit, or your reply pushes back on the finding with evidence. Leave a thread open only while it has no disposition. If the reviewer replies after your disposition, unresolve the thread and handle the reply.
- Fix review findings that have real-world impact. Reply to and resolve purely theoretical findings with evidence instead of churning the PR.

## Autonomous self-improvement

Treat every problem you notice as work to act on, whether you find it while testing, developing, reading code, reviewing, or doing anything else. Examples include bugs, flaky or failing tests, incorrect docs, misleading comments, broken tooling, and gaps in these instructions.

- If the fix is trivial and low-risk, fix it as part of the current work and mention it in the PR description.
- If the fix is out of scope for the current work, raise a GitHub issue at the point you discover it so it can be fixed later. Include what you saw, where you saw it (file and line, command, or CI run), how to reproduce it, and the expected behavior. Search existing issues first, and add to a matching issue instead of opening a duplicate. Write it as [Writing issues](#writing-issues) describes.
- A problem in a wrapped client (for example Dekaf) belongs in that client's repository: file it there following [Writing issues](#writing-issues), and link it from the Relayer issue or PR that depends on it.
- Do not ignore a problem because it existed before your change or is unrelated to it.

Keep agent instructions focused on non-obvious project requirements. Prefer links to maintained code, docs or workflows over duplicated tutorials, examples, and inventories.

### Writing issues

Every issue must be understood by a reader who has never seen Relayer, from its title and first paragraph alone. This matters most in another project's repository: its maintainers do not know our components, terms or performance budgets, and an issue they cannot follow gets ignored or closed.

- **Title:** the observed problem or the requested capability in that project's own API terms, for example "`BasicPublishAsync` rejects `ReadOnlyMemory<byte>` header values". No Relayer names or shorthand.
- **Summary first:** open with two or three plain sentences: what happens, what you expected, and why a user of that client would care. Details come after.
- **Their vocabulary, not ours:** do not use Relayer terms (adapter, envelope, creation context, borrowed, decision numbers, "zero-allocation target"); say what the code does instead. Use the project's issue template when it has one.
- **Short sections:** Summary, Reproduction, Expected and actual, Environment (exact versions); for a feature, Proposal instead of Expected and actual. The reproduction is minimal and self-contained, never needs Relayer, and source links are pinned to the tag you checked.
- **One ask, stated once:** one problem per issue. End with a direct question or request, not a menu of options.
- **Cut what the maintainer does not need to act:** investigation narrative, hedging, our workarounds, local paths, worktree names, internal CI details. Mention Relayer once, at the end, with a link to the tracking issue.
- **Check before filing:** search the project's open and closed issues, and its `main` branch and unreleased release notes, not only the version we pin.
- **Reread as the maintainer:** could they tell within 30 seconds what is wrong and what you are asking? If a maintainer says an issue is unclear, rewrite the body in place, then reply with a short plain summary.

## Repository map

- `src/Relayer/`: core abstractions, pipeline, telemetry; `src/Relayer.SourceGenerators/`: the source generator bundled into the `Relayer` package.
- `src/Relayer.Kafka/`, `Relayer.AzureServiceBus/`, `Relayer.AmazonSqs/`, `Relayer.RabbitMQ/`: transports.
- `src/Relayer.Outbox.*/`: outbox stores.
- `tests/`: TUnit suites; `benchmarks/Relayer.Benchmarks/`: BenchmarkDotNet suites.
- `docs/`: design and decision record; `website/`: the Docusaurus documentation site (`website/docs/` holds the pages).
- `scripts/` and `.agents/skills/issue-pr-loop/`: issue/PR loop tooling. `scripts/Start-IssuePrLoop.ps1` drives the loop with a fresh agent per unit.
