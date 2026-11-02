---
title: Performance regression gate
description: Run the benchmark gate, interpret allocations, and add raw-client transport comparisons.
---

Relayer's CI compares the current code with a pinned `main` snapshot on the same
runner. The gate covers public direct sends, hosted in-memory round trips,
concurrent asynchronous delivery, receive allocation attribution, and composition
of 100 distinct handlers. Its benchmarks live in
`benchmarks/Relayer.Benchmarks/RegressionGate/`, in the same project as the rest
of the BenchmarkDotNet suite.

## When it runs

The gate is the advisory `Performance` workflow
(`.github/workflows/performance.yml`). It is not part of `CI Passed` and never
blocks a merge. It runs:

- on every push to `main`, comparing with the previous `main` head;
- on demand from the Actions tab (`workflow_dispatch`);
- on ready pull requests labelled `performance`. Add the label when a change can
  move per-message cost; adding it starts a run;
- on ready pull requests that change the gate itself: `RegressionGate/`,
  `Program.cs`, the benchmark project file, the gate scripts or the workflow.

A red `Performance` run on `main` is a regression to investigate, not a reason
to rerun.

The merge and auto-merge scripts also treat this workflow as advisory, including
failed, cancelled and pending runs. They still enforce required CI, reviews and
conflict checks. A failure in another workflow remains blocking even if its job
has the same name as the performance job. Keep the reports and investigate the
measurement; an advisory result does not establish compliance with D18.

## Run locally

Use a Release build with no debugger or telemetry listener attached:

```powershell
dotnet run -c Release --project benchmarks/Relayer.Benchmarks -- --filter '*' --anyCategories RegressionGate --exporters fulljson --artifacts .artifacts/performance/candidate
```

For a quick execution check, add `--job Dry`. Dry results do not measure
steady-state performance. Use the default job for comparisons; `--job Short` is
useful during development but has less statistical confidence.

To compare two revisions, create separate worktrees for the candidate and a
pinned `origin/main` commit. In both worktrees, delete every benchmark source
file except `Program.cs` and `RegressionGate/`. Copy the candidate's
`Program.cs`, `RegressionGate/` and `global.json` into the baseline worktree,
replacing its versions. Each side keeps its own project file. Then run the
command above in each checkout with separate absolute artifact paths.
To match CI's sampling plan, run the baseline once before the candidate and once
after it, using a third artifact directory for the baseline repeat.
Before running the baseline, pin its benchmark driver to the candidate version:

```powershell
python .github/scripts/pin_benchmark_version.py C:/checkouts/candidate C:/checkouts/baseline
```

This changes only the baseline benchmark project's BenchmarkDotNet reference.
Keep the baseline's production source and package versions unchanged. This
measures both implementations with the same workload. Benchmarks outside
`RegressionGate/` never enter the comparison, so a pull request can add a
benchmark for a new API without breaking the baseline build. The source
generator also sees the same messages and handlers on both sides. Code in
`RegressionGate/` must use only API that exists on `main`; if the harness cannot
build against the baseline, update it to support both revisions before claiming
a comparison.

Compare the directories containing the full JSON reports:

```powershell
python .github/scripts/benchmark_gate.py compare C:/benchmark-results/baseline C:/benchmark-results/candidate
python .github/scripts/benchmark_gate.py compare C:/benchmark-results/baseline C:/benchmark-results/candidate --baseline-after C:/benchmark-results/baseline-after
python .github/scripts/benchmark_gate.py self-test C:/benchmark-results/baseline
```

The self-test accepts identical measurements and rejects deliberately injected
throughput and allocation regressions. It tests the detector; it does not prove
that separate executions produce identical timings. The self-test checks relative
regressions independently of the baseline's absolute startup time. The comparison
command still requires the candidate to compose 100 handlers in under 50 ms, so a
slow baseline cannot prevent a candidate that fixes startup from being measured.

## Cases and allocation attribution

| Suite | What is measured |
| --- | --- |
| `EndToEndBenchmarks.DirectSend` | Public `ISender`, routing, default source-generated JSON, headers, timeout, and native in-memory send admission/backpressure. A native receiver drains messages without core receive processing. |
| `EndToEndBenchmarks.SynchronousRoundTrip` | Public send through hosted receive composition, a handler that completes synchronously, and an acknowledgement channel. |
| `EndToEndBenchmarks.ConcurrentRoundTrip` | Eight messages in flight through eight consumer workers. A barrier holds all handlers on pending asynchronous reads before releasing them. Results are normalized per message with `OperationsPerInvoke = 8`. |
| `ConsumePipelineBenchmarks` | Scope-only, scope and handler resolution, direct scoped dispatch, and composed receive, with singleton and scoped handlers. A cached serializer isolates non-serializer allocations. |
| `InMemoryTransportBenchmarks` | Native opaque, native JSON, and composed JSON round trips, in queue and log modes. |
| `InMemoryQueuedBatchBenchmarks` (outside the gate) | A native queue with 64 admitted messages before the first delivery can settle, an extra pooled-callback comparison, and a channel-only coordination control. All results are per **batch**, not per message. |
| `ConcurrentChannelControlBenchmarks` (outside the gate) | Eight channel workers execute the gate's asynchronous handler and entry/release/completion protocol without JSON, DI or a transport. Results are per **item**, normalized with `OperationsPerInvoke = 8`. |
| `StartupBenchmarks.Build100Handlers` | A fresh consume pipeline, dispatch table and context pool for 100 distinct generated handlers. Registration and service-provider setup happen outside the measurement. This measures pipeline composition, not cold process startup or the entire host. |

Every suite uses `[MemoryDiagnoser]`. Receive totals include a fresh DI scope for
each delivery; scopes and scoped handlers are never reused. Driver channels,
native scheduling and JSON costs remain in the end-to-end totals. Report those
totals alongside the isolated attribution cases. Do not subtract a scoped
baseline and describe the difference as total pipeline allocations.

The absolute D18 goal remains **zero allocations per message from Relayer's own
pipeline**, excluding serializer and native-client allocations. Fresh Microsoft
DI scopes currently allocate even for singleton handlers. Matching direct scoped
and composed totals demonstrates zero *incremental composition* allocations;
it does not satisfy the absolute receive allocation goal. The regression gate
prevents increases from the measured baseline without accepting that gap as a
new budget. Async driver scheduling also prevents treating the full round-trip
total as pipeline-only allocation evidence.

### Populated queues and asynchronous coordination

Run the attribution controls alongside the end-to-end fixture:

```powershell
dotnet run -c Release --project benchmarks/Relayer.Benchmarks -- --filter '*InMemoryQueuedBatchBenchmarks*' '*ConcurrentChannelControlBenchmarks*' '*EndToEndBenchmarks.ConcurrentRoundTrip' --keepFiles
```

`QueuedBatchRoundTrip` holds the first native delivery until all 64 messages
have been admitted. Its callback returns the release channel's
`ValueTask<Disposition>` directly. `QueuedBatchWithPooledCallback` adds an
await through `PoolingAsyncValueTaskMethodBuilder`, reproducing the original
fixture's additional callback continuation. `ChannelBatchControl` retains the
same gated 64-item drain and acknowledgement, without native admission, message
copying, message storage or settlement. Divide each batch's reported time and
bytes by **64** for per-message costs. These are distinct workloads; the channel
control is not a raw-client throughput baseline and its total must not be
subtracted as an exact allocation attribution.

Allocation stacks in the original populated-queue fixture identify pooled
state-machine boxes in both the callback and the native read wrapper. Pooled
async methods can still allocate when their runtime cache has no available box.
The native receive and log fan-out loops now await the channel's source directly
and handle shutdown wake-ups after that await, removing the extra native state
machine. The pooled-callback comparison keeps the fixture-owned cost visible
without assigning it to native message storage.

The eight-worker channel control preserves genuinely incomplete handler reads
and the same three signal channels as `ConcurrentRoundTrip`. Its totals include
channel wait operations, the user handler's pooled continuation and worker
scheduling. Earlier end-to-end profiles also contain JSON objects, fresh DI scopes and
service caches, scoped handler instances, native multiple-reader channel waits,
and core receive/consume pooled state machines. [#203](https://github.com/thomhurst/Relayer/issues/203)
attributes the core continuation cache misses. Single-message dispatch now uses
instance-owned continuation storage as described below. Relayer-owned boxes
are never a serializer or native-client exclusion.
The control does not establish zero total core allocations or change the gate's
inclusive measurements and thresholds.

Use allocation stacks to establish ownership rather than inferring it from a
small difference between runs. For example, collect `gc-verbose` events with
`dotnet-trace` while repeatedly running a warmed fixture. Separate setup,
warmup and cleanup from the measured phase. `GCAllocationTick` events sample
allocations: their byte weights are not exact counts or bytes for each sampled
type. Pair the stacks with unprofiled `MemoryDiagnoser` reports; profiling can
change scheduling and its timing is not a throughput comparison.

### Instance-owned receive continuations

Outside the gate, `ConcurrentReceiveBenchmarks` isolates the single-message
continuation path with a cached serializer. Eight handlers remain pending before
an alternating dedicated worker completes them. `DirectScoped`, `DirectConsumer`
and `NativeReceive` all create fresh native DI scopes. The shared fixture returns
exact inclusive allocation counts from both the driver and completion worker;
`MemoryDiagnoser` reports its allocation totals separately. The regression test
uses these counters to compare composition with direct scopes after warmup.
Matching totals establish zero incremental continuation allocations in this case,
not zero total receive allocations. Batch dispatch, executable retry policies,
application handlers and native transports need their own attribution.

## CI thresholds and evidence

Draft PRs never run the gate. PRs use their pinned target commit as baseline; pushes to `main` use its previous
head. Manual runs use the current `main` snapshot. All executions use the
candidate harness and SDK on one runner, in a fixed **baseline, candidate,
baseline** sequence, with separate artifact directories. The second baseline
always runs, regardless of the first measurements. This exposes drift between
executions without retrying failed comparisons until they pass. No timing
baseline is downloaded from another machine.

The gate fails when any case:

- loses more than **5% throughput** relative to the baseline with separated
  BenchmarkDotNet confidence bounds. Since reports contain time per operation,
  the candidate's lower time bound must exceed `baseline upper bound / 0.95`
  for **both** baseline executions before the gate declares a regression.
  A mean beyond the threshold against either baseline without that confirmation
  is reported as inconclusive, not a proven regression. The summary shows both
  baseline intervals; its throughput percentage uses the first baseline.
  Separate process runs can still vary; bracketing does not prove unchanged
  performance or eliminate all runner noise;
- increases **per-message** allocations by more than **0.5 B/op**, a sub-byte allowance for
  amortization and rounding, not an extra allocation budget. The candidate must
  exceed **both** baselines by more than this limit before the gate declares an
  allocation regression. An increase against only the lower baseline is reported
  as **inconclusive**, not proof of unchanged allocations. Concurrent asynchronous
  work includes driver and scheduler allocations that can vary between unchanged
  process runs. Both baseline totals and the candidate total remain visible;
- takes **50 ms or more** to compose 100 handlers.

CI also hashes the actual comparison inputs after overlaying the common harness
and SDK. When the inventories are byte-identical, throughput losses and allocation increases are
reported as **inconclusive execution variation**, even above both baselines:
there is no implementation change to attribute them to. This is not evidence
that either performance goal is met. Confidence intervals describe the sampled
execution, not all future processes: JIT decisions, process layout and host load
can vary between launches. Bracketing alone cannot attribute that variation to
a source change. The inventory includes production source,
generators, the benchmark harness, SDK selection, dependency versions and build
configuration. Only `docs/`, `website/`, the CI workflow, the detector and its
tests, the common benchmark-version pinning script, the repository's agent guidance
(`.agents/skills/issue-pr-loop/references/repository.md`), the merge-only scripts
`Assert-PrGreen.ps1`, `AssertPrGreenChecks.ps1`, `Enable-AutoMerge.ps1` and
`Test-AssertPrGreenChecks.ps1` under `scripts/`, the cleanup-only scripts
`DockerCleanup.ps1`, `Remove-MergedWorktrees.ps1`, `Test-DockerCleanup.ps1` and
`Test-RemoveMergedWorktrees.ps1` and their `WorktreeLifecycle.md` documentation
under `scripts/`, and the two agent instruction aliases
(`.claude/skills/issue-pr-loop/SKILL.md` and
`.claude/skills/issue-pr-loop/references`) are excluded; those do not enter the
measured builds. These aliases are symlinks on Linux and can be plain files on
Windows. Other symlinks still fail verification.

CI also passes `--verify-test-sources`. After the measurements, the detector builds
the benchmark project and every transitive project in Release, then queries their
final MSBuild items and imports. This includes source added by build targets.
Only C# files under `tests/` that none of these projects consume are excluded from
the inventory. Linked test source, resources and analyzer inputs remain hashed;
test project files, configuration, other test assets and unknown files remain hashed
as well. Both project graphs and their input paths are retained in
`performance-inputs.json`. A failed build or incomplete query fails verification.
Custom build tasks must declare their file inputs in MSBuild items; the query does
not trace arbitrary file reads inside a task. Without this option, all test source
remains conservatively hashed.

Every other tracked or unignored file is hashed, including unknown scripts.
Different or unverified inventories retain the strict
5% throughput and 0.5 B/op rules. Startup and invalid-input checks remain active
in both cases. Inconclusive rows keep their measured loss, intervals and totals;
they do not establish unchanged performance or compliance with D18.

To use this check locally, add all three options to the comparison command:

```powershell
python .github/scripts/benchmark_gate.py compare C:/benchmark-results/baseline C:/benchmark-results/candidate --baseline-source C:/checkouts/baseline --candidate-source C:/checkouts/candidate --input-manifest C:/benchmark-results/performance-inputs.json --verify-test-sources
```

Use the separate checkouts that produced the reports, with their harness and SDK
overlay still in place. CI retains `performance-inputs.json` with the raw reports.
Comparisons without source verification keep the strict throughput and allocation rules.

### Investigating changed comparison inputs

When verified inventories differ, the summary lists added, removed and changed
input paths. This is an investigation aid, not a causal assignment or an exemption
from the thresholds. An unchanged benchmark and unchanged call-path source can
still run in a changed containing assembly. Other methods, metadata, generated
code and build inputs can affect the executable; matching source or method IL
alone does not establish matching JIT output or performance.

Preserve the original reports, logs, commit identities and input inventories
before their artifact retention expires. Record file lengths and SHA-256 hashes,
and verify the retained copies. Replay the original detector decision before
changing the harness. Keep a failed changed-input comparison visible even if a
later diagnostic experiment does not reproduce it.

For a controlled investigation, pin the original commits, common harness, SDK,
runtime and dependencies. Declare the case filters, launch count and measurement
settings before running. Run baseline, candidate, candidate, baseline in sequence
on one otherwise idle host to check for ordering effects as well as code differences.
Inspect each process launch, including allocation totals and operation counts in
the raw logs; an aggregate report can hide variation between launches. Automatic
pilot calibration can choose different invocation counts, so use a further
fixed-count experiment when that difference could explain the signal.

Retain the effective compile inputs, binaries, probe sources and commands with
the reports outside disposable worktrees. Compare the relevant assembly hashes
and method IL as well as source inputs. Variation between launches of one frozen
build demonstrates execution variation in that experiment. It does not identify
the cause of a historical run, rule out a change-dependent effect on another host,
or prove the raw-client 5% or zero-allocation budgets. Use allocation stacks and
appropriate controls before assigning bytes to a particular component. Document
remaining uncertainty rather than changing thresholds to make a signal pass.

Startup allocation totals remain visible but are informational. D18 sets an
allocation budget per message and a time budget for startup. Building frozen
dispatch tables can allocate different bucket layouts across processes because
message identities use randomized hashes. The same unchanged 100-handler
composition can therefore report different allocation totals. This does not
relax the per-message allocation threshold or the startup time budget.

Missing, duplicated, invalid, failed or mismatched benchmark cases fail closed,
including an incomplete or missing requested baseline repeat. Local comparisons
without `--baseline-after` retain the single-baseline rule.
The CI detector self-test shows that unchanged measurements pass and injected
regressions fail. The comparison prints mean timings as throughput changes and
allocation totals in the job summary. When a benchmark run fails, the job
prints the end of its log. The job uploads raw reports, logs, comparison
commits, input inventories and harness hashes for investigation. Inspect
BenchmarkDotNet error and variation columns when timings are noisy; do not rerun until a noisy
failure happens to pass. Local numbers are diagnostic only. Do not commit
benchmark reports, logs or evidence Markdown.

## Raw native-client comparisons

The regression threshold against `main` is distinct from D18's **at most 5%
throughput overhead against the raw native client**. In-memory and callback
microbenchmarks do not establish that real-broker budget. `KafkaSendBenchmarks`
already pairs raw Dekaf and Relayer adapter sends against a real broker for
pre-serialized single messages and native batches. These cases are outside the
`RegressionGate` category and do not cover the public default-JSON send pipeline
or receive path. Complete default-JSON send and receive comparisons for real
transports remain planned.

Each adapter must add paired raw-client and Relayer cases in the same benchmark
class, using `[MemoryDiagnoser]`, with the raw case marked
`[Benchmark(Baseline = true)]`. Use the same broker, native client version,
connection settings, payload, default generated JSON serializer, headers, batch
size, concurrency, ordering, settlement durability and telemetry state in both
cases. Measure receive and direct send separately, without an outbox. Await
durable settlement on receive and the same native send acknowledgement on send;
do not substitute queue admission for a broker acknowledgement.

Include synchronous completion where the native API permits it and genuinely
asynchronous concurrent delivery. Bound outstanding work and validate message
counts and successful settlement before cleanup. Attribute serializer and native
client allocations explicitly, keep fresh DI scope costs in the Relayer total,
and report the native/composed delta separately. Compare operations per second:
Relayer throughput must be at least `0.95 * raw throughput`. Add these cases to
the CI gate with their adapter and retain raw results for both paths. Never use
an in-memory result as a substitute for any of Kafka, Azure Service Bus, Amazon
SQS/SNS or RabbitMQ.
