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
| `ConsumePipelineBenchmarks` | Scope-only, scope and handler resolution, direct scoped dispatch, and explicit-failure-delegate receive, with singleton and scoped handlers. A cached serializer isolates non-serializer allocations. |
| `FailureConsumePipelineBenchmarks` | Direct scoped dispatch, explicit-failure-delegate receive, and the executable `FailurePipeline` used by broker transports. Both pipelines attach tracing and telemetry with no listeners. A scoped handler either completes immediately or returns a pending reusable source, completed by the driver only after dispatch suspends. Cached serialization and identical DI lifetimes expose incremental composition costs. |
| `InMemoryTransportBenchmarks` | Native opaque, native JSON, and composed JSON round trips, in queue and log modes. |
| `InMemoryQueuedBatchBenchmarks` (outside the gate) | A native queue with 64 admitted messages before the first delivery can settle, an extra pooled-callback comparison, and a channel-only coordination control. All results are per **batch**, not per message. |
| `ConcurrentChannelControlBenchmarks` (outside the gate) | Eight channel workers execute the gate's asynchronous handler and entry/release/completion protocol without JSON, DI or a transport. Results are per **item**, normalized with `OperationsPerInvoke = 8`. |
| `StartupBenchmarks.Build100Handlers` | A fresh consume pipeline, dispatch table and context pool for 100 distinct generated handlers. Registration and service-provider setup happen outside the measurement. This measures pipeline composition, not cold process startup or the entire host. |

The concurrent fixture publishes each handler's barrier entry only after registering
its release continuation. Checking that a read is incomplete and then publishing
entry before awaiting it would let the driver release that handler before it
suspends. The barrier fixes that ordering; it does not make channel, pooled
async-state-machine or scheduler allocation totals deterministic.

Every suite uses `[MemoryDiagnoser]`. Receive totals include a fresh DI scope for
each delivery unless a failure-pipeline consumer has only singleton handlers and
nothing reads `MessageContext.Services`; scopes and scoped handlers are never
reused. Driver channels,
native scheduling and JSON costs remain in the end-to-end totals. Report those
totals alongside the isolated attribution cases. Do not subtract a scoped
baseline and describe the difference as total pipeline allocations.

The absolute D18 goal remains **zero allocations per message from Relayer's own
pipeline**, excluding serializer and native-client allocations. Fresh Microsoft
DI scopes still allocate for scoped handlers, and for singleton handlers on the
explicit failure-delegate path. Matching direct scoped
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

The gated `FailureConsumePipelineBenchmarks` adds executable failure handling and
scoped handlers to that coverage. Its completion source resumes on the driver
thread, so its allocation results do not establish reuse across worker changes.
Compare `ProductionFailurePipeline` with `ExplicitFailureDelegate` to assess the
two consume implementations, and with `DirectScopedDispatch` to attribute the DI
scope and handler costs excluded by D52. These are composition comparisons;
the broker-backed Kafka comparison measures receive against the native client.
Neither a relative regression pass nor a single local timing result proves the
5% native-client budget.

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

The same verified project graphs also distinguish three repository policy files
from build inputs: `renovate.json`, `.github/workflows/claude-code-review.yml`
and `.github/scripts/test_dependency_review_policy.py`. These files are excluded
only when none of the measured projects consumes them as an MSBuild item or import.
An explicit resource, content or analyzer reference keeps the file hashed, including
an ignored file; a missing referenced file fails verification. Without
`--verify-test-sources`, these policy files remain hashed. Unknown configuration,
scripts and workflows remain hashed even with verification enabled.

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

The separate `Frozen full-delivery driver allocation control` workflow retains
an exact-driver summary for each historical input set. To validate downloaded
records and regenerate a summary without launching another experiment, run:

```powershell
python .github/scripts/summarize_frozen_driver.py C:/retained/results/42306276036-exact-driver
```

The summary requires all four declared ABBA launches, matching runtime and
architecture, fixed operation counts, and exact histogram count/byte sums.
It reports inclusive bytes and calling-thread workload-invocation and BDN-waiter
bytes separately. These regions exclude work on other threads; they do not
identify object types or attribute a historical allocation increase. Keep
unfavorable launches visible and never subtract separate experiments to assign
an owner. Missing or inconsistent records fail validation rather than producing
a partial comparison. This is diagnostic validation, not the regression gate.

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

For a retained changed-input run, replay the strict report comparison without
rebuilding the source checkouts:

```powershell
python .github/scripts/benchmark_gate.py compare C:/retained/performance-results/baseline C:/retained/performance-results/candidate --baseline-after C:/retained/performance-results/baseline-after --summary C:/investigation/strict-replay.md
```

Check the retained input inventories separately to confirm that the original run
had differing inputs. `--input-manifest` writes a new inventory from source
checkouts; it does not read an archived inventory. Do not point it at the original
evidence. Without source verification, the comparison applies the strict rules
even to an originally identical-input run, so this command does not reproduce
that run's verified-identical classification.

For a controlled investigation, pin the original commits, common harness, SDK,
runtime and dependencies. Declare the case filters, launch count and measurement
settings before running. Run baseline, candidate, candidate, baseline in sequence
on one otherwise idle host to check for ordering effects as well as code differences.
Inspect each process launch, including allocation totals and operation counts in
the raw logs; an aggregate report can hide variation between launches. Automatic
pilot calibration can choose different invocation counts, so use a further
fixed-count experiment when that difference could explain the signal.

CI runs BenchmarkDotNet with `--keepFiles` and retains every generated build
directory from both checkouts, including both baseline executions. The
`performance-builds/` artifact directory contains generated drivers, projects,
executables and dependencies, original source ZIPs, and the effective common
harness/SDK overlays. Each checkout has an `inventory-sha256.json` with its
commit, original file locations, lengths and verified SHA-256 hashes. To recreate
the source overlay, extract `source.zip`, remove benchmark C# files outside
`Program.cs` and `RegressionGate/`, replace `RegressionGate/`, then apply
`overlay/`. Generated binaries are evidence of those executions, not a portable
benchmark baseline. A failed build can leave an incomplete directory; verify
the corresponding execution log before claiming a successful measurement.

For a frozen-image control, locate the generated driver's DLL and benchmark ID
in the original log's `Execute:` line. Verify the retained inventory, then run
the DLL from its retained output directory with the recorded runtime. For example,
if that log identifies benchmark ID `1` as the case being investigated:

```powershell
dotnet ./Relayer.Benchmarks-DefaultJob-1.dll --benchmarkId 1
```

Check the generated `.notcs` source to confirm the ID's method and parameters.
Check this mapping in both images. With multiple failing cases, run the declared
sequence separately for each case and retain its own launch results. Do not pool
different methods or parameter combinations into one comparison.
The generated driver supports this direct execution without the original parent
process's anonymous pipes. Its job settings are compiled into the driver;
benchmark CLI options such as `--filter` or `--invocationCount` do not configure
this executable. Keep the complete dependency directory unchanged and verify
its hashes after the experiment. Capture stdout, stderr, exit status and runtime
identity for every launch. The raw `WorkloadResult` and `// GC:` lines retain
timings, operation counts and allocation counters; this direct execution does
not produce the parent runner's full JSON report.

Run the predeclared sequence with separate logs for every launch. An adaptive
pilot can still choose different operation counts for frozen images. A further
fixed-count control requires a separately configured driver or rebuilt benchmark;
retain that control's inputs and binaries separately and identify it as a new
experiment, not a replay of the original executable. Replaying on another host
tests variation there; it does not reproduce the historical host's conditions.

The frozen concurrent workflow also runs a separate fixed-count ABBA with
`frozen_concurrent_control.py --bdn-await`. It binds the retained
BenchmarkDotNet `AwaitHelper.GetResult<int>(ValueTask<int>)` once and completes
each original `ConcurrentRoundTrip` synchronously, as the generated BDN driver
does. The default external probe instead awaits that method asynchronously;
these completion boundaries can change scheduling and continuation-cache reuse.
Both modes keep 4096 warmup and 32768 measured batches, original binaries and
inclusive allocation counters. Their results are separate experiments: the BDN
waiter mode does not replay adaptive pilot calibration, identify historical
allocation owners, or permit subtracting one mode's totals from the other.
The protocol and each launch record name the completion boundary. Fixture,
synchronous round-trip and exact-driver counters cannot be combined with the
full-delivery BDN waiter mode.

The artifact retention is **90 days**. Copy evidence to an independent archive
before that deadline when an investigation must outlive it. Local PR evidence
can be deleted when the PR closes, so it is not a permanent archive. Historical
runs that uploaded only reports cannot recover their exact loaded binaries.

Retain the effective compile inputs, binaries, probe sources and commands with
the reports outside disposable worktrees. Compare the relevant assembly hashes
and method IL as well as source inputs. Variation between launches of one frozen
build demonstrates execution variation in that experiment. It does not identify
the cause of a historical run, rule out a change-dependent effect on another host,
or prove the raw-client 5% or zero-allocation budgets. Use allocation stacks and
appropriate controls before assigning bytes to a particular component. Document
remaining uncertainty rather than changing thresholds to make a signal pass.

The **Frozen original BenchmarkDotNet engine control** workflow accepts
`benchmark_case=composed-consumer-unscoped` with `parent=native`,
`performance_run=43456694323` and `runtime_version=10.0.15` through
`workflow_dispatch`. Use `same_image=abba` for a separately predeclared
baseline/candidate/candidate/baseline sequence. This reuses the original generated
runner, default job, native pipe parent and exact runtime selection. All three
original build inventories are checked before and after execution; the final A
slot repeats the first baseline image. Retain every launch log, native receipt,
operation count, timing, allocation observation and failure with the executable
inputs. These new executions cannot identify the historical cause or establish
throughput equivalence; host drift and adaptive operation counts remain
limitations. This does not rerun or relax the strict regression gate.

For a separately declared code-generation question, use the same workflow.
Its optional `jit_method` captures native JIT listings in the child only; `*`
retains all emitted methods, including async state machines. Retain all four
outcomes and listings. Exclude instrumented timings and allocations from
performance comparisons. Listings cannot reconstruct historical code, prove
which tier executed during measurements, or establish completeness for inlined,
precompiled or interleaved methods.

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
or receive path.

`KafkaReceiveBenchmarks` already compares raw Dekaf with the production serial
Kafka receive adapter, including the core receive pipeline and generated handler
dispatch. Both paths use the same source-generated JSON contract and commit native
fetch-batch checkpoints. See [Kafka performance validation](../transports/kafka.md#performance-validation)
for its maintained scope, broker configuration and invocation. These results are
local diagnostics, not a claim that the 5% receive budget has passed; the planned
parallel scheduler is outside this comparison.

For Kafka, complete default-JSON send-pipeline comparisons and parallel receive
coverage remain planned. Other transports already have paired cases, including
RabbitMQ and Service Bus send and receive benchmarks and SQS send benchmarks.
`SqsReceiveBindingBenchmarks` isolates body preparation without a broker; it is
not a real-broker receive comparison. Existing cases do not establish complete
default-JSON pipeline coverage or prove the 5% budget for those transports.

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

### Kafka delivery header snapshots

Kafka deliveries retain one native Dekaf header view for all envelope lookups and
failure copies. The readonly delivery value and its copies share that view;
replacing `Result` through an initializer or `with` expression obtains a view for
the replacement record. Equality and deconstruction still use only `Result` and
`PartitionId`. Native access through `context.Kafka().ConsumeResult` returns the
original record. Reading its `Headers` property separately still uses Dekaf's
native snapshot behavior.

Dekaf 1.27.0's `ConsumeResult.Headers` creates a lazy view for each property read.
Reading an element materializes safe owned header/value arrays. Retaining that
view removes three redundant snapshots from the normal four-lookups envelope
path (`ce_type`, `ce_id`, attempts and redeliveries). It reuses Dekaf's storage
lifetime checks and copying rather than introducing a second header cache or
borrowing pooled values past their lifetime. The receive pipeline clears the
delivery reference on completion, including failure and cancellation. Deliveries
remain borrowed values valid only during the receive callback.

`KafkaDeliveryHeaderBenchmarks` holds a real Kafka fetch batch and compares no
header access, one native lookup, four independent native lookups, one shared
native view and the Relayer binding. The Relayer case constructs a fresh delivery
inside every measured invocation, so construction and materialization are counted;
it does not reuse a snapshot warmed during setup. All four-lookup cases read the
same envelope and return the same result. Run alongside the production failure
composition in `KafkaReceiveBenchmarks`:

```sh
RELAYER_KAFKA_BOOTSTRAP_SERVERS=localhost:19092 dotnet run -c Release --project benchmarks/Relayer.Benchmarks -- --filter '*KafkaDeliveryHeaderBenchmarks*' '*KafkaReceiveBenchmarks*' --job short --exporters fulljson
```

A local ShortRun on macOS ARM64 (Apple M5 Pro, .NET 10.0.14, SDK 11.0.100,
Kafka 4.0.2, Dekaf 1.27.0) measured the following diagnostic controls:

| Header control | Allocated per invocation | Mean |
| --- | ---: | ---: |
| No header access | 0 B | Below timing resolution |
| One native lookup | 216 B | 22.28 ns |
| Four independent native lookups | 864 B | 91.63 ns |
| Four lookups sharing one native view | 216 B | 29.28 ns |
| Four Relayer lookups, including fresh delivery construction | 216 B | 28.71 ns |

The full production-failure receive comparison against base `c3140a1` measured
1.88/1.90 KB per Relayer message before this change and 1.27/1.27 KB after it
(health disabled/enabled). Raw Dekaf measured 1.04 KB per message in both runs.
Relayer means were 48.98/48.65 microseconds before and 51.54/48.76 microseconds
after. These three-iteration runs are noisy: the baseline raw/health-disabled
mean was 119.63 microseconds with 50.60 microseconds standard deviation, versus
49.44 microseconds in the candidate run. They demonstrate reduced allocations
without establishing a throughput improvement, regression or the 5% budget.
No unfavorable timing is discarded. The allocation controls attribute the
removed repeated snapshots independently of these end-to-end totals.

The remaining required snapshot is native-client cost under D52; redundant
snapshots caused by repeated Relayer property reads are avoidable. These controls
exclude fetching, JSON, scopes and settlement, so their timings cannot establish
the 5% end-to-end receive budget. The raw receive comparison reads no headers;
its difference also includes unequal native header work. ShortRun timings are
diagnostic and do not establish budget compliance.

### Kafka header allocation attribution

`KafkaHeaderAllocationBenchmarks` retains the attribution controls introduced in
[#673](https://github.com/thomhurst/Relayer/pull/673). Its binding case now uses
the shared delivery view described above. Use `KafkaDeliveryHeaderBenchmarks`
for fresh-delivery construction cost; a delivery retained during setup can reuse
an already materialized snapshot.

To collect separate allocation profiles of the complete receive comparison, set
`RELAYER_KAFKA_ALLOCATION_PROFILE=1` and filter `*KafkaReceiveBenchmarks*`.
This enables BenchmarkDotNet's GC-verbose EventPipe diagnostic execution in
addition to its ordinary MemoryDiagnoser execution. Whole-process profiles include
setup, warmup and cleanup, so use their stacks to locate owners, not their sample
weights as exact measured bytes. Keep the raw/native and composed reports and
profiled timings separate. A borrowed native lookup API is requested in
[Dekaf #4120](https://github.com/thomhurst/Dekaf/issues/4120); existing snapshots
must continue owning their bytes after the fetch is released.

### Frozen concurrent investigation

`Frozen concurrent allocation investigation` runs a separate diagnostic on native Ubuntu x64 against the retained executables from runs 42306276036, 42312305889 and 42310409459. It verifies every retained build inventory before executing an external cached-delegate probe. Each run uses separate baseline/candidate/candidate/baseline processes, 4096 warmup batches and exactly 32768 measured batches of eight messages. All outcomes, runtime identity, probe and original inputs are uploaded for 90 days, including failed launches. The workflow runs when its tooling changes in a pull request and can also be dispatched deliberately. It requires the original artifacts to remain available.

The process-wide allocation counter includes fixture, serializer, dependency-injection and transport work. These controls can reveal repeatable image-dependent behavior or variation within one frozen image; they do not identify an allocation owner, replay the original BenchmarkDotNet driver, establish throughput equivalence, or demonstrate the native-client performance budget. An unfavorable result remains evidence to investigate. This workflow does not change the strict regression gate or rerun it for green.

The investigation also records a **separate instrumented ABBA** with `dotnet-trace` GC allocation stacks. The external probe emits `Relayer-FrozenMeasurement` start/stop events outside its inclusive counter. `FrozenAllocationReader` requires one ordered pair in one process and retains only that process's allocation ticks between the markers, with complete stacks, per-process thread IDs, sample weights, excluded-sample counts and missing-stack counts. Identical stacks on different threads remain separate. The measurement record includes the start and stop marker thread IDs; these identify marker emitters, not stable workload roles. Async continuations can move threads, and thread IDs cannot be compared between launches. Setup, warmup and cleanup samples are excluded. Successful extraction removes the raw trace containers; failed extraction preserves them for diagnosis. The original executables remain unchanged.

These are weighted allocation samples, not exact object counts. A tick can include bytes from before a measurement boundary, and instrumentation changes scheduling. The separate profiled results must not be mixed into the untraced ABBA, subtracted as exact per-component bytes, or used alone to assign the historical excess. Use the stacks to choose a component-specific controlled experiment before proposing a production fix. Missing markers or an empty measured sample set fail visibly.

### Exact full-delivery driver control

`Frozen full-delivery driver allocation control` runs a separate instrumented
ABBA against each retained input set using `tools/FrozenDriverProbe`. It keeps
DI, JSON, native in-memory transport and the core receive pipeline in the original
full-delivery workload. Two preallocated recorders count calling-thread bytes
around the synchronous `ConcurrentRoundTrip()` invocation and the original
BenchmarkDotNet `AwaitHelper.GetResult` consumption separately. The helper is
bound once from each retained dependency directory; historical images are never
rebuilt. Each process uses 4096 warmup and 32768 measured eight-message batches,
validates the delivery sum and both record counts, and reports exact histograms.

Neither counter spans an await. A blocked waiter includes any inline continuation
work on its calling thread, but excludes allocation on other threads. These
regional byte counts do not identify object types or the historical excess.
Recording changes scheduling: compare within this separate ABBA and never
subtract it from an uninstrumented or isolated-fixture experiment. The workflow
verifies every image inventory before launching, fails visibly on missing inputs,
and retains all outcomes, original inputs and probe binaries for 90 days.

### Frozen handler fixture control

`tools/FrozenFixtureProbe` loads the retained executable's original `ConcurrentHandler`
and signal channels, binds a typed delegate once, and measures 4096 warmup plus
32768 eight-handler batches. It deliberately excludes DI, JSON, transport and the
Relayer pipeline. Every batch waits for all original handler ValueTasks after the
completion signals, so pooled handler state cannot spill into the next batch.

The `Frozen handler fixture allocation control` workflow verifies all retained
build inventories before a separate-process ABBA for each historical input set.
It preserves all outcomes and executable inputs for 90 days. This is a new
owner-isolating experiment, not a rerun of the regression gate. It measures the
original fixture, native scheduler and diagnostic driver together; its scheduling
differs from full delivery. Variation here cannot identify the historical excess,
and its totals must never be subtracted from end-to-end totals. The strict gate
and production performance requirements remain unchanged.

The fixture workflow also collects a **separate instrumented ABBA** using the same
`Relayer-FrozenMeasurement` phase events and `FrozenAllocationReader` as the full
delivery control. Markers remain outside the inclusive allocation counter; the
reader excludes setup and warmup. Retained measured-region stacks can distinguish
original handler, signal-channel and diagnostic-driver allocations within this
isolated workload. Weighted samples and missing-stack counts retain their existing
limits: these profiles cannot attribute the historical end-to-end excess, and
neither sample weights nor isolated totals may be subtracted from full delivery.
Untraced and instrumented results remain separate. Failed extraction preserves
raw captures, and every ABBA launch is attempted even after another launch fails.

The fixture workflow additionally runs a **separate exact invocation ABBA** using
`--exact-invocations`. Two `GC.GetAllocatedBytesForCurrentThread` reads enclose only
the synchronous call to the original cached handler delegate. There is no await
between the reads. A preallocated buffer records one exact byte count per measured
invocation; warmup calls are excluded. After measurement the result includes the
number of calls, their total bytes and the complete frequency histogram of bytes
per call. Counts include the original handler's synchronous channel/runtime calls,
including suspension setup, but exclude later continuations, other threads and
the driver's own work. They distinguish this synchronous region from the earlier
inclusive totals without relying on allocation-tick samples.

This mode changes scheduling and runs separately from both uninstrumented and
sampled controls. Its inclusive process total is an instrumented result too.
Equal byte counts do not prove equal allocated types; use sampled call sites to
choose further controls, rather than assigning a histogram bin to a type by size.
These measurements neither reconstruct the original BenchmarkDotNet execution
nor identify its historical excess by themselves. Do not subtract them from
full-delivery totals or mix them into the uninstrumented comparison.

Run 42310409459 is a separate changed-input comparison tracked in #357: its
original concurrent round trip reports 699/700 baseline and 701 candidate
B/message. Both full-delivery and isolated-fixture workflows include those exact
retained images. Results from the other historical input sets cannot establish
attribution for this run. Local ARM64 controls validate the tools; hosted Ubuntu
x64 controls retain the original platform family. Neither reproduces the original
BenchmarkDotNet execution or retrospectively identifies its allocation delta.

The first native x64 controls for #357 completed in full-delivery run
[42745408162](https://github.com/thomhurst/Relayer/actions/runs/42745408162)
and fixture run
[42745408270](https://github.com/thomhurst/Relayer/actions/runs/42745408270),
using .NET 10.0.14 and 262144 measured messages per launch:

| Untraced B/message | Baseline A1 | Candidate B1 | Candidate B2 | Baseline A2 |
|---|---:|---:|---:|---:|
| Full delivery | 700.303986 | 701.582031 | 700.116425 | 700.905823 |
| Isolated fixture | 193.379059 | 193.562042 | 193.432770 | 193.423615 |

One full-delivery candidate exceeds both baselines while the other falls below
both. Both isolated-fixture candidates exceed their baseline bracket. These
unfavorable observations remain unresolved; neither comparison establishes the
historical allocation owner. Separate measured-region profiles have no missing
stacks and show original handler channel reads and pooled handler state-machine
rents in the isolated fixture. Full delivery also samples DI scope dictionaries
and in-memory receiver channel reads. Sample weights do not assign exact bytes
to any of these owners. The exact synchronous-invocation ABBA now includes this
input set to measure the original handler call region separately; its results
must remain distinct from these untraced totals and sampled profiles.

The fixture workflow also runs a separate `--exact-releases` ABBA. Each record
counts calling-thread bytes around the entire synchronous batch of eight
`Release.Writer.TryWrite` calls. The native channel completes waiting readers
and schedules their continuations during these calls, so this tests a region
excluded by the handler-invocation counter. There are exactly 32768 measured
records, each representing eight writes; warmup is excluded. The JSON
`invocationAllocations` histogram counts release-batch delegate calls in this
mode, **not messages**. No await occurs inside the counter boundary. Other
threads and later continuations remain excluded, and byte counts do not identify
object types. This instrumented comparison neither attributes the historical
excess nor permits subtraction from the other controls' totals.

The separate native exact-invocation ABBA for #357 completed in
[run 42748223325](https://github.com/thomhurst/Relayer/actions/runs/42748223325).
All four histograms contain exactly 262144 measured calls, with verified weighted
byte sums. Synchronous handler-call bytes per invocation were
192.003052 / 192.000122 / 192.000122 / 192.004517 (A/B/B/A). Both
candidates fall below both baselines in this instrumented region. This does not
explain the earlier unfavorable untraced fixture comparison or identify historical
end-to-end allocation owners. Histogram sizes do not identify allocated types.
The separate exact release-batch ABBA also includes this input set, testing
calling-thread channel completion scheduling outside the handler-call boundary.
Its measurements must remain separate from handler-call and inclusive totals.


For #357, the full-delivery workflow also runs a **separate exact round-trip-call
ABBA** (`--exact-roundtrips`). It reuses the exact recorder around one synchronous
call to the frozen original `ConcurrentRoundTrip` delegate, before awaiting the
returned `ValueTask<int>`. Each of 32768 records represents an eight-message call;
4096 warmup calls are excluded. This covers synchronous full-delivery work that
the isolated handler and release controls do not execute, including any send and
pipeline work reached before the first suspension. Two calling-thread allocation
counter reads enclose only that delegate call; the probe awaits its result after
the second read. Histograms are processed after measurement.

This boundary excludes other threads and subsequent continuations, even if they
complete before the delegate returns. Scheduling depends on instrumentation; its
inclusive process totals and exact counts belong to this separate experiment.
Neither subtraction from isolated or inclusive controls nor histogram-based type
inference identifies the historical owner. Earlier unfavorable measurements
remain unresolved. Native exact release-batch run
[42751736039](https://github.com/thomhurst/Relayer/actions/runs/42751736039) reported
0/0/0/0 calling-thread bytes per eight-write record for the original #357 inputs;
this excludes excess only in that instrumented release region. PR #409 has merged
and its required CI run 42752233964 passed. Historical attribution remains open.

Native exact synchronous round-trip run
[42754519395](https://github.com/thomhurst/Relayer/actions/runs/42754519395)
reported 96 / 96.05078125 / 96.0029296875 / 96.02490234375 calling-thread
bytes per eight-message call (A/B/B/A). Each histogram has 32768 records with
verified weighted sums. One candidate exceeds both baselines; the other lies
between them. This does not identify an allocation type or settle the original
end-to-end comparison. #418 merged and its required CI run 42754917916 passed.

### Original BenchmarkDotNet engine control for #357

The **Frozen original BenchmarkDotNet engine control** workflow uses
`.github/scripts/frozen_bdn_control.py` to run a separate A/B/B/A against the
retained generated BenchmarkDotNet executable from run 42310409459. Unlike the
external fixed-count probes, this executes the original generated runner,
`AwaitHelper`, Default job, pilot, warmup, workload and extra GC-statistics pass.
No workload or generated executable is rebuilt; no iteration counts, allocation
thresholds or historical reports change. All three input inventories verify
before launch and again afterwards. A2 deliberately repeats the first baseline
image, as in the earlier controls.

The launcher discovers the `ConcurrentRoundTrip` runner from the inventoried
generated source and requires eight operations per invocation and enabled GC
statistics. It uses BenchmarkDotNet's existing `NoAcknowledgementConsoleHost`
entry point instead of the original parent's anonymous pipes. The workflow runs
on Ubuntu x64 with SDK 10.0.403; the generated x64 executable cannot run under an
ARM64 .NET process. Every process log and unsuccessful outcome is retained;
failed launches are not retried or presented as measurements.

`outcomes.json` records the native `// GC:` bytes and operation count, their
unrounded ratio and BenchmarkDotNet's nearest-even rounded B/message value.
The operation count already includes the eight messages per invocation; it is
not divided by eight again. The parser rejects missing, ambiguous or incomplete
GC results. The native format and rounding come from
[BenchmarkDotNet 0.15.8 GcStats](https://github.com/dotnet/BenchmarkDotNet/blob/v0.15.8/src/BenchmarkDotNet/Engines/GcStats.cs).

These are new original-engine executions on another host, with the console host
replacing the parent pipe protocol. They do not reconstruct the historical
execution, identify object types or prove the raw-native 5% budget. Native job
calibration can choose different operation counts for each launch; the raw logs
preserve those choices. Keep these results separate from external-driver totals,
regional exact counters and sampled profiles. Unfavorable historical and control
measurements remain open until an allocation owner or execution-dependent cause
is demonstrated.
