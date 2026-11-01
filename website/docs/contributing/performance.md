---
title: Performance regression gate
description: Run the benchmark gate, interpret allocations, and add raw-client transport comparisons.
---

Relayer's CI compares the current code with a pinned `main` snapshot on the same
runner. The gate covers public direct sends, hosted in-memory round trips,
concurrent asynchronous delivery, receive allocation attribution, and composition
of 100 distinct handlers. It reuses the existing BenchmarkDotNet suite.

## Run locally

Use a Release build with no debugger or telemetry listener attached:

```powershell
dotnet run -c Release --project benchmarks/Relayer.Benchmarks -- --filter '*' --anyCategories RegressionGate --exporters fulljson --artifacts .artifacts/performance/candidate
```

For a quick execution check, add `--job Dry`. Dry results do not measure
steady-state performance. Use the default job for comparisons; `--job Short` is
useful during development but has less statistical confidence.

To compare two revisions, create separate worktrees for the candidate and a
pinned `origin/main` commit. Copy the candidate's `benchmarks/` directory and
`global.json` into the baseline worktree, replacing its benchmark harness, then
run the command above in each checkout with separate absolute artifact paths.
Before running the baseline, pin its benchmark driver to the candidate version:

```powershell
python .github/scripts/pin_benchmark_version.py C:/checkouts/candidate C:/checkouts/baseline
```

This changes only the copied benchmark project's BenchmarkDotNet reference.
Keep the baseline's production source and package versions unchanged. This
measures both implementations with the same workload, even when the candidate
introduces a benchmark. If the harness cannot build against the baseline API,
update the harness to support both revisions before claiming a comparison.

Compare the directories containing the full JSON reports:

```powershell
python .github/scripts/benchmark_gate.py compare C:/benchmark-results/baseline C:/benchmark-results/candidate
python .github/scripts/benchmark_gate.py self-test C:/benchmark-results/baseline
```

The self-test accepts identical measurements and rejects deliberately injected
throughput and allocation regressions. It tests the detector; it does not prove
that separate executions produce identical timings.

## Cases and allocation attribution

| Suite | What is measured |
| --- | --- |
| `EndToEndBenchmarks.DirectSend` | Public `ISender`, routing, default source-generated JSON, headers, timeout, and native in-memory send admission/backpressure. A native receiver drains messages without core receive processing. |
| `EndToEndBenchmarks.SynchronousRoundTrip` | Public send through hosted receive composition, a handler that completes synchronously, and an acknowledgement channel. |
| `EndToEndBenchmarks.ConcurrentRoundTrip` | Eight messages in flight through eight consumer workers. A barrier holds all handlers on pending asynchronous reads before releasing them. Results are normalized per message with `OperationsPerInvoke = 8`. |
| `ConsumePipelineBenchmarks` | Scope-only, scope and handler resolution, direct scoped dispatch, and composed receive, with singleton and scoped handlers. A cached serializer isolates non-serializer allocations. |
| `InMemoryTransportBenchmarks` | Native opaque, native JSON, and composed JSON round trips, in queue and log modes. |
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

## CI thresholds and evidence

The `Performance regression gate` job feeds the required `CI Passed` check.
Draft PRs skip CI; the job runs after draft review, when the PR becomes ready.
PRs use their pinned target commit as baseline; pushes to `main` use its previous
head. Manual runs use the current `main` snapshot. Both executions use the
candidate harness and SDK on one runner, sequentially, with no downloaded timing
baseline from another machine.

The gate fails when any case:

- loses more than **5% throughput** relative to the baseline with separated
  BenchmarkDotNet confidence bounds. Since reports contain time per operation,
  the candidate's lower time bound must exceed `baseline upper bound / 0.95`
  before the gate declares a regression. A mean beyond the threshold with
  overlapping bounds is reported as inconclusive, not a proven regression;
- increases allocations by more than **0.5 B/op**, a sub-byte allowance for
  amortization and rounding, not an extra allocation budget;
- takes **50 ms or more** to compose 100 handlers.

Missing, duplicated, invalid, failed or mismatched benchmark cases fail closed.
The CI detector self-test shows that unchanged measurements pass and injected
regressions fail. The comparison prints mean timings as throughput changes and both
allocation totals in the job summary. The job uploads raw reports, logs,
comparison commits and harness hashes for investigation. Inspect BenchmarkDotNet
error and variation columns when timings are noisy; do not rerun until a noisy
failure happens to pass. Local numbers are diagnostic only. Do not commit
benchmark reports, logs or evidence Markdown.

## Raw native-client comparisons

The regression threshold against `main` is distinct from D18's **at most 5%
throughput overhead against the raw native client**. In-memory and callback
microbenchmarks do not establish that real-broker budget. Real transport adapters
and their raw-client cases remain planned.

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
