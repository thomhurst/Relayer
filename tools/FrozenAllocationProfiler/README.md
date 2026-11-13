# Frozen CLR allocation callback control

This external investigation tool counts `ICorProfilerCallback.ObjectAllocated` notifications and
`ICorProfilerInfo4.GetObjectSize2` object sizes between native begin/end exports. It reuses the
official dotnet/samples SDK and no-op callback scaffolding at commit
`3810558800760fc39282cc939bac645735514209`; no ABI declarations are handwritten.

Unlike EventPipe allocation ticks, these are individual CLR heap allocation notifications,
not weighted samples. Counts distinguish the pooled handler state-machine boxes suggested
by #488's sampled traces. Nested type definitions and generic arguments are resolved inside
the allocation callback, once per ClassID. Resolution/callback failures make the report invalid.
The fixture checks known object counts/sizes from four simultaneous allocating threads,
including nested generic types, while excluding warmup allocations.

This is a **separate instrumented execution**. Enabling callbacks changes CLR allocation fast
paths; native locking and metadata resolution also change scheduling. Counts do not reconstruct
the historical Performance run, establish production throughput, prove a candidate regression,
or permit subtraction from other controls. Object sizes are the runtime's `GetObjectSize2`
values, not a claim to account for every byte of allocation-context/alignment overhead.
Default captures collect no allocation stacks; the parallel workflow's opt-in managed
stack mode is described below. Retained workload assemblies are never rebuilt or wrapped.

The dedicated workflow builds the native profiler and .NET 10 external probe, runs known-type
fixture checks, verifies all original input inventories, and runs the existing original-BDN-waiter
ABBA launcher. Each process has a separate report; complete inputs, native SDK, binaries and
all outcomes (including failures) are uploaded. No Performance threshold changes or retries.

For #537, `frozen-exact-tail-control.yml` reuses this profiler with
`frozen_bdn_control.py --allocation-profiler <library>`. This mode supports only
the default `--case concurrent`; the native callback provider and operation-count
validation are specific to that retained benchmark. Other cases fail before launch. Set
`RELAYER_ALLOCATION_BDN_TAIL=1` only for this mode. The existing pinned official
[EventPipe profiler sample](https://github.com/dotnet/samples/blob/3810558800760fc39282cc939bac645735514209/core/profiling/eventpipe/src/EventPipeProfiler.cpp)
documents synchronous delivery on the emitting thread. The profiler enables only
BDN's existing EngineEventSource, checks provider/payload/version/thread identity,
and counts allocations after each WorkloadActualStop. A subsequent timed stop
resets earlier counts; BenchmarkStop disables collection. Shutdown writes the final
counts, the complete native event sequence, operation denominator and coverage.
The reader validates event order and cross-checks the final denominator with native
`// GC`. Missing/extra boundaries, metadata failures and malformed reports fail closed.
The known-count fixture checks multiple timed iterations and four allocating threads,
excludes earlier tails/timed/post-stop allocations, and exercises missing/extra boundaries.

This envelope includes final timed-iteration cleanup/GC/logging and extra-stat
setup/work/cleanup; it **does not isolate MeasureWithGc**. The event callback's
serialization lock determines the boundary for concurrent allocations. Counts are
exact CLR notifications within that instrumented envelope, not allocation-context
bytes, allocation call sites, or historical owner attribution. No stacks are captured.
Compare complete ABBA outcomes only within their own bracket; never subtract native
GC totals from callback totals or independent controls. Retain every type, event,
coverage record, original image and failed outcome. A successful new run still does
not identify the historical signal or justify a production change.

The exact-tail workflow defaults to retained Performance run `43124385683` and
requires its unexpired `performance-43124385683-1` artifact. Hosted artifacts expire
after their configured retention period (these control outputs use 90 days). A
missing input fails the download rather than validating the profiler. Preserve
inputs independently before expiry; for a later experiment, dispatch with an
explicit retained Performance run instead of treating a download failure as a
workload result.

## Original adaptive-engine method control (#488)

`frozen-exact-adaptive-control.yml` reuses this profiler with the original retained
BDN executable and Default job. The launcher selects `--allocation-boundary method`;
the default `tail` boundary preserves the separate #537 control. Opt-in `RELAYER_ALLOCATION_TYPE` and
`RELAYER_ALLOCATION_METHOD` select a non-inlined metadata method; the launcher selects
`BenchmarkDotNet.Engines.Engine.MeasureWithGc`. Native CLR
`SetFunctionIDMapper2` and `SetEnterLeaveFunctionHooks3WithInfo` filter hooks to that
method. Slowpath frame-info stubs preserve the managed calling convention, following
[the runtime's official slowpath ELT fixture](https://github.com/dotnet/runtime/blob/v10.0.14/src/tests/profiler/native/eltprofiler/slowpatheltprofiler.cpp).
The existing ObjectAllocated callback collects process-wide type counts between
entry and normal leave. Reports are written at normal profiler shutdown. Exactly
one entry/leave is required; missing, repeated, exception-unwound or tailcalled
boundaries fail closed. Manual Begin/End exports reject method mode.

BDN 0.15.8's
[MeasureWithGc](https://github.com/dotnet/BenchmarkDotNet/blob/v0.15.8/src/BenchmarkDotNet/Engines/Engine.cs#L268)
is non-inlined and invokes ReadInitial, the original WorkloadAction, and ReadFinal.
This excludes surrounding iteration cleanup/logging and GetExtraStats setup/cleanup
from the earlier sampled tail. It still includes the two counter reads and concurrent
background activity. Allocation callbacks are serialized against method-boundary
callbacks; objects racing a boundary are assigned by callback lock order. Runtime
object sizes need not sum to the inclusive GC counter, which also accounts for
allocation-context/alignment overhead.

This is a new instrumented execution, not the historical allocation stream. ELT
changes JIT code, allocation callbacks change fastpaths, and native locking and
metadata resolution change scheduling. Keep each native GC denominator alongside its
report and preserve every unfavorable outcome. Do not subtract independent controls,
claim historical delta ownership, use timings as throughput evidence, or infer a
production fix. The known-count multithread fixture validates exact object counts,
warmup exclusion and failure handling before hosted frozen-image execution.

## Native-parent method control (#537)

The exact-adaptive workflow accepts `parent=native` to reuse the reviewed
`FrozenNativeHost` / BenchmarkDotNet 0.15.8 DotNetCliExecutor route. Each launch
gets its own retained CLI wrapper with profiler variables set only for the child;
the managed parent is unprofiled and cannot truncate or contaminate the child's
report. The launcher keeps native receipts/logs, validates callback boundaries,
and attempts every launch after a failure. Console replay remains the default.
This combination measures owner types in a new instrumented native-parent
execution, not the historical allocation stream. All existing ELT/JIT, callback,
locking, adaptive-count and host-drift limits apply. It does not isolate pipe
effects or permit subtraction of independent runs. Use explicit original inputs
and inspect every type family and failed outcome before choosing an intervention.

## Concurrent receive test fixture (#687)

`concurrent-allocation-owners.yml` covers net10/net11 independently. By default it
builds and retains the test image. Manual dispatch can instead set `ci_run_id` and
`ci_run_attempt` to download that failed CI run's original core-test artifact. The
workflow verifies its binary manifest and commit against the selected CI run
(PR checkouts must be two-parent merges whose second parent matches the run's
immutable `head_sha`; current PR base/head metadata can change after later pushes
and cannot verify the historical base. Other events require
the run's exact head commit),
selects the matching recorded runtime (failing if unavailable), and executes those
binaries without rebuilding or modifying them. It retains the downloaded inputs,
source-run metadata and before/after hashes along with every new outcome. These
are new diagnostic executions, never recovery of historical allocation ownership
or replacement of failed CI. The workflow then executes
its unchanged TUnit allocation test, a plain external fixture control and an exact
callback control. `FrozenReceiveProbe` binds once to that image's original
`ConcurrentReceiveFixture.Run`; it does not compile a substitute fixture. It keeps
1000 warmup triples and 1000 measured Scoped/Consumer/Receive triples, every window,
and exact native-scope equality. All three outcomes are attempted and retained even
when a comparison fails. The profiler's existing known-count checks run first.

The exact region surrounds all measured triples, including the external loop and
all process threads; it reports aggregate object types and partitions them into the
external checkpoint windows described below. It does not collect allocation stacks. The external delegate driver excludes TUnit and its
parallel test activity. It is a separate execution: callback scheduling and fastpath
changes can hide or expose the failure. A passing run cannot explain the historical
12,360-byte window, and callback object sizes cannot be subtracted from thread byte
counters or independent runs. Preserve every unfavorable outcome and correlate any
new excess with its own exact report before proposing a production fix. Original
unit-test assertions, warmup and normal test parallelism remain unchanged.


## Concurrent receive window correlation (#687)

The external `FrozenReceiveProbe` checkpoints native callback totals after each
Scoped, Consumer and Receive invocation. The retained fixture remains unchanged:
1000 warmup triples, 1000 measured triples and exact thread-byte equality. Each
`window` row carries its sequential index, mode (0/1/2) and the fixture's returned
thread-byte counter. Preceding `window-type` rows partition the aggregate callback
counts. The reader rejects missing/reordered boundaries, incorrect totals or types,
and the receive reader checks all 3000 counters against the same process's full log.
Known-count checks exercise worker allocations, caller allocations and an empty
window. Other profiler clients keep their existing aggregate-only report.

These boundaries are **external process envelopes**, wider than the fixture's own
counter intervals. They include marker transitions, caller bookkeeping and concurrent
background allocations. Native callback lock order assigns racing allocations to a
window. The final checkpoint stops capture under that same lock, before report-writing
transitions. Counter bytes and CLR object sizes remain distinct: do not subtract them or
claim they must sum. Callbacks and checkpoints change scheduling; no stacks are
collected and a passing run does not explain a historical failure. Preserve all
windows and types, including baseline excesses, failed equality and failed coverage.
Use an unfavorable same-execution window to narrow candidate objects before choosing
further stack diagnostics or a demonstrated fix.

## Actual parallel core-test capture (#687)

`parallel-receive-allocation-owners.yml` runs the complete `Relayer.Tests` process
on Linux x64, independently on net10/net11, with its normal TUnit parallelism.
Unlike `FrozenReceiveProbe`, it retains the other tests in that process. The test
links the existing `NativeAllocationRegion` helper and enables checkpoints only
when `RELAYER_ALLOCATION_PROFILER` is set. Loading occurs before the unchanged
1000 warmup triples; markers surround the unchanged 1000 measured triples, outside
the fixture's thread counters. With no profiler configured, the test keeps its
original exact scope comparisons. It introduces no concurrency throttle or GC
change. Counters are printed even when final diagnostic validation fails.

The workflow reuses the exact callback profiler, known-count fixture and readers;
it retains the full test log, every checkpoint/type report, selected runtime
binaries, test executables, before/after input hashes and SHA256SUMS, including
unfavorable outcomes. The TUnit-output adapter verifies every one of the 3000
checkpoint counters against that same execution's 1000 test triples. Identical
captured-output echoes are accepted; conflicting echoes fail validation. Exact
equality remains the test's assertion, not a parser prerequisite, so an excess is
retained and correlated rather than discarded.

These are all-thread process envelopes: allocations from unrelated parallel tests,
checkpoint bookkeeping and work outside the fixture's counters are included.
Callback overhead and marker calls alter scheduling. Type presence in a window
therefore identifies candidates, not the allocating owner of a counter excess;
default captures have no stacks, and the capture does not reproduce historical scheduling or
permit subtraction across separate executions. Preserve any unfavorable window
and obtain stacks for a reproduced candidate before proposing a production or
runtime correction. A passing parallel capture still cannot close #687.

The parallel workflow also accepts `ci_run_id`, `ci_run_attempt` and `framework`
(`both`, `net10.0` or `net11.0`). Select the framework whose failure artifact
exists. This reuses the retained-input manifest and Git/run provenance checks
from the isolated receive workflow, then runs the original test image and its
recorded installed runtime without rebuilding or editing that image. The image
must include the opt-in checkpoint integration introduced by #705; older images
cannot supply the required 3000 checkpoints and fail report validation.
The before/after hashes and original artifact remain in every outcome's archive.
This is a new parallel execution of the same inputs, not historical scheduling
replay or a rerun of the failed CI check. A passing result still does not assign
an owner to an earlier counter excess.


### Optional managed allocation stacks

After a candidate reproduces, dispatch the parallel workflow with `stacks=true`
(and optionally the existing `ci_run_id`, `ci_run_attempt` and `framework`).
`RELAYER_ALLOCATION_STACKS` names a separate JSONL report. The profiler uses the
CLR's synchronous `DoStackSnapshot` on the allocating thread, reusing the exact
allocation callbacks and checkpoint boundaries. It records managed function IDs,
resolved method names, allocation thread IDs and per-window/class/stack counts.
The reader checks every stack count against that execution's original callback
report; snapshot failures, missing frames and mismatched totals fail validation
while the workflow retains all raw reports and executable inputs. A known-count
fixture verifies allocating call sites on four worker threads and the caller.

This opt-in mode changes scheduling and callback overhead further. Inlined frames
and native call sites are absent, and unresolved method names remain explicit in
the validated report. All-thread checkpoint envelopes include unrelated parallel
work; even matching object sizes cannot be subtracted from fixture thread counters.
A new stack-enabled run is neither historical replay nor proof of ownership for a
previous failure. Default captures still collect no stacks. No assertions, warmup,
measurement counts, GC settings or test parallelism change.
