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
No allocation stacks are collected. Retained workload assemblies are never rebuilt or wrapped.

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
