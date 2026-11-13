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
