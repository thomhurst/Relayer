---
title: Partition scheduler
description: "Key-ordered scheduling, native backpressure and bounded drain for log transport authors."
---

`PartitionScheduler<TIncoming,TBinding,TPartition,TPosition>` runs an existing receive/failure pipeline with key ordering and bounded concurrency. It builds on [partition completion watermarks](partition-completion.md). This is a transport-author SPI; [Kafka consumer integration](kafka.md#register-a-consumer) remains planned in [#182](https://github.com/thomhurst/Relayer/issues/182). Kafka registration still requires `Concurrency(1)`.

## Compose once, pump on the native owner

Supply the native source, the composed `ReceivePipeline.ProcessAsync` delegate, and a struct binding implementing `IPartitionMessageBinding<TIncoming>`. The native types below stand for your adapter's record, binding, partition and checkpoint types:

```csharp
var scheduler = new PartitionScheduler<NativeRecord, NativeBinding, NativePartition, NativeCheckpoint>(
    source, pipeline.ProcessAsync, new PartitionSchedulerOptions
    {
        ConcurrencyPerPartition = 4,
        MaxConcurrency = 32,
        MaxBufferedPerPartition = 64,
        MaxBufferedDeliveries = 512,
    });

var assignment = scheduler.Assign(partition);

// In native arrival order, while this assignment is owned:
if (!scheduler.TryEnqueue(assignment, record, checkpoint))
{
    // Keep this record and retry it before offering later records from its partition.
}
scheduler.Pump();
```

Serialize **every** scheduler call on the native owner's receive/control pump, including assignment, revocation and loss callbacks. `Pump()` never waits. It observes completed pipeline `ValueTask`s exactly once, dispatches eligible keys, publishes safe checkpoints and calls native pause/resume. Call it regularly even when native fetching is paused or returns no records. Handler continuations never invoke the source or change scheduler state. Native callbacks and binding methods must not reenter the scheduler.

`IPartitionMessageBinding<TIncoming>` extends `IMessageBinding<TIncoming>` with `KeysEqual(first, second)`. Compare typed/native keys in place, without decoding, allocating, or retaining a key separately. Define null-key equality according to the adapter's native semantics. Equal keys execute serially in arrival order; unrelated keys can use the configured concurrency. Admission scans the bounded retained window to find a key's current head. Per-key queues and a ready queue then dispatch without repeated key comparisons. Ready assignments share global handler capacity in round-robin order.

Reuse the existing receive/failure pipeline, including its serializer, inbox, durable retry/dead-letter fallbacks and telemetry. Only `Complete` and `SettledByHandler` advance the watermark. A thrown exception, cancellation, `Redeliver` or native `DeadLetter` stops dispatch for that assignment and leaves a gap. Other assignments continue. Read `GetFailure(assignment)` and report it through the adapter's existing worker error/health path; recovery must revoke and replay, never skip the failed delivery.

Size the receive pipeline's context pool for `MaxConcurrency`. The scheduler itself uses no worker tasks, continuations, locks, reflection or per-delivery allocations. Construction and assignment allocate reusable storage and one cancellation source per assignment.

## Limits, backpressure and borrowed storage

`MaxBufferedPerPartition` bounds each assignment's active, queued and completed-but-blocked deliveries. `MaxBufferedDeliveries` bounds all retained deliveries, including late handlers from old assignments. `MaxConcurrency` also counts those late handlers. `ConcurrencyPerPartition` bounds handlers within one assignment. All limits must be positive; per-partition limits must fit the global limits, and concurrency must fit the corresponding buffer capacity. Invalid combinations fail at construction.

`TryEnqueue` returns `false` for full buffers, stopping, failed, revoking, fenced, foreign or default assignment tokens. A rejected delivery still belongs to the caller. Retain it and retry before admitting later deliveries from that partition; never drop it or skip ahead. Finish each admission batch with `Pump()` so native backpressure takes effect. The pump pauses full or failed assignments and resumes them when capacity returns. Each fresh assignment explicitly sets native pause/resume state; a pause from an old assignment cannot strand its replacement.

Accepted deliveries belong to the scheduler until `TBinding.Release(message)` runs exactly once. This static virtual callback defaults to no action. Implement it to return a native lease or pooled buffer; it must not throw. Retain borrowed body, headers, keys **and checkpoint storage** until release. Completed deliveries behind a gap remain retained because their opaque checkpoints can borrow the same native storage. Native fetch batches that invalidate records on disposal need a lease that survives until every accepted record is released.

## Native checkpoint publication

Implement `IPartitionSource<TPartition,TPosition>` with synchronous `Commit`, `Pause` and `Resume` methods. These run on the scheduler's owner. `Commit` receives the last contiguous durable checkpoint, preserving the exact native position supplied on admission. Positions are never compared or incremented. Encode native last-position versus next-position semantics in the checkpoint itself.

`Commit` must capture the checkpoint before returning. The transport owns native commit batching, asynchronous flush, retries, tracing and ownership checks through flush completion. Copy or retain borrowed checkpoint data if asynchronous native operations need it after this call. Native pause and resume must affect only the supplied partition.

If checkpoint capture throws, the error propagates from `Pump`; the scheduler retains the checkpoint and deliveries and retries capture on the next pump before observing further completions. Pause/resume errors also propagate and retry on a later pump. A native ownership loss must call `Lose` before any more completion publication. The core cannot fence a native commit that the source already accepted: the transport must fence its own pending commit queue on ownership changes.

## Revoke, loss and shutdown

Keep the existing token for retained partitions during cooperative rebalancing. Call `Revoke(assignment, timeout)` only for revoked partitions. This immediately stops admission and dispatch for those assignments, applies native pause, and starts a monotonic deadline. Continue pumping until `IsDraining` is false, then flush the source's safe captured checkpoints before returning native revocation. Active work can advance the contiguous prefix during the drain; queued work is replayed. Retained partitions keep dispatching throughout.

At the deadline, the scheduler fences the old assignment, discards queued work and requests cancellation of active handlers. `Lose(assignment)` does this immediately and publishes no further checkpoints, including completed work not yet observed by the pump. Neither path resumes the old assignment. Reassignment gets a fresh token; stale tokens and late completions cannot affect its watermark or pause state.

`Stop(timeout)` rejects new work and begins revocation of every owned assignment. It shortens any existing revocation deadline to the shutdown deadline when necessary, without extending an earlier deadline. All assignments enter draining before native pause callbacks run. If a pause throws, the error propagates, but every assignment still stops dispatch and keeps its shutdown deadline; continue pumping to retry pending pauses and finish draining. Pump until `IsDraining` is false for a bounded ownership drain. A zero timeout fences at the next pump, including assignments already draining after cooperative revocation. Cancellation is cooperative: handlers that ignore it can outlive ownership. Keep pumping until `BufferedCount == 0` to observe their eventual results and release native storage; do not dispose buffers underneath them. Such work continues to count against global capacity and concurrency, including after reassignment. An indefinitely blocked handler necessarily retains its borrowed storage.

For the existing `IConsumerHealthProvider` contract, use `BufferedCount == 0` as idle and `LastProgressAt` for durable pipeline progress. `ActiveCount == 0` alone is not idle: a failed gap can still retain work. Scheduler state is owner-only; publish health snapshots through the adapter's existing thread-safe health state.

`PartitionSchedulerTests` covers ordering, gaps, capacity, ownership epochs, timeout, shutdown and borrowed lifetime with deterministic completion signals and a manual clock. `PartitionSchedulerBenchmarks` uses `MemoryDiagnoser` for synchronous dispatch, unrelated out-of-order completion and equal-key queues. Its raw delegate baseline isolates bookkeeping; it does not establish the end-to-end native-broker overhead budget.
