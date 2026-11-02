---
title: Partition completion watermarks
description: "Bounded arrival-order completion tracking for log transport authors."
---

`PartitionCompletionWindow<TPosition>` is a core building block for log transports. It tracks deliveries in native arrival order and releases only the contiguous completed prefix. Positions are opaque: the window never compares, sorts or increments them. A position can include a native next offset and leader epoch, or be a string checkpoint.

The window does not dispatch handlers, commit to a broker, pause fetching or drain workers. The core [partition scheduler](partition-scheduler.md) composes those mechanisms. [Kafka integration](kafka.md#register-a-consumer) remains planned under [#182](https://github.com/thomhurst/Relayer/issues/182); Kafka consumers still require `Concurrency(1)`.

## Tracking and completion

Create one window per assigned partition. A single coordinator must serialize **all** window access, including completion reports from concurrent workers. Workers must report completions back to that coordinator instead of calling the window concurrently. There are no internal locks.

Track every delivery before dispatching it. Only report completion after its handler and any durable failure handling succeed. Leave failed, cancelled or unfinished deliveries incomplete.

```csharp
var window = new PartitionCompletionWindow<long>(capacity: 64);

// These are native next-offset checkpoints; gaps do not need synthetic entries.
window.TryTrack(15, out var first);
window.TryTrack(91, out var second);

window.TryComplete(second, out _, out var count); // accepted; count == 0
window.TryComplete(first, out var checkpoint, out count); // checkpoint == 91; count == 2
```

`TryComplete` returns `true` when it accepts a completion, including a completion blocked behind an earlier gap. A positive `completedCount` means the prefix advanced: the returned position is the exact checkpoint supplied for its last delivery. A zero count means no checkpoint is available; ignore the position output in that case. The count includes previously completed records released when the gap closes, so it can supply the batch count for an amortized commit span.

The transport owns native last-position versus next-position semantics. It also owns batching, commit tracing, commit retries and the last successfully committed position. The window releases completed entries before any broker commit and does not retain the returned checkpoint for retry.

## Capacity and memory lifetime

`Count` includes both unfinished deliveries and completed deliveries blocked behind them. `IsFull` becomes true at the fixed `Capacity`. `TryTrack` then returns `false` with an invalid default token and changes nothing. Keep that delivery available for later admission and apply native backpressure; never dispatch it without tracking it, discard it or acknowledge past it. Completing later work cannot free capacity while the oldest gap remains.

The window retains each position until its prefix advances or `Reset()` clears the assignment. It does not copy positions or own their underlying buffers. Keep borrowed position storage valid for that entire time, then through any asynchronous native commit using a released checkpoint. The scheduler or adapter must also retain native message bodies and headers until processing finishes; this window tracks only positions.

Construction allocates fixed storage. `TryTrack` and `TryComplete` allocate no managed memory and take constant time per tracked/released delivery, amortized over a completed prefix. Clearing a prefix releases its position references. `Reset` clears the full capacity and releases all retained positions. `PartitionCompletionWindowBenchmarks` compares in-order and out-of-order two-delivery windows with `MemoryDiagnoser`; it measures this primitive, not end-to-end broker overhead.

## Ownership changes

Treat `PartitionCompletionToken` as opaque. Default, foreign, duplicate, reused-slot and pre-reset tokens are rejected without changing the window. Tokens do not retain the window or its positions.

Stop dispatch before revocation or loss. The adapter separately owns bounded draining, a safe commit while still owning the partition, and replay from the last native commit. Call `Reset()` on loss or before reusing the window for a new assignment. Reset discards positions without acknowledging them, and late completions from the previous assignment cannot acknowledge new deliveries, even when a ring slot is reused.

Tracking alone does not make a rebalance safe: native ownership and commit fencing still belong to the transport and scheduler.
