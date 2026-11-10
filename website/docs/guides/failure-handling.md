---
title: Failure handling
description: "Classify errors, retry in process, redeliver with delay and dead-letter with full error history, using native broker features where they exist."
---

There is one failure pipeline for every transport:

```text
exception
   │
   ├─ classify ──► permanent (configured types, deserialization, unknown type) ──► dead-letter
   │
   ├─ immediate retry (in process, jittered backoff, N attempts)
   │
   ├─ delayed redelivery (native delay if supported, else a scheduled copy) with jitter
   │
   └─ dead-letter (native DLQ, or a copy to the dead-letter endpoint)
```

## Classification

`DefaultFailureClassifier` treats unknown message types and invalid bodies as permanent (`DeadLetter`) and everything else as retryable (`Retry`). Deserialization and contract failures therefore never block a partition or crash a consumer.

Compose your own rules once and share the classifier across consumers:

```csharp
var classifier = new FailureClassifierBuilder()
    .On<ArgumentException>(FailureAction.DeadLetter)
    .On<TimeoutException>(FailureAction.DeadLetter,
        static (_, context) => context.Attempt > 5)
    .Build();
```

Rules match the declared exception type and its derived types, in registration order. The first matching rule whose predicate returns true wins. Unmatched failures fall back to `DefaultFailureClassifier`, or to a custom `IFailureClassifier` passed to `Build`. Predicates must be thread-safe.

## Retries, redelivery and dead-letters

```csharp
var failures = new FailurePipeline(services, "broker", "orders", "billing",
    new FailureOptions
    {
        RetryDelays = new[] { TimeSpan.FromMilliseconds(100), TimeSpan.FromMilliseconds(500) },
        RedeliveryDelays = new[] { TimeSpan.FromSeconds(30), TimeSpan.FromMinutes(5) },
        DeadLetterEndpoint = "orders.errors"
    }, copy: CopyOriginalDelivery, scheduler: durableScheduler);
```

Each delay entry permits one more attempt. Empty retry and redelivery lists mean immediate dead-lettering. Jitter shortens each delay by up to 20%, which keeps configured delays within capability limits.

At composition the pipeline checks every configured delay and the dead-letter destination against the endpoint's [capabilities](../concepts/capabilities.md), and picks the mechanism:

1. Native redelivery, when the delay fits within `MaxRedeliveryDelay`.
2. Otherwise a native scheduled copy, when it fits within `MaxDeliveryDelay`.
3. Otherwise a durable scheduler (`IFailureScheduler`), supplied by an outbox integration.

Native dead-lettering is used only when the endpoint guarantees error metadata. Otherwise a copy goes to the dead-letter endpoint and the original is completed. If no mechanism is available, startup fails.

## Retries never duplicate side effects

Each immediate retry runs in a fresh DI scope. Messages the handler sends through `ISender` are buffered and only sent after the handler, middleware and scope disposal all succeed; a failed attempt discards them. Sends enlisted in an outbox transaction are rolled back with that transaction. Calls you make directly on a native client are your responsibility.

## Manual settlement

Settlement is automatic by default. A handler or consume middleware can request an explicit outcome through the context:

```csharp
var settlement = context.Features.GetRequired<ISettlementFeature>();
settlement.DeadLetter("Order has no customer");
// Or: settlement.Retry(TimeSpan.FromMinutes(1));
// Or: settlement.Complete();
```

These calls queue a decision; they do not acknowledge the native delivery inline. Only one request is allowed per attempt. Another request throws `InvalidOperationException`. Negative retry delays and empty dead-letter reasons are rejected. Do not retain this pooled feature after processing, or use it concurrently.

Requests apply after the handler, consume middleware and async scope disposal succeed. `Complete()` also waits for buffered sends to flush. If the handler or middleware throws after any request, the exception wins: the request is cleared and the normal classification, retry and dead-letter policy runs. Each immediate retry starts without a request. Cancellation, scope-disposal errors, send-flush errors and durable fallback errors propagate without a settlement decision.

With an executable `FailurePipeline`, `Retry` and `DeadLetter` discard buffered direct sends, including sends made during scope disposal. The [EF Core consumer boundary](../reference/ef-core-inbox.md) also rolls back its owned transaction for these requests. Without that boundary, the application owns transaction rollback. Calls to native clients remain the application's responsibility. The boundary requests completion before commit when no outcome is pending; further settlement requests during scope disposal throw because the database work is already durable. `Complete` and automatic completion flush successful direct sends as usual.

`Retry(delay)` requests a new delivery using the exact delay, without jitter or the configured in-process retry sequence. `FailurePipeline` chooses native redelivery, then a scheduled copy, then a durable scheduler, using the same capability limits and executors as exception handling. A delay without an executor throws at the request call with an actionable message. The request can then be handled by the normal exception policy. Manual dead-lettering uses native dead-lettering or a durable copy. Copies include the requested reason in `FailureHeaders.DeadLetterReason` (`relayer.dead_letter.reason`). Prior exception history is preserved; manual outcomes do not invent an exception.

Batch handlers use the feature on each `batch.Items[i].Context`. A reported per-item failure overrides that item's request; a thrown batch exception overrides every request. Requests from failed items are cleared before in-process retry. Batch sends share one buffer: when some items complete and others fail, retry or dead-letter, buffered sends make the entire attempt fail because those sends cannot be attributed safely. If every item retries or dead-letters, buffered sends are discarded. With no buffered sends, outcomes remain independent. Cancellation, disposal and fallback failures prevent returning any batch settlement decisions.

When composing the lower-level `ConsumePipelineBuilder` with an explicit `ConsumeFailureDelegate`, successful manual requests become native dispositions directly. That composer owns capability validation, side-effect durability and fallback handling. The in-memory hosted consumer uses this direct native path. Transport-specific operations such as Azure Service Bus defer remain available through native transport features.

## Consumer circuit breaking

Kafka consumers can opt into native pause/resume protection with `consumer.Options.CircuitBreaker`. It is disabled by default. The policy counts one logical delivery after its in-process retries, using fixed sampling windows. Only exceptions classified as `FailureAction.Retry` count as circuit failures. Permanent (`DeadLetter`) and intentional (`Discard`) outcomes follow the classifier immediately and count as successful circuit outcomes, so a permanent poison record cannot trap recovery in a loop. Once the minimum throughput and failure ratio are reached, the consumer pauses its assigned partitions natively. The threshold-crossing failed delivery is retained in Kafka without a dead-letter copy or an offset commit past it; deliveries while open do not enter handlers.

```csharp
consumer.Options.CircuitBreaker = new()
{
    FailureRatio = 0.5,
    MinimumThroughput = 10,
    SamplingDuration = TimeSpan.FromSeconds(30),
    BreakDuration = TimeSpan.FromSeconds(5),
};
```

These are the defaults when configured. Startup rejects non-finite ratios, ratios outside `(0, 1]`, minimum throughput outside `1..1073741823`, and durations below one millisecond or above the native timer limit. In-process retries run before the circuit records a retryable failure; retryable failures below the threshold still follow the normal dead-letter policy.

After the break duration, one retained delivery probes recovery. A failed probe opens the circuit again without copying or committing it; a successful probe closes it and resumes normal consumption. Kafka's native polling and ownership handling continue while paused. Assignment changes invalidate borrowed batches, and replay controls still drain the current batch before seeking. Shutdown stops polling without waiting for the break duration; a later timer signal cannot restart the receiver. Delivery remains at-least-once, so completed messages from an uncommitted batch can repeat.

The endpoint's `relayer.circuit.state` metric records actual transitions: 0 closed, 1 open, 2 half-open. Service Bus and other transports' native circuit integration remains planned.

## Error history

Copies keep the original message ID and trace context. Dead-lettered and redelivered messages carry their failure history: the original ID, total failed attempts, first and last failure times, and for every attempt the consumer, host, exception type, message and stack trace. The history is serialized with generated JSON, so it works under Native AOT.

Delivery remains at-least-once: a crash between sending a copy and completing the original can duplicate it.

For the full contract, including what transport adapters must persist, see the [design document](https://github.com/thomhurst/Relayer/blob/main/docs/design.md#5-failure-handling).
