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

## Error history

Copies keep the original message ID and trace context. Dead-lettered and redelivered messages carry their failure history: the original ID, total failed attempts, first and last failure times, and for every attempt the consumer, host, exception type, message and stack trace. The history is serialized with generated JSON, so it works under Native AOT.

Delivery remains at-least-once: a crash between sending a copy and completing the original can duplicate it.

For the full contract, including what transport adapters must persist, see the [design document](https://github.com/thomhurst/Relayer/blob/main/docs/design.md#5-failure-handling).
