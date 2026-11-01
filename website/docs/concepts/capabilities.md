---
title: Capabilities
description: "How each endpoint declares what it supports natively, which fallbacks the core runs, and why unsupported configuration fails at startup."
---

Each transport declares the capabilities of each endpoint, because an SQS FIFO queue and a standard queue differ. A feature that needs a capability either uses the native mechanism or a documented core fallback. When there is neither, startup fails with a clear error. Relayer never degrades silently.

The core runs every fallback, so transports contain native code only.

## The capability record

`TransportCapabilities` is a sealed record with init-only values. A new record declares queue consumption, no optional features, no native delays and a maximum batch count of one.

| Property | Meaning |
|---|---|
| `MaxDeliveryDelay` | Longest native scheduled delivery. `null` means no native scheduling. |
| `MaxRedeliveryDelay` | Longest native delayed redelivery. `TimeSpan.Zero` means immediate redelivery only; `null` means none. |
| `DeadLetter` | The endpoint can dead-letter natively with error metadata. |
| `OrderedByKey` | The endpoint preserves order per key (partition key, session, message group). |
| `MaxBatchSize` | Native batch count. Defaults to 1. |
| `Transactions` | Native transactions are available. |
| `PauseResume` | The consumer can be paused and resumed natively. |
| `Replay` | The endpoint supports seeking or replay. |
| `MaxMessageBytes` | Declared message size limit. `null` means not declared, not unlimited. |
| `ConsumptionModel` | Queue or log-based consumption. |

## Native mechanisms and fallbacks

| Feature | Native when available | Fallback |
|---|---|---|
| Delayed delivery / scheduling | Service Bus `ScheduledEnqueueTime`, SQS `DelaySeconds` on standard queues (up to 15 minutes; not on FIFO queues), RabbitMQ delayed-message plugin | Outbox `deliver_at` |
| Delayed redelivery of a received message | SQS visibility timeout (up to 12 hours); immediate only: Service Bus abandon, RabbitMQ requeue | Scheduled re-send of a copy, else the outbox |
| Dead-letter with error metadata | Service Bus DLQ with properties | Send a copy to the dead-letter endpoint (Kafka DLT topic, SQS DLQ queue), then complete |
| Ordering | Kafka partition key, Service Bus sessions, SQS FIFO message group | None: the route must not declare an ordering key |
| Batching | Kafka, SQS (10), Service Bus batches | Client-side batching |
| Transactions | Kafka transactions, Service Bus cross-entity transactions | Outbox |

## Validation

Before receive services start, the core asks each runtime adapter for the capabilities of every declared source and destination, once. It checks that delays are null or non-negative, batch counts are at least one, declared size limits are positive and every ordering key targets an `OrderedByKey` endpoint. Errors name the transport, route and endpoint, and recommend a fix. See [Configuration](../guides/configuration.md#startup-and-reload) for the full list.
