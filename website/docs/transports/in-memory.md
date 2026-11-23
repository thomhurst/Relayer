---
title: In-memory transport
description: "A process-local transport in the core package for tests, samples and modular monoliths, with native queue and log semantics."
---

The in-memory transport ships in `Relayer`. Use it for process-local delivery, tests and samples. Each instance owns its endpoints; named transports and service providers do not share messages.

```csharp
using Relayer;
using Relayer.InMemory;

services.AddRelayer(relayer =>
{
    relayer.AddMessageTypesFromThisAssembly();
    relayer.InMemory("local", local =>
    {
        local.Send<OrderPlaced>("orders");
        local.Consume("orders", consumer => consumer.Handler<OrderPlacedHandler>());
    });
});
```

Declared endpoints already exist as native in-memory entities when the adapter is constructed. Topology validation and provisioning report no external differences; unknown names still fail.

Starting the host composes generated handlers into the core receive pipeline and starts native receivers. The default endpoint is a bounded queue with native scheduling, redelivery and dead-lettering. Consumers use the same `consumer.Failures(policy)` and `consumer.Classify(classifier)` calls as broker transports. Retryable failures get three in-process retries by default, then go to the native dead-letter collection. Redelivery delays use native scheduling and retain attempt counts and error history independently for each log group. In-memory redelivery requires a delay within the endpoint’s `MaxRedeliveryDelay`; copy and outbox redelivery fallbacks are rejected during startup (and for manual retry requests) to retain the failing group and its admission credit. Increase the native limit or shorten the delay. With native dead-lettering disabled, configure `DeadLetterEndpoint` to a separate declared endpoint for the core copy fallback. A dead-letter destination equal to the source is rejected at startup because copying would wait on the current delivery’s admission credit. An unavailable failure path fails at startup.

The host composes all consumers for a named in-memory transport before registering any native receivers for that transport. If pipeline composition throws, retrying startup on the same host can compose those consumers again.

A typed CloudEvents send route to an endpoint with a hosted consumer must use the default `ce_` header prefix. Startup reports incompatible `SendRouteOptions.HeaderNames` values before any receiver starts for that transport. Send-only endpoints can use a custom prefix.

Configure declared endpoints through named Options. An endpoint with no override uses queue defaults. An override for an undeclared endpoint fails startup.

For deterministic tests, configure a `TimeProvider` for the named transport before the host starts. The clock controls scheduled delivery, redelivery and retry timing. The default is `TimeProvider.System`.

```csharp
local.Options.Configure(options => options.UseTimeProvider(clock));
```

Keep `TransportCapabilities.Tombstones` disabled. In-memory endpoints store byte bodies and reject this capability during construction because they cannot preserve native null values. Native single and batch sends also reject any `OutgoingMessage` marked as a tombstone. Ordinary empty byte bodies are supported.

```csharp
using Relayer.Transports;
local.Options.Configure(options => options.Endpoints.Add("orders", new()
{
    Capacity = 128,
    Partitions = 4,
    Capabilities = new()
    {
        ConsumptionModel = Relayer.Transports.ConsumptionModel.Log,
        OrderedByKey = true,
        MaxDeliveryDelay = TimeSpan.FromHours(24),
        MaxRedeliveryDelay = TimeSpan.FromHours(1),
        DeadLetter = true,
        MaxBatchSize = 100,
    },
}));
local.Send<OrderPlaced>("orders", route => route.OrderingKey(order => order.CustomerId));
local.Consume("orders", consumer =>
{
    consumer.Options.Group = "billing";
    consumer.Concurrency(4).Handler<OrderHandler>();
});
```

Queue consumers compete for individual messages. Unordered queues use one partition and support multiple concurrent receivers. In ordered queues, a key hashes to one partition. Logs fan out records to each declared consumer group, assign positions in native partition order, and advance each group's position only after settlement. A group has one consumer declaration. Ordered queues and logs require concurrency equal to the partition count, with one serial receiver per partition. Retries hold that ordered partition's head; unordered queue retries allow ready messages to proceed. The core partition scheduler can later add finer key-level parallelism without changing native ordering.

`Capacity * Partitions` bounds all outstanding native messages, including scheduled sends, retries and messages retained by slow log groups. Sends await admission and observe caller cancellation. A buffered send back to a full queue waits for admission during the flush. When the route timeout cancels that wait, the hosted receiver faults with `TimeoutException` and leaves the original delivery unsettled. Scheduled sends complete when accepted, before their due time. Direct sends through `ISender` and the native transport both use the transport's configured `TimeProvider` to check future `DeliverAt` values against `MaxDeliveryDelay`. The scheduler considers all accepted due times; a future message does not block immediate sends. Native size limits count UTF-8 body, header names, header values and ordering key. Batch sends preserve input order and may partially complete. Native replay, transactions, pause/resume and batch-handler composition are not implemented; requesting them fails explicitly.

Each partition reuses one scheduler timer and channel wait, including while idle and when an earlier deadline arrives. Scheduler wake-ups do not consume message admission capacity. Stopping preserves accepted schedules for restart; a full partition does not block scheduled delivery to other partitions. Scheduling adds no steady-state managed allocations after message storage and buffers have warmed up.

Unordered queues cap the advertised `MaxRedeliveryDelay` at construction to the time between the transport clock and `DateTimeOffset.MaxValue`. At settlement they check the current UTC clock again. A delay beyond either limit raises `InvalidOperationException`. `InMemoryDelivery.Redeliver` raises it before settling the message, so the callback can choose a shorter delay or another disposition. A manual redelivery's delay starts when the call succeeds. If the callback returns a redelivery disposition, the delay starts when the receiver accepts it.

Ordered queue and log redelivery waits check elapsed time after each timer wake-up. Timer precision cannot release the retained partition head before its requested delay. Positive waits round up to whole milliseconds, including delays shorter than one millisecond. Stopping preserves the retry deadline; time spent stopped counts toward the delay when the transport restarts.

`InMemoryTransport` also exposes the native SPI directly: construct it with explicit endpoints, register `InMemoryReceiveDelegate` callbacks before first start, and send `OutgoingMessage` values. Adapt a composed core consumer with `ReceivePipeline<InMemoryDelivery, InMemoryMessageBinding>`. `NativeMessage<InMemoryDelivery>` exposes the original receive view. Manual settlement uses `InMemoryDelivery.Complete`, `Redeliver` or `DeadLetter`; return `Disposition.SettledByHandler` after doing so. Do not retain native views or their storage beyond processing.

A successful native settlement call records the outcome. The receiver applies it when the callback ends, even if later callback work throws. The original exception can still fault `Completion`. On restart, completed and dead-lettered messages stay settled, and a native redelivery retains its requested delay. In hosted handlers, mark the native feature's disposition as `Disposition.SettledByHandler` when returning after native settlement. If the handler throws after native settlement, the error propagates without a handler retry or another settlement. Portable [settlement requests](../guides/failure-handling.md#manual-settlement) still wait for successful processing.

Hosted consumers emit a process span that links to the producer's creation context and includes native settlement. `Complete`, `Redeliver` and `DeadLetter` emit their own client settlement spans for both core and manual calls. Subscribe to the provider-owned `Relayer` ActivitySource to enable them. Standalone instances opt in with the constructor's `tracing:` parameter; configure the composed consumer's `Tracing` and native settlement callback as described in [Tracing, metrics and logging](../guides/telemetry.md#delivery-and-settlement-tracing). These native methods already trace settlement, so do not also wrap them with `WrapSettlement`.

Stop first stops fetching and drains active handlers. Cancelling stop cancels active processing; unsettled messages and queued messages remain for restart. A native queue redelivery already recorded by the callback retains its scheduled deadline and attempt even when stop is cancelled. Unexpected receive or settlement failures stop fetching. If a receive callback throws `OperationCanceledException` before transport shutdown, `Completion` becomes canceled once the loops stop, and awaiting it throws the original exception. Other unexpected failures fault `Completion`.

A direct receive callback must return an explicit disposition: returning `default(Disposition)` or a default `ValueTask<Disposition>` faults `Completion`, leaves the committed position unchanged and retains the delivery for restart. The hosted service observes this failure through `BackgroundService`. `StopAsync` reports the failure; disposal releases resources without reporting the same processing failure again. Dispose closes admission and wakes blocked senders. Data and dead letters exist only in this process; disposal is not durable storage.

Inspect owned dead-letter snapshots with `transport.DeadLetters(endpoint)` and native group progress with `GetCommittedPosition`. Use `WireString.Utf8` to read the original bytes of byte-valued headers in the message and its dead-letter error metadata. Copied body, headers and UTF-8 ordering keys outlive the borrowed send buffers; completed messages return their storage to a bounded pool. Size `MessagePoolCapacity` for expected retained traffic and warm representative body/header sizes when measuring steady-state allocation.

For populated-queue allocation measurements, use the [64-message batch and channel controls](../contributing/performance.md#populated-queues-and-asynchronous-coordination). The native receive loop no longer adds a pooled async wrapper around channel reads. Callback continuations, concurrent channel waits, serialization and DI can still allocate; inclusive round-trip totals do not measure native message storage alone.

The native adapter preserves `traceparent` and `tracestate` unchanged. Its system identifier is `InMemoryTransport.MessagingSystem` (`relayer.inmemory`). Queue and partitioned log modes pass the reusable [transport conformance suite](conformance.md), including shared send/process/settlement tracing and creation-context preservation.
