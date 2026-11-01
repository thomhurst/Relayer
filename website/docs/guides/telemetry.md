---
title: Tracing, metrics and logging
description: Delivery and settlement spans, consume metrics, and generated failure logs.
---

`AddRelayer` registers a single DI-owned `RelayerInstrumentation`. Its meter is named `Relayer`; subscribe with your OpenTelemetry provider's `AddMeter(RelayerTelemetry.MeterName)`. `RelayerTelemetry.SchemaUrl` pins the semantic conventions version. No OpenTelemetry SDK dependency is needed in the library.

Transport authors compose endpoint telemetry at startup, alongside generated dispatch. For example, after registering and freezing handlers:

```csharp
var instrumentation = provider.GetRequiredService<RelayerInstrumentation>();
var process = instrumentation.CreateEndpoint(new EndpointTelemetryOptions
{
    System = "servicebus",
    Destination = "orders",
    Subscription = "billing",
    ServerAddress = "example.servicebus.windows.net",
});

var consumer = new ConsumePipelineBuilder(
        provider.GetRequiredService<IServiceScopeFactory>(),
        provider.GetRequiredService<MessageTypeRegistry>(),
        provider.GetRequiredService<IMessageSerializer>(),
        HandleFailureDurablyAsync)
    .Handler(orderHandlerDescriptor)
    .Telemetry(process)
    .Settle(SettleNativeMessageAsync)
    .Build();
```

The settlement callback reads the transport's native feature and its disposition. It runs only after durable processing, failure handling and async scope disposal succeed. It must honor `SettledByHandler`, use the broker's native semantics, and record its native operation with a separate endpoint recorder (`OperationName = "complete"`, `"ack"`, `"commit"`, etc.). `context.Services` is unavailable after scope disposal. A settlement error propagates to the receive loop without re-entering failure handling. If settlement runs outside this callback, its duration cannot be included in the process metric. Amortized commits stay outside per-message processing, as design §8.2 requires.

Both consumer constructors support telemetry and settlement. With an executable `FailurePipeline`, the native feature receives the resolved failure disposition before the settlement callback runs. Immediate retries share one delivery count and duration, including all attempt scopes, send flushes, durable failure work and settlement. A successful retry clears earlier failures from the duration's `error.type` only after its scope disposal and buffered send flush succeed. Each failed attempt is still logged. A later settlement error marks the duration as failed.

The consumer records one `messaging.client.consumed.messages` measurement per delivery and a `messaging.process.duration` measurement after settlement. Unrecovered processing failures, including ones durably dead-lettered or otherwise handled by the failure pipeline, mark the duration with `error.type = "_OTHER"`. Actual exception objects are retained in logs. This bounded fallback avoids runtime type lookups and message-derived metric dimensions. Delivery-token cancellation still counts as a delivery and records duration, but does not add `error.type`. If handling already failed and no retry recovered before cancellation, the duration retains that earlier failure. Cancellation before receive adaptation starts is not a delivery.

Other execution components use the same recorder methods: `RecordSent` counts messages attempted at the broker, including failed attempts, but never an outbox enqueue. `RecordDuration` selects the process histogram only for `OperationName = "process"`; other operations use the client-operation histogram. Durations and dispatch lag use seconds. Outbox dispatch, retry, inbox, circuit and transport implementations call the corresponding recorder only when the actual action occurs. The current core exposes those recording seams; it does not infer retries or dead-letter success from a classifier decision. `RecordConsumerLag` accepts the native client's lag calculation. Circuit values are 0 closed, 1 open and 2 half-open.

Each recorder retains precomputed `TagList` values, including its failure tags. Tags include only declared system, operation, destination, group, subscription and broker address/port. Do not create recorders per message or use dynamic destination names, message IDs or keys. Both convention histograms publish the [pinned conventions' recommended bucket advice](https://github.com/open-telemetry/semantic-conventions/blob/v1.44.0/docs/messaging/messaging-metrics.md).

The consumer reads the transport binding's CloudEvents time header only when `relayer.critical_time` has a listener. Invalid or missing timestamps produce no critical-time measurement. With logging, metrics and tracing disabled, optional headers remain unread and telemetry adds no per-delivery allocations.

Generated error logs use category `Relayer.ConsumePipeline` and event ID 1. When Error logging is enabled for that category, each generated failure log opens a scope carrying `MessageId`, the stable wire `MessageType`, and `CorrelationId` from the optional CloudEvents `correlationid` extension (`ce_correlationid` or `cloudEvents:correlationid`, resolved through `HeaderNames.CorrelationId`). Failures in handling, failure handling, disposal and settlement receive this scope. It surrounds only the generated log call; application and failure-handler logs retain the application's ambient scopes. Successful deliveries open no Relayer logging scope, because `LoggerFactory` allocates when pushing an external scope. Cancellation with the delivery token canceled propagates without an Error log, while unrelated cancellation failures are logged normally. Failures log the exception and the ambient activity's `TraceId` and `SpanId`; no activity is created by this metrics/logging integration. Raw malformed envelope fields remain loggable without bypassing the failure classifier. Producers opt into correlation by writing the extension through their native header writer using `names.CorrelationId`; the envelope writer does not invent a correlation ID. Native adapters expose that header through their lazy binding, including messages from non-Relayer producers. For example, after writing the envelope:

```csharp
NativeHeaderWriter.SetHeader(nativeMessage, names.CorrelationId, correlationId);
```

Scope identities are captured in a read-only snapshot only when a generated failure log is emitted. Successful deliveries do not format identities or read optional correlation headers merely because Error logging is enabled. Typed deliveries reuse the precomputed wire type string; native malformed fields remain available without envelope parsing. A provider can retain the scope snapshot for asynchronous export after the context is cleared or reused. Snapshot, scope-push and enumeration allocations occur only on the failure path. Exceptions from logging providers are contained so they cannot replace the delivery error or bypass durable failure handling.

## Delivery and settlement tracing

Register the DI-owned `MessageTracing` source with `AddRelayer(r => r.Telemetry())`, then subscribe with your OpenTelemetry provider's `AddSource(RelayerTelemetry.ActivitySourceName)`. The source is named `Relayer` and publishes `RelayerTelemetry.SchemaUrl`. Configure `TracingOptions` through the Options pattern; setting `CreationContextAsParent = false` disables creation-context parenting, and `IncludeBodySize = true` records serialized body sizes.

Compose tracing once per consumer, alongside the metrics recorder:

```csharp
var tracing = provider.GetRequiredService<MessageTracing>();
var endpoint = new MessagingEndpoint
{
    System = "servicebus",
    Destination = "orders",
    Subscription = "billing",
    ServerAddress = "example.servicebus.windows.net",
};
// Wrap each actual native call separately, using its operation name.
var complete = new MessageOperationTracing(tracing, endpoint,
        MessagingOperation.Settle, "complete")
    .WrapSettlement(CompleteNativeMessageAsync);

var consumer = new ConsumePipelineBuilder(scopeFactory, messageTypes,
        serializer, HandleFailureDurablyAsync)
    .Handler(orderHandlerDescriptor)
    .Telemetry(process)
    .Tracing(tracing, endpoint)
    .Settle(context => context.Features.GetRequired<NativeMessage<BrokerDelivery>>().Disposition.Kind switch
    {
        DispositionKind.Complete => complete(context),
        DispositionKind.SettledByHandler => ValueTask.CompletedTask,
        _ => throw new NotSupportedException("Compose wrappers for native redelivery and dead-letter operations."),
    })
    .Build();
```

`BrokerDelivery` and `CompleteNativeMessageAsync` stand for the adapter's native delivery type and completion callback. This example handles completion only; an adapter must compose and select wrappers for every native settlement disposition it supports before starting consumers.

The `process orders` consumer span covers middleware, deserialization, handlers, immediate retries, async scope disposal, buffered send flushing, durable failure handling and the settlement callback. A successful immediate retry clears earlier failures; an unrecovered error sets status `Error` and `error.type = "_OTHER"`. Delivery-token cancellation alone does not mark failure. Exceptions are logged with trace and span IDs, without exception span events. Tracing also enables the same generated failure logs when no metrics recorder is attached, using the source owner's optional `ILoggerFactory`.

Each delivery links to the immutable W3C creation context in its native headers. With no ambient activity, that context is also the parent by default. An ambient activity always wins. Invalid or absent context produces no creation link. Native redelivery starts a new process span; immediate retries remain in the same span. Relayer creates no `receive` spans.

`WrapSettlement` caches a delegate that surrounds exactly one native settlement call, with a client span named `complete orders`, `ack orders`, `dead_letter orders`, or the adapter's other native operation. Use that delegate both in the core settlement callback and in typed manual-settlement extensions. Manual settlement must update the native disposition to `SettledByHandler` only after success; the core callback must skip that disposition. The wrapper preserves native semantics and does not update dispositions itself. A failed native call logs its exception under the settlement span, even if a handler catches it. Propagation through the pipeline does not duplicate that log. Amortized Kafka commits stay outside individual process spans; start the settle primitive with `BatchCount`, without per-message links.

`MessagingEndpoint` precomputes names and constant sampling attributes. Native `IMessageBinding<TIncoming>` implementations can supply a cached partition string with `GetPartitionId`; samplers receive it and creation links at span start. `EnrichActivity` adds transport-specific attributes such as Kafka key, offset and tombstone or Service Bus delivery count, only when all activity data is requested. Message ID, CloudEvents wire type/source and correlation ID are also read only in that state. Body size is opt-in. Registered `IActivityEnricher` services run after these attributes, only for recorded spans.

With no listener, the integration does not read tracing headers, partition IDs or native attributes and adds no allocations over the same receive/DI work. A listener that drops spans still reads creation context and partition for sampling but skips recorded-only metadata. Run `ConsumeTracingBenchmarks` to compare identical native adaptation and DI scopes with tracing disabled, dropped, or recorded; Microsoft DI's own scope allocations remain visible in both cases. These local measurements do not establish the throughput budget against a real broker.

For single-message consumers, `ReceivePipeline.ProcessBatchAsync` invokes each delivery separately and creates one process span per delivery. A consumer composed with `BuildBatch` instead creates one process span per batch, using the same `.Tracing(tracing, endpoint)` configuration. The batch span has one creation link per message, `messaging.batch.message_count`, and only ambient parenting, even for a one-message batch. It omits per-message and partition attributes. Immediate retries stay inside that span, and a fully recovered batch clears earlier error status. A cancellation while failed items remain retains failure status.

`BuildBatch` also supports `.Settle(callback)`: after all durable batch work and scope disposal succeed, it invokes the callback for each context inside the batch process span. Failure dispositions are already applied to native features. Use the same native wrappers and honor `SettledByHandler`; successful manual settlement takes precedence over an earlier failure disposition in the returned decisions. A settlement error propagates without retrying batch handling. Single-message metric recorders remain unsupported on batch composition; record batch metrics around the call. The existing `BuildBatch(..., activitySource)` entry point remains available for custom source composition; do not combine it with `.Tracing`, which supplies the shared source and complete endpoint metadata.

See [Health checks](health-checks.md) for readiness integration. Native broker adapters and outbox instrumentation remain planned; see the [roadmap](../roadmap.md).
