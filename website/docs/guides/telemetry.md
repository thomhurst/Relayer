---
title: Tracing, metrics and logging
description: Send, outbox, delivery and settlement spans, messaging metrics, and generated failure logs.
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

## Tracing registration and primitives

`AddRelayer` automatically registers one `MessageTracing` source owner per service provider. Subscribe with your OpenTelemetry provider's `AddSource(RelayerTelemetry.ActivitySourceName)`. Repeated registration shares the source within that provider; separate providers own separate sources named `Relayer`:

```csharp
services.AddRelayer(relayer => { /* Declare transports and handlers here. */ });
services.AddOptions<TracingOptions>().Bind(configuration.GetSection("Relayer:Tracing"));
services.AddSingleton<IActivityEnricher, ApplicationEnricher>();
```

`ApplicationEnricher` is your implementation of `IActivityEnricher`. `TracingOptions` uses init-only properties. `CreationContextAsParent` and `CreateSpansForBatches` default to true; `IncludeBodySize` defaults to false. The source is named `Relayer` and carries schema URL `https://opentelemetry.io/schemas/1.44.0`. Tracing and metrics share the single `RelayerTelemetry.SemanticConventionsVersion` pin. The source follows [semantic conventions 1.44.0](https://github.com/open-telemetry/semantic-conventions/blob/v1.44.0/docs/messaging/messaging-spans.md).

Changes to the shared conventions pin require a release note and a mapping review.

For the built-in integrations, see [send tracing](sending.md#send-tracing), [delivery and settlement tracing](#delivery-and-settlement-tracing), and [outbox tracing and metrics](#outbox-tracing-and-metrics). The primitives below support custom transport composition.

Adapters compose operations once at startup, using native metadata rather than the configured transport instance name:

```csharp
var tracing = provider.GetRequiredService<MessageTracing>();
var endpoint = new MessagingEndpoint
{
    System = "kafka",
    Destination = "orders",
    ConsumerGroup = "billing",
    ServerAddress = "broker",
    ServerPort = 9092,
};
var process = new MessageOperationTracing(tracing, endpoint, MessagingOperation.Process);
var commit = new MessageOperationTracing(tracing, endpoint, MessagingOperation.Settle, "commit");
```

`Start(in MessageTraceData)` returns an `Activity` to dispose after the operation finishes, or null when no span is created. It validates batch metadata independently of listeners, then returns null before tag or link work when no listener is attached. Batch counts must be positive, and `Create` never accepts batch metadata, even for an existing creation context. Single processing links to `MessageTraceData.CreationContext`; absent an ambient activity, that context is also the parent unless disabled. An ambient activity always takes precedence. Set `BatchCount` for batch operations and supply one `ActivityLink` per message in `Links`. Batch processing uses links only and never chooses a message as parent. Batch spans omit single-message attributes, even when a batch contains one message.

Prefer the existing [send tracing seam](../transports/authoring.md#send-tracing) for custom adapters. For primitive batch sending, check `tracing.Options.CreateSpansForBatches`. When true, start and dispose a `Create` operation per message that has no creation context, then start a batch `Send` operation with links to those contexts. When false, skip the create operations; a send without existing creation contexts or links is a producer, and its context becomes the creation context for every new message. A send with an existing creation context or links is a client span. Settle operations are client spans with the native operation name; start them while processing is current so settlement remains inside the process duration.

Use `MessageCreationContext.Read<TMessage, TReader>(message, names)` with the adapter's header binding to read remote W3C context. UTF-8 values are decoded on demand. `WriteIfAbsent<TMessage, TBinding>(message, names, context)` writes only when traceparent is absent, preserving external, resend, and dead-letter headers byte for byte, even when invalid. The binding implements both `IMessageHeaderReader<TMessage>` and `IMessageHeaderWriter<TMessage>`. Preserve the chosen context through retries and use it when writing the envelope. The native client's instrumentation must also preserve it. The tracing source is not needed for propagation.

Call `MessageOperationTracing.Fail(activity, errorType)` on failure. Pass a native error code, a cached exception type name, or `_OTHER` (also used for missing or whitespace error types); this avoids reflection on the message path. Log the exception with its trace IDs in the owning pipeline; the helper emits no exception event. Enrichers run only for recorded spans, must support concurrency, and should not throw. Enricher exceptions are ignored so diagnostics do not interrupt delivery. They can add native per-message attributes or rename spans. Supply partition IDs in `MessageTraceData` so samplers see them at span start.

`TracingBenchmarks` measures these primitives with no listener, dropped spans and recorded spans; `CreationContextBenchmarks` measures header decoding separately. These do not measure native broker overhead or establish the throughput budget against a real broker. `SendTracingBenchmarks` and `ConsumeTracingBenchmarks` cover pipeline integration. Tracing tests run in a dedicated executable because TUnit's HTML reporter attaches a listener to every activity source, which would change sampling and invalidate no-listener measurements.

## Delivery and settlement tracing

The [provider-owned tracing source](#tracing-registration-and-primitives) supplies delivery and settlement spans; setting `CreationContextAsParent = false` disables creation-context parenting, and `IncludeBodySize = true` records serialized body sizes.

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

Standalone `new MessageTracing(...)` callers pass an `ILoggerFactory` to enable generated tracing failure logs when no metrics recorder supplies a logger. Omitting it uses a null logger.

The `process orders` consumer span covers middleware, deserialization, handlers, immediate retries, async scope disposal, buffered send flushing, durable failure handling and the settlement callback. A successful immediate retry clears earlier failures; an unrecovered error sets status `Error` and `error.type = "_OTHER"`. Delivery-token cancellation alone does not mark failure. Exceptions are logged with trace and span IDs, without exception span events. Tracing also enables the same generated failure logs when no metrics recorder is attached, using the source owner's optional `ILoggerFactory`.

Each delivery links to the immutable W3C creation context in its native headers. With no ambient activity, that context is also the parent by default. An ambient activity always wins. Invalid or absent context produces no creation link. Native redelivery starts a new process span; immediate retries remain in the same span. Relayer creates no `receive` spans.

`WrapSettlement` caches a delegate that surrounds exactly one native settlement call, with a client span named `complete orders`, `ack orders`, `dead_letter orders`, or the adapter's other native operation. Use that delegate both in the core settlement callback and in typed manual-settlement extensions. Manual settlement must update the native disposition to `SettledByHandler` only after success; the core callback must skip that disposition. The wrapper preserves native semantics and does not update dispositions itself. A failed native call logs its exception under the settlement span, even if a handler catches it. Propagation through the pipeline does not duplicate that log. Amortized Kafka commits stay outside individual process spans; start the settle primitive with `BatchCount`, without per-message links.

`MessagingEndpoint` precomputes names and constant sampling attributes. Native `IMessageBinding<TIncoming>` implementations can supply a cached partition string with `GetPartitionId`; samplers receive it and creation links at span start. `EnrichActivity` adds transport-specific attributes such as Kafka key, offset and tombstone or Service Bus delivery count, only when all activity data is requested. Message ID, CloudEvents wire type/source and correlation ID are also read only in that state. Body size is opt-in. Registered `IActivityEnricher` services run after these attributes, only for recorded spans.

With no listener, the integration does not read tracing headers, partition IDs or native attributes and adds no allocations over the same receive/DI work. A listener that drops spans still reads creation context and partition for sampling but skips recorded-only metadata. Run `ConsumeTracingBenchmarks` to compare identical native adaptation and DI scopes with tracing disabled, dropped, or recorded; Microsoft DI's own scope allocations remain visible in both cases. These local measurements do not establish the throughput budget against a real broker.

For single-message consumers, `ReceivePipeline.ProcessBatchAsync` invokes each delivery separately and creates one process span per delivery. A consumer composed with `BuildBatch` instead creates one process span per batch, using the same `.Tracing(tracing, endpoint)` configuration. The batch span has one creation link per message, `messaging.batch.message_count`, and only ambient parenting, even for a one-message batch. It omits per-message and partition attributes. Immediate retries stay inside that span, and a fully recovered batch clears earlier error status. A cancellation while failed items remain retains failure status.

`BuildBatch` also supports `.Settle(callback)`: after all durable batch work and scope disposal succeed, it invokes the callback for each context inside the batch process span. Failure dispositions are already applied to native features. Use the same native wrappers and honor `SettledByHandler`; successful manual settlement takes precedence over an earlier failure disposition in the returned decisions. A settlement error propagates without retrying batch handling. Single-message metric recorders remain unsupported on batch composition; record batch metrics around the call. The existing `BuildBatch(..., activitySource)` entry point remains available for custom source composition; do not combine it with `.Tracing`, which supplies the shared source and complete endpoint metadata.

Hosted in-memory consumers configure delivery tracing automatically. Their native `Complete`, `Redeliver` and `DeadLetter` methods emit settlement spans, including manual calls. Core completion runs after durable processing and scope disposal, and skips a successfully settled delivery. For standalone `InMemoryTransport` instances, pass `tracing:` to the constructor and configure `.Tracing` on the composed consumer. In-memory native methods already trace settlement; do not additionally wrap them with `WrapSettlement`.

Native adapters that log settlement errors themselves implement `IMessageBinding.TryConsumeLoggedSettlementError` to consume a delivery-local marker for the exact exception already logged. This prevents a second pipeline failure log while preserving native logs for errors a handler catches. Clear markers before native state is reused. The in-memory adapter implements this hook; it retains a marker only when a native log was actually emitted.

See [Health checks](health-checks.md#explicit-adapters) for readiness integration with optional native connectivity and receiver-liveness adapters for Kafka, Service Bus, SQS and RabbitMQ. Register each adapter explicitly alongside `AddRelayerHealthChecks`, using the exact transport name declared in `AddRelayer`.

## Outbox tracing and metrics

With `AddRelayer`, sends through the PostgreSQL EF Core or ADO.NET outbox create a
`create {destination}` producer span inside the caller's trace, before serialization and
enlistment. The span's W3C creation context is stored with the row's headers. Enqueue
failures mark this span with status `Error` and `error.type = "_OTHER"`.

Direct dispatch, leased unordered recovery and ordered lane dispatch emit
`send {destination}` client spans through the configured native adapter. A native batch
has one send span with a link to every message's creation context. Single sends link to
that message's creation context. Dispatch never creates a new creation context, including
for legacy rows with missing or invalid trace headers. Existing headers are preserved.
The consumer's process span links to the enqueue span; it does not link to the dispatcher.
With no ambient consumer span, the creation context is also its parent by default.

`messaging.client.sent.messages` counts native send attempts, including dispatch retries,
and never counts enqueue. Adapters count immediately before invoking a native send API,
after validation, native customization and message or batch preparation. A cancelled
call rejected before that boundary, a preparation failure, and an unattempted batch
tail contribute zero. Each Service Bus size/session/partition split counts only its
own messages, including a failed split. Kafka batches without tombstones count the
messages supplied to each native `ProduceAllAsync` call. Batches containing tombstones
count each invoked `ProduceAsync` separately. Retries inside the native client do not
add Relayer attempts.
It uses the adapter's precomputed native system and destination
dimensions. Built-in Kafka, Service Bus and in-memory adapters use this shared send instrumentation.
Standalone `MessageTracing` callers can supply `instrumentation:` to enable the same
send metrics without dependency injection. Custom adapters use `ITransportSendTracing`
and `SendTracing` for the same creation and dispatch behavior. `SendTracing.SendAsync`
and `SendBatchAsync` compose spans without counting sends. Custom adapters call
`SendTracing.RecordNativeSend(count)` at each native send boundary, using the count
actually passed to that call. Do not also call `EndpointTelemetry.RecordSent` for
the same messages.

Kafka and Service Bus also record `messaging.client.operation.duration` in seconds
for each actual native send call, from invocation through acknowledgement or failure.
This includes native retries and works without an activity listener. A homogeneous Kafka
batch records one duration for `ProduceAllAsync`; a Kafka batch containing tombstones
uses individual native sends and records each completion. Mixed destinations are measured
at their native send boundaries. Service Bus records each `SendMessageAsync` or
`SendMessagesAsync` call, including each batch split by size, session or partition.
The logical batch span can cover several native calls, so its duration can differ.

Durations carry `messaging.system`, `messaging.operation.name = "send"` and
`messaging.destination.name`. Service Bus also supplies `server.address` from the native
client's fully qualified namespace. Kafka does not claim a broker address or port before
native routing identifies it. Failed calls, including cancellation returned by the native
client, add `error.type = "_OTHER"`; successful calls omit it. Empty batches, validation
or preparation failures before a native call, and outbox enqueue emit no send duration.
Outbox dispatch uses the same native timing, without a second measurement in the pipeline.

Custom adapters call `SendTracing.StartNativeSend()` immediately before each native send
and call the returned timer's `Complete(failed)` once in `finally`, after native completion.
Set `failed` to `true` initially and clear it only when the native await succeeds. The timer
uses precomputed endpoint tags and reads the clock only when the duration instrument has
a listener. `IsEnabled` lets concurrent adapters avoid a completion wrapper when no
measurement is needed. The timer does not create another meter or span.

The PostgreSQL transaction integrations and dispatch workers also record:

- `relayer.outbox.pending`: an up-down counter of durable row-count changes observed by
  this instance. A successful commit adds its rows; a successful fenced dispatch deletion
  subtracts its rows. Rollback, failed sends, competing claims and failed completion do
  not subtract rows. Ordered deletions count only after their transaction commits.
- `relayer.outbox.dispatch_lag`: seconds from the row's enqueue timestamp to the start
  of each native dispatch attempt. Recovery uses the original durable timestamp. Failed
  native attempts also record lag.

- `relayer.outbox.dead_lettered`: an alert counter incremented after a PostgreSQL poison
  transition commits. Durable dead storage is authoritative if a crash loses the metric.

These store-wide metrics aggregate across stores and transports, without endpoint
or message dimensions. The pending counter is a delta, not a database snapshot: it can
be negative on an instance that recovers rows written elsewhere or before startup.
Use the [outbox backlog health check](health-checks.md) or durable store count to query
current eligible backlog. Neither enqueue nor claim reads emit a sent measurement.
