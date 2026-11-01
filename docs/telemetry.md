# Metrics and logging

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

The consumer reads the transport binding's CloudEvents time header only when `relayer.critical_time` has a listener. Invalid or missing timestamps produce no critical-time measurement. With logging and metrics disabled, optional headers remain unread and telemetry adds no per-delivery allocations.

Generated error logs use category `Relayer.ConsumePipeline` and event ID 1. When Error logging is enabled for that category, each generated failure log opens a scope carrying `MessageId`, the stable wire `MessageType`, and `CorrelationId` from the optional CloudEvents `correlationid` extension (`ce_correlationid` or `cloudEvents:correlationid`, resolved through `HeaderNames.CorrelationId`). Failures in handling, failure handling, disposal and settlement receive this scope. It surrounds only the generated log call; application and failure-handler logs retain the application's ambient scopes. Successful deliveries open no Relayer logging scope, because `LoggerFactory` allocates when pushing an external scope. Cancellation with the delivery token canceled propagates without an Error log, while unrelated cancellation failures are logged normally. Failures log the exception and the ambient activity's `TraceId` and `SpanId`; no activity is created by this metrics/logging integration. Raw malformed envelope fields remain loggable without bypassing the failure classifier. Producers opt into correlation by writing the extension through their native header writer using `names.CorrelationId`; the envelope writer does not invent a correlation ID. Native adapters expose that header through their lazy binding, including messages from non-Relayer producers. For example, after writing the envelope:

```csharp
NativeHeaderWriter.SetHeader(nativeMessage, names.CorrelationId, correlationId);
```

Scope identities are captured in a read-only snapshot only when a generated failure log is emitted. Successful deliveries do not format identities or read optional correlation headers merely because Error logging is enabled. Typed deliveries reuse the precomputed wire type string; native malformed fields remain available without envelope parsing. A provider can retain the scope snapshot for asynchronous export after the context is cleared or reused. Snapshot, scope-push and enumeration allocations occur only on the failure path. Exceptions from logging providers are contained so they cannot replace the delivery error or bypass durable failure handling.

Health integration is tracked separately in #85; tracing in #13. See [design §8](design.md#8-observability) for their contracts.
