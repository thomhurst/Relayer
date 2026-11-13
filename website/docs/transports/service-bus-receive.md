---
title: Service Bus receive primitives
description: "Use the native Service Bus callback with Relayer's receive pipeline and manual settlement view."
---

`Relayer.AzureServiceBus` provides `ServiceBusMessageBinding` for `ProcessMessageEventArgs`, `ServiceBusSessionMessageBinding` for `ProcessSessionMessageEventArgs`, and a borrowed `context.ServiceBus()` view. These are low-level building blocks for applications that already own an Azure SDK processor. Most applications can use [named native processors](service-bus.md#register-receiving), which compose failure handling, settlement, telemetry and lifecycle automatically.

Compose a [consume pipeline](consume-pipeline.md) with generated handlers, serialization and an explicit durable failure delegate. Then adapt your existing native processor:

```csharp
using Relayer.Pipeline;
using Relayer.AzureServiceBus;
using Relayer.Transports;

// consumerPipeline is the composed ConsumePipeline, built once at startup.
var receive = new ReceivePipeline<ProcessMessageEventArgs, ServiceBusMessageBinding>(
    consumerPipeline, new HeaderNames("cloudEvents:"), contextPoolCapacity: 8);

await using var processor = client.CreateProcessor("orders", new ServiceBusProcessorOptions
{
    AutoCompleteMessages = false,
    MaxConcurrentCalls = 8,
});

processor.ProcessMessageAsync += async args =>
{
    var disposition = await receive.ProcessAsync(args, args.CancellationToken);
    switch (disposition.Kind)
    {
        case DispositionKind.Complete:
            await args.CompleteMessageAsync(args.Message, args.CancellationToken);
            break;
        case DispositionKind.SettledByHandler:
            break;
        default:
            // Map Redeliver to native abandon and DeadLetter to native dead-lettering.
            // A delayed redelivery requires a durable core fallback before completion.
            throw new NotSupportedException("Configure native failure settlement for this decision.");
    }
};
// Also register ProcessErrorAsync, then start/stop the processor with the application.
```

The application owns processor lifecycle, failure-policy composition, native automatic settlement and telemetry in this low-level integration. The snippet shows the success path; use the [native receive contract](receive.md) to handle every configured failure disposition. Keep `AutoCompleteMessages` disabled, and honor `SettledByHandler` so the SDK does not complete a manually deferred or abandoned message again. Settle only after the handler's durable work finishes.

Inside a handler, native access keeps the original callback and message:

```csharp
var native = context.ServiceBus();
var sequenceNumber = native.Message.SequenceNumber;
// Durably store sequenceNumber before deferring so the application can retrieve it later.
await native.DeferAsync();
// Retrieve it later with ServiceBusReceiver.ReceiveDeferredMessageAsync.
```

`CompleteAsync()`, `AbandonAsync(propertiesToModify)`, `DeferAsync(propertiesToModify)` and `DeadLetterAsync(reason, description, propertiesToModify)` call the native SDK and use `context.CancellationToken`. Property dictionaries pass through unchanged except session dead-lettering: that SDK overload requires a concrete `Dictionary<string, object>`, so other `IDictionary` implementations are copied on this error path. A concrete dictionary retains its identity. The view marks `SettledByHandler` only after the SDK call succeeds. Failure or cancellation propagates without that mark. A second helper settlement after success fails early. The view does not serialize concurrent settlement attempts: await each operation and settle each delivery once.

`Callback` exposes `ProcessMessageEventArgs` for other native operations, including lock renewal. For session deliveries, `SessionCallback` exposes the unchanged `ProcessSessionMessageEventArgs`, including session state and session lock operations; `Callback` fails explicitly on a session delivery. The common `Message` and settlement helpers work with either callback kind. If you settle directly through a callback, set the corresponding `NativeMessage<ProcessMessageEventArgs>` or `NativeMessage<ProcessSessionMessageEventArgs>` disposition to `Disposition.SettledByHandler` after success. The view, callback, message, body and header storage are borrowed for the current processing call; never retain them after it returns.

`ServiceBusMessageBinding` reads only the requested header. The logical `content-type` header comes from the native `ServiceBusReceivedMessage.ContentType` property, as required by the CloudEvents AMQP binding. When the native property is absent, the binding falls back to an application property named `content-type`; when both exist, the native property wins. Other headers come from AMQP application properties. Text stays text; binary values stay borrowed UTF-8 memory. Missing properties and other native value types return `false`; access numeric and other typed values through `native.Message.ApplicationProperties`. CloudEvents attributes use the `cloudEvents:` prefix; creation context remains in `traceparent` and `tracestate`. No SDK tracing header is substituted for the creation context.

For an indexed single AMQP data section, the binding borrows the original body memory and creates no allocation. Other body layouts use the SDK's `Body` conversion, preserving native behavior; that conversion may allocate and join multiple sections. Header reads use the raw AMQP property dictionary to avoid the SDK's per-access read-only dictionary wrapper.
