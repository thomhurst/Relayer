---
title: Service Bus receive primitives
description: "Use the native Service Bus callback with Relayer's receive pipeline and manual settlement view."
---

`Relayer.AzureServiceBus` provides a `ServiceBusBinding` for `ProcessMessageEventArgs` and a borrowed `context.ServiceBus()` view. These are low-level building blocks for applications that already own an Azure SDK processor. [Named sending, client ownership and topology](service-bus.md) are implemented independently. Complete receiver composition, session processor bindings and health adapters remain planned in [#130](https://github.com/thomhurst/Relayer/issues/130).

Compose a [consume pipeline](consume-pipeline.md) with generated handlers, serialization and an explicit durable failure delegate. Then adapt your existing native processor:

```csharp
using Relayer.AzureServiceBus;
using Relayer.Transports;

// consumerPipeline is the composed ConsumePipeline, built once at startup.
var receive = new ReceivePipeline<ProcessMessageEventArgs, ServiceBusBinding>(
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

`CompleteAsync()`, `AbandonAsync(propertiesToModify)`, `DeferAsync(propertiesToModify)` and `DeadLetterAsync(reason, description, propertiesToModify)` call the native SDK and use `context.CancellationToken`. Native property dictionaries are passed unchanged. The view marks `SettledByHandler` only after the SDK call succeeds. Failure or cancellation propagates without that mark. A second helper settlement after success fails early. The view does not serialize concurrent settlement attempts: await each operation and settle each delivery once.

`Callback` exposes `ProcessMessageEventArgs` for other native operations, including lock renewal. If you settle through `Callback` yourself, set `context.Features.GetRequired<NativeMessage<ProcessMessageEventArgs>>().Disposition` to `Disposition.SettledByHandler` after success. The view, callback, message, body and header storage are borrowed for the current processing call; never retain them after it returns. This binding is for ordinary processor callbacks; session processor callbacks are separate planned work.

`ServiceBusBinding` reads only the requested AMQP application property. Text stays text; binary values stay borrowed UTF-8 memory. Missing properties and other native value types return `false`; access numeric and other typed values through `native.Message.ApplicationProperties`. CloudEvents attributes use the `cloudEvents:` prefix; creation context remains in `traceparent` and `tracestate`. No SDK tracing header is substituted for the creation context.

For an indexed single AMQP data section, the binding borrows the original body memory and creates no allocation. Other body layouts use the SDK's `Body` conversion, preserving native behavior; that conversion may allocate and join multiple sections. Header reads use the raw AMQP property dictionary to avoid the SDK's per-access read-only dictionary wrapper.
