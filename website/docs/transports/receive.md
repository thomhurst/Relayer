---
title: Receiving messages
description: "Adapt a composed consume pipeline to a native receive callback with a struct binding, and settle from the returned disposition."
---

Compose a `ConsumePipeline` once with generated handler descriptors, the serializer, middleware, and an explicit durable failure delegate. Adapt it to the native callback with a struct binding:

```csharp
public readonly struct BrokerBinding : IMessageBinding<BrokerDelivery>
{
    public static ReadOnlySequence<byte> GetBody(BrokerDelivery message) => message.Body;
    public static int? GetDeliveryCount(BrokerDelivery message) => message.DeliveryCount;
    public static bool TryGetHeader(BrokerDelivery message, string name, out WireString value)
        => message.TryGetHeader(name, out value);
}

// Once at startup. Match both pool capacities to the consumer's concurrency.
var receive = new ReceivePipeline<BrokerDelivery, BrokerBinding>(
    consumerPipeline, new HeaderNames("ce_"), contextPoolCapacity: 8);

// In the native callback. The adapter owns settlement and any native property mapping.
var disposition = await receive.ProcessAsync(delivery, cancellationToken).ConfigureAwait(false);
await SettleNativelyAsync(delivery, disposition, cancellationToken).ConfigureAwait(false);
```

`BrokerDelivery` and `SettleNativelyAsync` stand for the native client's delivery type and settlement calls. The binding passes the original delivery and borrowed body through unchanged. It inherits `IMessageHeaderReader<TIncoming>`; no separate header model or reader is needed. A binding returns a delivery count starting at one, or `null` when unavailable. The latter gives `MessageContext.Attempt` a value of one.

The CloudEvents wire type is resolved against the consumer's precomputed identities without decoding known UTF-8 types. Unknown identities, malformed types, and missing IDs enter the consumer's failure delegate as permanent failures. Required identity headers are checked without decoding. `MessageContext.Headers.TryGetValue` returns a `WireString` on demand, through the existing `MessageHeaders<TIncoming,TBinding>` reader. It does not enumerate, copy, or decode header values. `MessageId` converts the ID only on first access; asking for a UTF-8 ID as a string necessarily allocates that string.

Handlers and transport extensions access the untouched native delivery through `context.Features.GetRequired<NativeMessage<BrokerDelivery>>().Message`. The feature, header view, and context are borrowed for processing only. The receive adapter clears its body, headers, native delivery, and cached metadata on success, failure, and cancellation. Pools are bounded; concurrency above their configured capacities creates temporary overflow objects rather than retaining an unbounded number of deliveries.

The native feature's `Disposition` starts as `Complete`. A failure delegate sets it only after durable failure handling finishes. It selects `Redeliver(delay)` or `DeadLettered(info)` only after resolving the endpoint's capabilities; if a fallback send completes durably, it selects `Complete`. This receive seam does not implement failure policies, capability resolution, or broker fallbacks. A transport extension that successfully settles through the native client sets `SettledByHandler`, preventing the adapter from settling twice. A returned dead-letter decision retains its error metadata for native settlement; the caller must keep its header storage valid through that operation.

`ProcessAsync` returns only after handler work, failure handling, and async scope disposal succeed. Exceptions and cancellation propagate without returning a disposition. The native receive loop applies its ordinary failure behavior in that case.

`ProcessBatchAsync(ReadOnlyMemory<TIncoming>, Memory<Disposition>, cancellationToken)` processes deliveries in input order and writes one decision per message. It adapts a native receive batch to single-message consume dispatch; it does not compose an `IBatchHandler<T>`. Output must have at least as many slots as input; an unused tail stays unchanged. Settle from the output only after the entire call succeeds. A failed call may have completed durable work for earlier messages, so redelivery remains at-least-once. Input and output storage must remain valid and unchanged until the call completes.
