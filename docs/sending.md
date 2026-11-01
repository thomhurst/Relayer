# Send transport contract

`ITransport.SendAsync(OutgoingMessage, CancellationToken)` and `SendBatchAsync(ReadOnlyMemory<OutgoingMessage>, CancellationToken)` are the common native send seam. Routing chooses the named transport before invoking these methods. The same shape serves direct sends, outbox dispatch, scheduled copies, and dead-letter copies. Transport adapters preserve native delivery, ordering, and batching semantics; batch completion does not promise atomic delivery.

The core serializes the body and resolves headers, ordering, and scheduling before invoking a transport. `OutgoingMessage.Headers` is borrowed memory containing named `WireString` values. It includes the stable CloudEvents attributes and creation context written using the [envelope primitives](envelopes.md). Each name is already mapped to the selected transport's binary-mode binding. An adapter maps those values to its native header/property representation without rebuilding the envelope or changing the original creation context.

```csharp
// Body and headers are already serialized/resolved by the core.
var outgoing = new OutgoingMessage(
    destination: "orders.placed",
    body: body,
    headers: headers,
    orderingKey: customerId,
    deliverAt: scheduledTime,
    options: sendOptions);
await transport.SendAsync(outgoing, cancellationToken);
```

The body, header entries, header value buffers, batch array, and native options must remain alive and unchanged until the send completes, including faulted or cancelled completion. A transport must finish using that memory before completion; it cannot return early while a native operation still uses the buffers. The caller can then return pooled storage. An outbox copies the body and headers into durable storage before enlistment completes; dispatch reconstructs the same send shape from the persisted fields. The transport name belongs to routing/outbox storage rather than the message passed to an already selected transport.

`options` carries only the native callbacks at this seam. The caller has already resolved `SendOptions.Headers`, `OrderingKey`, and `DeliverAt` into the other arguments. Options are borrowed, so callers must not mutate them during a send.

A transport constructs its outgoing native object, maps body/headers/ordering/scheduling, then calls `outgoing.ApplyNative(native)` immediately before handing it to the native client. `ApplyNative<TNative>` uses the transport's declared native outgoing type. Callbacks accepting that type or a base type run in registration order. An incompatible callback throws an actionable error instead of being dropped. Callbacks are stored directly, without wrapping closures, reflection, or dynamic invocation.

Before persisting or enlisting a message, outbox integrations call `outgoing.ValidateForOutbox()`. A send containing native callbacks fails because delegates cannot be persisted; portable headers, ordering, and scheduling remain available. Detecting an active transaction and choosing enlistment instead of direct sending belongs to the scoped sender/outbox integration.

This contract does not implement routing, serialization, outbox storage, timeout policy, or native transport adapters. Those components build on this seam. Adapters must observe the cancellation token supplied by the caller.
