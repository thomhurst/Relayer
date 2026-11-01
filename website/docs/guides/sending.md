---
title: Sending messages
description: "Send messages with ISender: routing, per-send options, middleware, timeouts and the outbox."
---

`ISender` is scoped. Register message identities, an `ITransport` service for each named transport, and one route per message type:

```csharp
services.AddSingleton<ITransport>(myTransport); // Name == "events"
services.AddRelayer(relayer =>
{
    relayer.AddMessageTypesFromThisAssembly();
    relayer.AddTransport<BrokerOptions, NativeRouteOptions, NativeConsumerOptions>("events", broker =>
        broker.Send<OrderPlaced>("orders", route =>
        {
            route.OrderingKey(static message => message.CustomerId);
            route.SendOptions = new SendRouteOptions
            {
                Source = "/orders",
                Timeout = TimeSpan.FromSeconds(30)
            };
        }));
});

await sender.SendAsync(new OrderPlaced(customerId), cancellationToken);
```

This uses the existing [typed configuration declarations](../guides/configuration.md). Native registration extensions wrap this seam with their own option types. Native options remain unchanged; portable send settings live on `RouteBuilder.SendOptions`.

When runtime `ITransport` services are registered, route composition resolves message identities, named transports, and destination capabilities once at host startup. Unknown transports, duplicate names, duplicate routes, missing message identities, nonpositive or infinite timeouts, and ordering-key declarations on endpoints without `OrderedByKey` support fail at startup. Declaration-only hosts remain usable without native adapters; resolving `ISender` still validates its routes. A send without a route fails with an actionable error. Routing uses the generic message type passed to `SendAsync<TMessage>`, so send the concrete contract type rather than `object`.

String ordering selectors remain strings. Scalar selectors constrained to `IUtf8SpanFormattable` format into pooled UTF-8 storage without boxing or allocating a string. `OutgoingMessage.OrderingKey` carries either representation as a nullable `WireString`. A native adapter uses the original text or bytes directly, or converts at its required native boundary.

Use per-send options to add native headers, override the route's ordering key, schedule delivery, or configure the native outgoing message:

```csharp
await sender.SendAsync(message, options =>
{
    options.Headers["tenant"] = tenantId;
    options.OrderingKey = customerId;
    options.DeliverAt = deliverAt;
    options.ConfigureNative<MyNativeMessage>(static native => native.Priority = 5);
}, cancellationToken);
```

The core serializes the body and writes the [CloudEvents envelope](../transports/envelopes.md). Set `SendRouteOptions.HeaderNames` to the transport's binary-mode binding. Per-send headers cannot override the envelope identity or creation context. `MessageEnvelopeMode.Raw` omits the envelope and allows explicit legacy headers. The serializer's content type is supplied in `content-type`; the transport maps it to its native property as needed. The transport applies native callbacks immediately before sending and retains its native scheduling and ordering semantics.

Per-send ordering and scheduling options, including changes made by middleware, are checked before serialization, transport send, or outbox enlistment. An ordering-key override requires an endpoint with `OrderedByKey` support, even when using the outbox. A direct send with `DeliverAt` requires native scheduling (`MaxDeliveryDelay` is not null), and its remaining delay must fit that limit. Elapsed timestamps pass through on scheduling-capable endpoints. An active outbox transaction preserves `DeliverAt` for deferred dispatch regardless of the native delay limit. Without an active transaction, unsupported scheduling fails with an actionable error; `SendNowAsync` always validates the native delay because it bypasses the outbox.

Send middleware implements `ISendMiddleware` and calls `next(context)` to continue. Register it with `UseSendMiddleware<TMiddleware>()` in execution order. The middleware chain is composed once; middleware instances are singletons, must be thread-safe, and cannot depend on scoped services. The send context, options, body, and headers are borrowed until the operation completes; do not retain them. Reading the object-valued `SendContext.Message` boxes value-type contracts, and reading the string-valued `MessageId` formats a string on demand. Neither occurs on the default send path.

Return or await the `ValueTask` from `next`; do not run it in the background. A middleware retry may call `next` again after the previous attempt completes. Retries preserve the message ID and creation context. Concurrent calls to `next` for the same context are unsupported.

Every send has a finite timeout, covering middleware, serialization, and transport send or outbox enlistment. The linked cancellation token is available through `SendContext.CancellationToken` and is passed to the transport or outbox. Those implementations must observe cancellation and complete their operation before returning, as required by the transport SPI's borrowed-memory contract. Cancellation is cooperative: an operation that completes successfully remains successful even if cancellation or the timeout fires before the sender observes completion. When an operation reports cancellation, expiration surfaces as `TimeoutException`; caller cancellation remains `OperationCanceledException`. A timeout does not prove that the broker did not accept the message.

Outbox integrations register a scoped `IOutboxEnlistment`. When `IsActive` is true, `SendAsync` enlists the serialized message instead of sending it. Enlistment must copy borrowed body and headers into the active transaction before completing. Native callbacks cannot be persisted and fail before enlistment. `SendNowAsync` bypasses the outbox; `SendViaOutboxAsync` requires an active transaction. These explicit methods support the same configuration and cancellation arguments as `SendAsync`.

The pipeline pools contexts, serialization buffers, envelope storage, and timeout sources. After warmup, the default direct path allocates no managed memory with no tracing listener. Caller code, middleware, serializer implementations and native clients can add their own allocations.
