---
title: Sending messages
description: "Send messages with ISender: routing, per-send options, middleware, timeouts and the outbox."
---

## What has completed when I await a send?

| Situation | Successful completion of `SendAsync` |
| --- | --- |
| Direct send outside a handler | The native send completed according to the transport. For in-memory messaging, the queue accepted the message. |
| Direct send inside a handler | The attempt buffered the message. Relayer flushes after the handler, middleware and scope disposal succeed. A failed attempt discards it. |
| Active outbox transaction or selected EF Core unit of work | The unit enlisted the message. Its database commit makes it durable. Dispatch follows separately. |

With an outbox registered and no active unit of work, `SendAsync` throws. `SendWithoutOutboxAsync` bypasses the outbox and still follows the handler buffering rule.

## Configure a send route

Register a native transport and a route, then inject the scoped `ISender` into your
endpoint or application service. For example, with `Relayer.Kafka`:

```csharp
using Relayer;
using Relayer.Kafka;

services.AddRelayer(relayer =>
{
    relayer.AddMessageTypesFromThisAssembly();
    relayer.Kafka("events", kafka =>
    {
        kafka.TopologyMode = TopologyMode.None; // Provision topics externally.
        kafka.Options.Configure(options => options.BootstrapServers.Add("localhost:9092"));
        kafka.Send<OrderPlaced>("orders", route =>
        {
            route.OrderingKey(static message => message.CustomerId);
            route.SendOptions = new SendRouteOptions
            {
                Source = "/orders",
                Timeout = TimeSpan.FromSeconds(30)
            };
        });
    });
});

// In an application scope, using the injected sender:
await sender.SendAsync(new OrderPlaced(customerId), cancellationToken);

[Message("orders.placed", Version = 1)]
public sealed record OrderPlaced(string CustomerId);
```

The native registration extension owns client and runtime adapter registration. Use your
broker's [transport guide](../transports/index.md) for connection settings and native options.
Portable send settings live on `RouteBuilder.SendOptions`; see [configuration](configuration.md).
Transport authors use the lower-level [registration seam](../transports/authoring.md#registration).
For atomic business changes and outgoing messages, follow the [outbox guide](outbox.md).

When runtime `ITransport` services are registered, route composition resolves message identities, named transports, and destination capabilities once at host startup. Unknown transports, duplicate names, a repeated route to the same destination, missing message identities, nonpositive or infinite timeouts, and ordering-key declarations on endpoints without `OrderedByKey` support fail at startup. Declaration-only hosts remain usable without native adapters; resolving `ISender` still validates its routes. Both hosted startup and standalone `ISender` resolution reject a route-level native callback declared for the wrong transport. A send without a route fails with an actionable error. Routing uses the generic message type passed to `SendAsync<TMessage>`, so send the concrete contract type rather than `object`.

## Sending to several routes

Declare `Send<T>` more than once to send one message to several destinations, on the same transport or on different ones. For example, a domain event can go to a Kafka topic for other services and to a Service Bus queue for a billing worker:

```csharp
relayer.Kafka("events", kafka => kafka.Send<OrderPlaced>("orders",
    route => route.OrderingKey(static message => message.CustomerId)));
relayer.ServiceBus("bus", bus => bus.Send<OrderPlaced>("billing"));

// Inside an active outbox transaction:
await sender.SendAsync(new OrderPlaced(customerId), cancellationToken);
```

Each send enlists one outbox row per route in the same transaction, in declaration order. Every row carries the same envelope identity (`ce_id`) and time, so consumers can deduplicate by message ID. The first row's outbox ID is the message ID; each later row gets its own UUIDv7 outbox ID, which dead-letter tools and dispatch failures report for that row. Each route keeps its own ordering key, capability checks and send span. A per-send `configure` callback runs once for each route. If enlisting a later route fails, the exception reaches the caller before commit; roll back the transaction rather than committing the rows already enlisted.

Sending to several routes needs an active outbox transaction. A direct send cannot be atomic across destinations, so `SendWithoutOutboxAsync`, and `SendAsync` without an active transaction, fail before anything is sent. Request and response types need exactly one route. A repeated route to the same destination fails at startup.

## Choosing the destination per message

A route can declare several native destinations and pick one from each message at send time. This suits one envelope type that carries many logical events, or destinations partitioned by a field such as region:

```csharp
kafka.Send<IntegrationEnvelope>(["orders-eu", "orders-us"], static message => message.Topic);
```

Every declared destination is validated and provisioned at startup like a single-destination route, and the route's `SendOptions` and ordering key apply to each. The selector runs once per send, before middleware and outbox enlistment, so the outbox row stores the selected destination and dispatch is unchanged. A selector that returns a destination the route does not declare fails the send with an `InvalidOperationException`. A selector route counts as one route when the message type also has other routes, so a send through the outbox enlists one row for the selected destination plus one for each other route ([sending to several routes](#sending-to-several-routes)); declaring the same destination in both fails at startup. Selector routes cannot carry request replies or typed tombstones, which need a single-destination route.

String ordering selectors accept nullable strings. A null result leaves the ordering key unset, and an empty string is passed to the adapter as a key. Scalar selectors constrained to `IUtf8SpanFormattable` format into pooled UTF-8 storage without boxing or allocating a string. `OutgoingMessage.OrderingKey` carries either representation as a nullable `WireString`. A native adapter uses the original text or bytes directly, or converts at its required native boundary.

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

Declare known send requirements on the existing route settings:

```csharp
broker.Send<OrderPlaced>("orders", route => route.SendOptions = new()
{
    RequiredDeliveryDelay = TimeSpan.FromDays(1),
    TransactionRequirement = SendTransactionRequirement.Outbox
});
```

`RequiredDeliveryDelay` declares the longest delay the route needs; it does not set a message's `DeliverAt`. Startup accepts an exact native `MaxDeliveryDelay` boundary. A longer delay or an endpoint without native scheduling requires a registered scoped `IOutboxEnlistment` and matching due-row dispatch worker. The send must run within an active outbox transaction when it needs that fallback. Runtime checks still validate actual `DeliverAt` values, including changes made by middleware and schedules that were not declared at startup.

When hosted startup checks outbox capabilities, it awaits disposal of the validation scope. A custom executor or its scoped dependencies can implement `IAsyncDisposable`. If a route needs those capabilities, start the host before resolving `ISender` when the executor or a scoped dependency requires async disposal.

`TransactionRequirement.None` is the default. `Outbox` requires a registered transaction executor at startup and an active transaction for every send on that route. `SendWithoutOutboxAsync` cannot bypass it. `Native` requires the endpoint to advertise native `Transactions`; the application still configures and owns the transaction through the native client. It does not automatically begin or commit a broker transaction. Native transactions cannot be replaced by database outbox durability, and an active outbox is rejected on a native-required route; use `SendWithoutOutboxAsync` with the native transaction configuration. A native-required route also cannot use an outbox scheduling fallback. Current built-in adapters advertise conservative transaction capabilities and reject a native requirement until their native transaction integration supports it.

For PostgreSQL fallback validation, the outbox options and registered worker must agree on schema and table prefix. Ordered workers also need the same lane count. Applications remain responsible for pointing writers and workers at the same database. EF Core startup uses the outbox options registered through the Options pattern; match them to the participating DbContext options. Execution also checks the selected DbContext's actual store, so a per-context override cannot silently use another store's scheduler. Third-party outbox integrations report executable scheduling support through `IOutboxEnlistment.SupportsScheduling` and `SupportsOrderedScheduling`; the defaults are false. These are executor capabilities, not user switches that enable an unimplemented fallback.

The core serializes the body and writes the [CloudEvents envelope](../transports/envelopes.md). Set `SendRouteOptions.HeaderNames` to the transport's binary-mode binding. Per-send headers cannot override the envelope identity or creation context. `MessageEnvelopeMode.Raw` omits the envelope and allows explicit legacy headers. The serializer's content type is supplied in `content-type`; the transport maps it to its native property as needed. The transport applies native callbacks immediately before sending and retains its native scheduling and ordering semantics.

Per-send ordering and scheduling options, including changes made by middleware, are checked before serialization, transport send, or outbox enlistment. An ordering-key override requires an endpoint with `OrderedByKey` support, even when using the outbox. A direct send with `DeliverAt` requires native scheduling (`MaxDeliveryDelay` is not null), and its remaining delay must fit that limit. Elapsed timestamps pass through on scheduling-capable endpoints. An active outbox transaction preserves `DeliverAt` only when a matching due-row dispatcher is registered. Commit-only direct dispatch is insufficient because it skips future rows. PostgreSQL maintenance handles unordered schedules; ordered schedules require the native ordered lane dispatcher with the writer's lane count. These checks apply even when the broker supports native scheduling, because an outbox send persists the schedule rather than invoking that broker immediately. Without an active transaction, unsupported scheduling fails with an actionable error; `SendWithoutOutboxAsync` always validates the native delay because it bypasses the outbox.

Send middleware implements `ISendMiddleware` and calls `next(context)` to continue. Register it with `UseSendMiddleware<TMiddleware>()` in execution order. The middleware chain is composed once; middleware instances are singletons, must be thread-safe, and cannot depend on scoped services. The send context, options, body, and headers are borrowed until the operation completes; do not retain them. Reading the object-valued `SendContext.Message` boxes value-type contracts, and reading the string-valued `MessageId` formats a string on demand. Neither occurs on the default send path.

Return or await the `ValueTask` from `next`; do not run it in the background. A middleware retry may call `next` again after the previous attempt completes. Retries preserve the message ID and creation context. Concurrent calls to `next` for the same context are unsupported.

Every send has a finite timeout, covering middleware, serialization, and transport send or outbox enlistment. The linked cancellation token is available through `SendContext.CancellationToken` and is passed to the transport or outbox. Those implementations must observe cancellation and complete their operation before returning, as required by the transport SPI's borrowed-memory contract. Cancellation is cooperative: an operation that completes successfully remains successful even if cancellation or the timeout fires before the sender observes completion. When an operation reports cancellation, expiration surfaces as `TimeoutException`; caller cancellation remains `OperationCanceledException`. A timeout does not prove that the broker did not accept the message.

Outbox integrations register a scoped `IOutboxEnlistment`. When `IsActive` is true, `SendAsync` enlists the serialized message instead of sending it. When an outbox is registered but inactive, `SendAsync` throws before per-send configuration, middleware, serialization or transport sending. Begin an outbox transaction or select an EF Core pending unit of work with `db.EnlistRelayerOutbox()` first. Without an outbox registration, `SendAsync` sends directly. `TransactionRequirement.None` does not restore the inactive-outbox fallback. See [Upgrade outbox sends](../migration/outbox-sends.md) for existing applications.

Enlistment must copy borrowed body and headers into the active transaction before completing. Per-send native callbacks cannot be persisted and fail before enlistment; a transport's route-level callback, such as [Service Bus `route.ServiceBus(...)`](../transports/service-bus.md#sessions-scheduling-and-native-callbacks), applies on dispatch instead. `SendWithoutOutboxAsync` deliberately bypasses the outbox; `SendViaOutboxAsync` requires a registered outbox and active transaction or pending unit of work. These explicit methods support the same configuration and cancellation arguments as `SendAsync`.

Inside a handler, Relayer buffers sends that do not enlist in the outbox, including `SendWithoutOutboxAsync`. It flushes them after the handler, middleware, and scope disposal succeed. If the attempt fails, Relayer discards the buffered messages. Await each send before returning from the handler.

The pipeline pools contexts, serialization buffers, envelope storage, and timeout sources. Each route retains at most 64 completed send operations; the scoped sender pool also retains at most 64 senders. Sends beyond those limits still proceed normally, allocating temporary state that is discarded on return. Operations with body buffers above 64 KiB, ordering-key buffers above 4 KiB, more than 64 envelope slots, envelope value buffers above 4 KiB, additional-header dictionary capacity above 128, or native callback capacity above 16 are also discarded on return. These are storage retention limits, not message size or concurrency limits.

After warmup, the default direct path allocates no managed memory with no tracing listener while sends fit within these retention limits. Repeated oversized sends or bursts above the pool capacity allocate again rather than permanently retaining peak memory. Caller code, middleware, serializer implementations and native clients can add their own allocations.

## Send tracing

`AddRelayer` registers one provider-owned `ActivitySource` named `Relayer`. Enable that source in your tracing exporter (for example, OpenTelemetry's `AddSource("Relayer")`). The in-memory adapter integrates it automatically; custom adapters use the [send tracing seam](../transports/authoring.md#send-tracing).

An immediate `SendAsync` or `SendWithoutOutboxAsync` creates a `send {destination}` producer span when middleware dispatches the message, before serialization. Its context is written into the message's W3C `traceparent` and `tracestate` headers before the native adapter copies the envelope. The span remains open until middleware and the native send finish, including asynchronous completion. Raw messages with an existing valid creation context use a client span linked to that context and preserve their headers. Core-made copies retain the original identity and creation context.

When a handler buffers a CloudEvents send, Relayer records a `create {destination}` producer span during that attempt and uses its context in the message headers. When the successful attempt flushes the send, Relayer records a separate `send {destination}` client span linked to the message's creation context. A failed attempt can therefore have a creation span even though Relayer discards the buffered send.

The native `ITransport.SendBatchAsync` entry point creates one `create {destination}` producer span per new message, followed by a client `send {destination}` span linked to those contexts. Existing contexts are preserved and linked without another create span. Batch spans carry `messaging.batch.message_count` and omit individual message IDs. The scoped sender still accepts one message per call; native batching does not imply atomic delivery.

Disable per-message batch spans, or opt into serialized body sizes, through the Options pattern before building the provider:

```csharp
services.AddSingleton<Microsoft.Extensions.Options.IOptions<TracingOptions>>(
    Microsoft.Extensions.Options.Options.Create(new TracingOptions
    {
        CreateSpansForBatches = false,
        IncludeBodySize = true,
    }));
```

Without per-message create spans, a batch with no existing contexts has one producer span whose context is shared by its messages. Existing contexts remain unchanged; a batch that links existing contexts uses a client span. These options also bind from configuration through `AddOptions<TracingOptions>().Bind(...)`.

Failed send spans have status `Error` and `error.type = "_OTHER"`. The exception is logged with trace and span IDs, without an exception span event. `IActivityEnricher` extensions run only for recorded spans. With no listener, send tracing adds no steady-state allocations. See [outbox tracing and metrics](telemetry.md#outbox-tracing-and-metrics) and [delivery and settlement tracing](telemetry.md#delivery-and-settlement-tracing) for those integrations.
