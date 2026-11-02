---
title: Writing a transport
description: "The contract a transport package implements: typed registration, runtime adapters, capabilities and the native send seam."
---

A transport package wraps one native client. It owns the client, the receive loop, the mapping to native messages and settlement. The core owns everything that is the same on every broker: serialization, the CloudEvents envelope, routing, the failure pipeline and its fallbacks, the outbox, the inbox and telemetry.

A transport author writes five small pieces: a [message binding and receive loop](receive.md), the native send methods described below, [capabilities](../concepts/capabilities.md), [topology](../guides/topology.md#transport-adapters), and typed builder extensions.

## Registration

`IRelayerBuilder.AddTransport<TTransportOptions, TRouteOptions, TConsumerOptions>` is the core seam for a transport package's typed registration extension. It declares routes and consumers; it does not substitute for a native client or start a broker receive loop. The application-side call site is in [Configuration](../guides/configuration.md), with executable coverage in `BuilderTests`.

Pass native option types through unchanged. `TransportBuilder.Options` is an ordinary named `OptionsBuilder<TTransportOptions>`: use `Configure`, `Bind`, and `Validate` rather than inventing another configuration model. Every transport has a unique, case-sensitive name, shared by its named options. Register native client factories and hosted services in the same `IServiceCollection` exposed by `IRelayerBuilder.Services`.

Register each runtime adapter as an `ITransport` singleton with `Name` equal to its transport declaration's name. Before receive services start, the core calls `GetCapabilities(endpoint)` once for each distinct declared source or destination on that adapter. Resolve endpoint-specific values from native options or already available topology metadata; capability discovery must not start consumers. Duplicate adapter names fail startup. When any runtime adapter is registered, every transport declaration must have a matching adapter. With no runtime adapters, declaration-only applications can still validate and inspect configuration without a broker.

Use typed extensions on `RouteBuilder<TMessage, YourRouteOptions>` and `ConsumerBuilder<YourConsumerOptions>` for native features such as consumer groups or sessions. `Options` exposes the exact option instance. Do not add Kafka settings to another transport's builder. Do not expose both `Produce<T>` and `Send<T>` for the same route: the core uses `Send<T>` for destinations of every native kind.

At startup, resolve `RelayerConfiguration`. Each transport definition contains its routes and consumers in declaration order. Cast a route or consumer to its known closed generic definition once during composition. `RouteDefinition<TMessage, TRouteOptions>.GetOrderingKeySelector<TKey>()` returns the original typed delegate; `null` means no ordering key. An incompatible key type fails explicitly. The direct send path invokes the typed selector without reflection, boxing, or a new wrapper delegate. Core send composition formats scalar keys into pooled UTF-8 storage and passes a borrowed `WireString` to the adapter; string selectors retain their original text. Native adapters map the representation to their partition key, session ID, or message group ID. Portable route settings such as timeout and envelope binding are configured through `RouteBuilder.SendOptions`; see [Sending messages](../guides/sending.md).

`IConsumerDefinition.Handlers` contains explicit handler types for generated dispatch. `Handler<THandler>` requires `IHandlerMarker`, inherited by `IHandler<T>` and `IBatchHandler<T>`. It registers the handler scoped unless the application already registered it. Do not discover message contracts by reflection. Receive-pipeline composition uses generated descriptors, and checks handler/consumer compatibility there.

Native adapters must honor the concurrency and batching declarations using native behavior and the core scheduler where applicable, and validate their native limits before starting. These declarations do not emulate a native batching or ordering feature.

## Sending

`ITransport.SendAsync(OutgoingMessage, CancellationToken)` and `SendBatchAsync(ReadOnlyMemory<OutgoingMessage>, CancellationToken)` are the common native send seam. Routing chooses the named transport before invoking these methods. The same shape serves direct sends, outbox dispatch, scheduled copies, and dead-letter copies. Transport adapters preserve native delivery, ordering, and batching semantics; batch completion does not promise atomic delivery.

`OutgoingMessage.Headers` is borrowed memory containing named `WireString` values. Each name is already mapped to the selected transport's binary-mode binding. An adapter maps those values to its native header/property representation without rebuilding the envelope or changing the original creation context.

```csharp
// Body and headers are already serialized/resolved by the core.
var outgoing = new OutgoingMessage(
    destination: "orders.placed",
    body: body,
    headers: headers,
    orderingKey: new WireString(customerId),
    deliverAt: scheduledTime,
    options: sendOptions);
await transport.SendAsync(outgoing, cancellationToken);
```

The body, header entries, header value buffers, batch array, and native options must remain alive and unchanged until the send completes, including faulted or cancelled completion. A transport must finish using that memory before completion; it cannot return early while a native operation still uses the buffers. The caller can then return pooled storage. An outbox takes owned copies of the body and headers before enlistment completes and persists them before transaction commit; dispatch reconstructs the same send shape from the persisted fields. The transport name belongs to routing/outbox storage rather than the message passed to an already selected transport.

`options` carries only the native callbacks at this seam. The caller has already resolved `SendOptions.Headers`, `OrderingKey`, and `DeliverAt` into the other arguments. Options are borrowed, so callers must not mutate them during a send.

A transport constructs its outgoing native object, maps body/headers/ordering/scheduling, then calls `outgoing.ApplyNative(native)` immediately before handing it to the native client. `ApplyNative<TNative>` uses the transport's declared native outgoing type. Callbacks accepting that type or a base type run in registration order. An incompatible callback throws an actionable error instead of being dropped. Callbacks are stored directly, without wrapping closures, reflection, or dynamic invocation.

Before persisting or enlisting a message, outbox integrations call `outgoing.ValidateForOutbox()`. A send containing native callbacks fails because delegates cannot be persisted; portable headers, ordering, and scheduling remain available. The scoped sender performs this validation before calling `IOutboxEnlistment`.

### Send tracing

Implement `ITransportSendTracing` alongside `ITransport`. Construct one `SendTracing` per declared endpoint at startup, using the provider's `MessageTracing`, the adapter's `MessagingEndpoint`, and cached delegates to its native single and batch implementations:

```csharp
var sendTracing = new SendTracing(
    tracing,
    new MessagingEndpoint
    {
        System = "kafka",
        Destination = topic,
        ServerAddress = brokerHost,
        ServerPort = brokerPort,
        ClientId = clientId,
    },
    SendNativeAsync,
    SendBatchNativeAsync,
    logger,
    headerNames);
```

`GetSendTracing(destination)` returns that endpoint's instance. The scoped pipeline uses it to capture the producer context before serialization. Implement `ITransport.SendAsync` by forwarding to `sendTracing.SendAsync`; implement homogeneous native batches through `sendTracing.SendBatchAsync`. The native delegates must call the client directly, rather than re-entering these tracing entry points. The seam suppresses duplicate spans for sends already traced by the scoped pipeline, and preserves existing header values byte for byte. Native instrumentation and native configuration callbacks must not overwrite the resulting creation context.

The seam borrows prepared headers and batch memory until the native delegate completes. It preserves body, ordering, scheduling, native callbacks, input order and partial-failure behavior. Supply the native system identifier and destination format from the semantic conventions, rather than the arbitrary registration name. Endpoint metadata and span names are computed at startup. Deferred handler sends use a create span before their envelope is copied into the attempt buffer, then a linked send span when the buffer flushes.

Each endpoint retains send preparation storage up to its observed concurrency high-water mark, including workloads above 64 overlapping single or batch sends. Storage stays exclusive to a send until its native delegate completes, including failure or cancellation. Initial concurrency growth and larger batches can allocate; repeated bursts at the warmed concurrency and batch size reuse that storage without per-message locks. Each slot retains its largest preparation arrays and header buffers for the endpoint lifetime, so a temporary concurrency or batch-size spike can increase retained memory. `System.Diagnostics` activity allocations and framework async-builder costs remain separate. Run `SendTracingConcurrencyBenchmarks` for complete burst costs with dropped and recorded spans.

`InMemoryTransport` implements this seam. DI registration supplies `MessageTracing` automatically. For a standalone instance, pass `tracing:` to its constructor. Its homogeneous batches use one batch span; mixed-destination batches retain native input order and use one single-message span per message. The Kafka producer adapter also implements this seam. Other broker adapters must provide this integration when implemented.

Run the reusable [transport conformance suite](conformance.md) against the real broker or emulator before shipping an adapter.

## Asynchronous capability evidence

An adapter that needs broker I/O before it can declare endpoint capabilities implements `ITransportCapabilityInitializer.InitializeCapabilitiesAsync(TopologyMode, CancellationToken)`. Hosted startup awaits that evidence before caching `GetCapabilities` results, validating route scheduling requirements or composing send routes, including when hosted services start concurrently. Initialize once per adapter instance and report failures before receive execution starts.

`None` and `Validate` must remain read-only. Do not publish probes or create temporary entities to discover capabilities in those modes. When AMQP cannot inspect a feature read-only, keep conservative capabilities and select an existing core fallback. For example, RabbitMQ only advertises a delayed exchange after an explicit native declaration succeeds in `Provision`. Unverified capability state must not be cached through an early send-pipeline resolution.

Use `MessagingEndpoint.Attributes` for precomputed convention-defined native endpoint attributes that samplers need when a span starts, such as RabbitMQ's routing key. Attribute names and meanings still follow the messaging semantic conventions; Relayer additions use the `relayer.` prefix.
