---
title: Core model
description: "Transports, endpoints, routes, consumers, the message context and the feature collection that exposes native messages."
---

```text
          ┌──────────── send side ────────────┐            ┌────────── receive side ──────────┐
 your code → ISender → send middleware → outbox? → transport → native client → broker → native client → transport
                                                                                                │
                                         handler ← consume middleware (retry, inbox, telemetry…) ┘
```

## Concepts

| Concept | Meaning |
|---|---|
| **Transport** | An adapter over one native client (Kafka, Azure Service Bus, SQS, RabbitMQ). Registered with a name, so you can have several of the same kind. |
| **Endpoint** | A native destination or source: a Kafka topic, a Service Bus queue or topic/subscription, an SQS queue, a RabbitMQ exchange or queue. |
| **Route** | A mapping from a message type to an endpoint, plus how to derive native properties such as the key, session ID, partition key or message group ID. |
| **Consumer** | A subscription to an endpoint that dispatches to handlers. |
| **MessageContext** | The per-message context: message ID, stable message type, headers, attempt count, scoped services, cancellation and a feature collection that exposes the native message. |
| **Capabilities** | What an endpoint can do natively: dead-lettering, delayed delivery and redelivery, ordering, batching, transactions, maximum message size. See [Capabilities](capabilities.md). |

## Ownership

The core owns everything that is the same on every broker: serialization, the CloudEvents envelope, routing, the failure pipeline and its fallbacks, the inbox, the outbox and telemetry. A transport owns only what is native: the client, the receive loop, the mapping to native messages and settlement. Generics carry the native types through the core, so the core is written once and still runs specialized, allocation-free code for each transport.

## The native escape hatch

`MessageContext.Features` follows the same pattern as ASP.NET Core's `HttpContext.Features`. A transport publishes the untouched native delivery as `NativeMessage<TIncoming>`, and its package adds a typed extension to reach it:

```csharp
public async ValueTask HandleAsync(OrderPlaced message, MessageContext context, CancellationToken ct)
{
    var kafka = context.Kafka();          // throws if this consumer is not on Kafka
    logger.LogInformation("partition {P} offset {O}", kafka.Partition, kafka.Offset);
}
```

On the send side, a typed callback receives the native outgoing object just before it is handed to the client:

```csharp
await sender.SendAsync(new ChargeCard(orderId, amount),
    options => options.ConfigureNative<ServiceBusMessage>(m => m.SessionId = customerId),
    cancellationToken);
```

`context.Kafka()` and the Service Bus types come from transport packages that are still in development; the `NativeMessage<TIncoming>` feature and `ConfigureNative<TNative>` exist in the core today.

Native configuration is passed straight through and never re-modelled: Dekaf's builders, `ServiceBusClientOptions`, `ConnectionFactory` and `AmazonSQSConfig`.

## Message identity on the wire

Every message carries a stable type such as `orders.placed;v=1` in CloudEvents headers, together with a UUIDv7 ID, source, time and W3C trace context. Consumers route by that type, so several message types can share one topic or queue, and non-Relayer applications in any language can produce and consume the same messages. See [Envelopes and headers](../transports/envelopes.md).
