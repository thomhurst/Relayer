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

`MessageContext.Features` follows the same pattern as ASP.NET Core's `HttpContext.Features`. Transports can expose native deliveries through `NativeMessage<TIncoming>` and provide typed extensions to access them. Use borrowed native deliveries only while processing the message.

In a Kafka handler, `context.Kafka().ConsumeResult` exposes Dekaf's original record, including its `Partition` and `Offset`. See [Kafka consumer registration](../transports/kafka.md#register-a-consumer).

```csharp
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Relayer;
using Relayer.Kafka;

public sealed class OrderPlacedHandler(ILogger<OrderPlacedHandler> logger) : IHandler<OrderPlaced>
{
    public ValueTask HandleAsync(OrderPlaced message, MessageContext context, CancellationToken cancellationToken)
    {
        var record = context.Kafka().ConsumeResult;
        logger.LogInformation("Kafka partition {Partition}, offset {Offset}", record.Partition, record.Offset);
        return ValueTask.CompletedTask;
    }
}
```

In an Azure Service Bus handler, import `Relayer.AzureServiceBus` for `context.ServiceBus()`. Use `.Message` for the original `ServiceBusReceivedMessage`, `.Callback` for a non-session callback, or `.SessionCallback` for a session callback. See [Azure Service Bus receiving](../transports/service-bus.md#register-receiving).

On the send side, `ConfigureNative<TNative>` passes the outgoing object to a typed callback before the transport hands it to the client. Import `Azure.Messaging.ServiceBus` for `ServiceBusMessage` in this example.

```csharp
await sender.SendAsync(new ChargeCard(orderId, amount),
    options => options.ConfigureNative<ServiceBusMessage>(m => m.SessionId = customerId),
    cancellationToken);
```

Native configuration is passed straight through and never re-modelled: Dekaf's builders, `ServiceBusClientOptions`, `ConnectionFactory` and `AmazonSQSConfig`.

## Message identity on the wire

Every message carries a stable type such as `orders.placed;v=1` in CloudEvents headers, together with a UUIDv7 ID, source, time and W3C trace context. Consumers route by that type, so several message types can share one topic or queue, and non-Relayer applications in any language can produce and consume the same messages. See [Envelopes and headers](../transports/envelopes.md).
