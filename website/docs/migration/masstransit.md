---
title: Migrate from MassTransit
description: "Move message contracts, consumers and routes to explicit Relayer registrations."
---

Start with one endpoint and run both systems on separate destinations during verification. This is an application migration, not a compatible replacement for MassTransit envelopes or persisted state. The source model below follows [MassTransit's consumer documentation](https://masstransit.io/documentation/concepts/consumers).

## Map the common path

| MassTransit call site | Relayer equivalent |
| --- | --- |
| `IConsumer<T>.Consume(ConsumeContext<T>)` | `IHandler<T>.HandleAsync(T, MessageContext, CancellationToken)` |
| Publish or endpoint send | `ISender.SendAsync` with one explicit typed route to a native topic or queue |
| Consumer/endpoint registration | `AddMessageTypesFromThisAssembly()` and typed `Consume(...).Handler<THandler>()` |

For a consumer that previously read `context.Message`, use the message parameter directly:

```csharp
[Message("orders.placed", Version = 1)]
public sealed record OrderPlaced(string OrderId);

public sealed class OrderPlacedHandler(ILogger<OrderPlacedHandler> logger) : IHandler<OrderPlaced>
{
    public ValueTask HandleAsync(OrderPlaced message, MessageContext context, CancellationToken cancellationToken)
    {
        logger.LogInformation("Received {OrderId}", message.OrderId);
        return ValueTask.CompletedTask;
    }
}
```

Use the complete [Service Bus web API sample](../guides/samples.md) for host, route, source generation and consumer registration. A route to a queue has native point-to-point behavior; a topic's subscriptions or Kafka consumer groups supply fan-out. `SendAsync` does not discover subscribers or create a MassTransit-style topology by convention. Configure [native topology](../guides/topology.md) explicitly.

## Preserve durability and operational behavior

1. Assign stable `[Message]` names and source-generated JSON metadata. Old MassTransit messages cannot be assumed to deserialize as Relayer's [CloudEvents binding](../transports/envelopes.md). Drain old destinations or use an application-owned bridge that reads through the old client and sends a new Relayer message. Preserve business identifiers and make the bridge idempotent.
2. Translate retry/redelivery intent with the selected transport's [failure capabilities](../guides/failure-handling.md). Retry counts, dead-letter destinations and delay support are explicit; do not copy middleware settings without checking native limits.
3. Replace database outbox use with the implemented [PostgreSQL EF outbox](../reference/ef-core-outbox.md), [direct dispatch](../reference/direct-dispatch.md) and recovery. For a database consumer, configure the [EF inbox boundary](../reference/ef-core-inbox.md). Neither implementation imports MassTransit database rows.
4. Re-test native concurrency, ordering, duplicate delivery, poison handling and restart with the [testing tools](../guides/testing.md). Keep old consumers running until their queues and pending outbox entries are drained, then switch producers and retire the bridge.

## Features requiring redesign

Relayer has explicit [saga state transitions](../guides/sagas.md), [timeouts](../guides/saga-timeouts.md) and [request/response](../guides/request-response.md); these do not import state machines, correlation conventions or persisted MassTransit state. Port each workflow and verify its database boundary. Routing slips and job-consumer parity are unavailable; do not treat them as implemented. Kafka parallel receive, topology management and transactions remain planned as documented in [Kafka](../transports/kafka.md). Check the [roadmap](../roadmap.md) before moving an endpoint that depends on them.
