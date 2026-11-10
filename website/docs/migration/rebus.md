---
title: Migrate from Rebus
description: "Replace Rebus handlers and bus routing with explicit Relayer contracts and native endpoints."
---

Rebus exposes [`IHandleMessages<T>.Handle(T)`](https://github.com/rebus-org/Rebus/blob/master/Rebus/Handlers/IHandleMessages.cs) and [`IBus` send, publish and subscription methods](https://github.com/rebus-org/Rebus/blob/master/Rebus/Bus/IBus.cs). Move one message flow at a time, preserving business identifiers and testing duplicate delivery before changing production routes.

## Replace the handler and route

| Rebus | Relayer |
| --- | --- |
| `IHandleMessages<OrderPlaced>` | `IHandler<OrderPlaced>` |
| `Task Handle(OrderPlaced message)` | `ValueTask HandleAsync(OrderPlaced message, MessageContext context, CancellationToken cancellationToken)` |
| `IBus.Send(message)` / `IBus.Publish(message)` | `ISender.SendAsync(message, cancellationToken)` through one declared route |
| Type-based endpoint mapping / subscribe | Typed `Send<T>(destination)` and explicit native consumer declarations |

Declare `[Message("orders.placed", Version = 1)]` on a concrete contract, add it with `AddMessageTypesFromThisAssembly()`, and register its handler explicitly. Follow the complete [getting started](../getting-started.md) example to try a local queue; choose a real broker from the [samples](../guides/samples.md) for durable delivery. Relayer does not scan assemblies at runtime.

Queue versus topic semantics belong to the native destination. Map each Rebus owner endpoint and subscription deliberately: native Service Bus topic/subscriptions or Kafka groups can provide fan-out. There is no separate portable `Publish` or runtime `Subscribe` API. Relayer's stable identity and [CloudEvents headers](../transports/envelopes.md) are different from Rebus's type/header conventions; reusing a queue name does not make existing records interoperable.

## Move transactions and retries deliberately

Relayer's [PostgreSQL EF outbox](../reference/ef-core-outbox.md) persists sends with business data in the same EF transaction. Register dispatch and recovery too; enlistment alone does not deliver rows. The [EF consumer inbox](../reference/ef-core-inbox.md) can deduplicate the same message identity and commit handler changes and outgoing events atomically. These tables cannot import Rebus transaction or saga records automatically.

Translate error queue and retry policies to [failure handling](../guides/failure-handling.md), checking each transport's durable redelivery support. Delayed sending uses native scheduling or an explicitly registered durable fallback; it is not a replacement timeout manager installed by convention. [Request/response](../guides/request-response.md) and [sagas](../guides/sagas.md) are implemented through explicit declarations, not automatic imports of Rebus reply addresses or saga correlations. A universal Rebus wire/state compatibility layer is unavailable. Kafka parallel receive and transactions remain planned; check the [Kafka guide](../transports/kafka.md) before relying on those behaviors.

## Cut over safely

Drain the original input and error queues and pending transactional sends, or run an application-owned bridge that consumes with Rebus and sends a new Relayer envelope. Do not run both libraries as competing readers on one mixed-format queue. Verify business idempotency, retries, dead-letter inspection, shutdown and broker reconnect with [real-broker tests](../guides/testing.md). Switch producers only after every consumer of that route understands the new contract; retain old infrastructure until its backlog is handled.
