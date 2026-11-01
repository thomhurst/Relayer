---
title: Getting started
description: "Declare a message, write a handler, configure a route and send your first message with Relayer."
---

This page builds the smallest useful Relayer application: one message, one handler, one route.

:::note[Before packages ship]

Relayer is not on NuGet yet, and the broker transports are still in progress. To try the core today, clone the repository and reference `src/Relayer/Relayer.csproj` from a .NET 10 project. The examples use placeholder option types where a transport package will later supply its native ones.

:::

## Requirements

- .NET 10 SDK or later
- C# 14 (the default for `net10.0`)

## 1. Declare a message

Messages are identified on the wire by a stable name and version, never by their CLR type name. Renaming or moving the class never breaks a consumer, and services do not need to share a contracts assembly.

```csharp
using Relayer;

[Message("orders.placed", Version = 1)]
public sealed record OrderPlaced(string OrderId, string CustomerId);
```

If you forget the attribute, analyzer [RLY001](reference/diagnostics.md#rly001) warns and offers a code fix.

## 2. Write a handler

A handler is a plain class. It is resolved from a fresh DI scope for every message, so it can inject scoped services such as a `DbContext`.

```csharp
public sealed class OrderPlacedHandler(AppDbContext db) : IHandler<OrderPlaced>
{
    public async ValueTask HandleAsync(OrderPlaced message, MessageContext context, CancellationToken cancellationToken)
    {
        db.Invoices.Add(Invoice.For(message));
        await db.SaveChangesAsync(cancellationToken);
    }
}
```

If the handler throws, the [failure pipeline](guides/failure-handling.md) classifies the error, retries it and finally dead-letters it. A bad message never crashes or stalls the consumer.

## 3. Configure Relayer

Register messages, handlers and a transport with its routes and consumers:

```csharp
services.AddRelayer(relayer =>
{
    // Source-generated: registers every [Message] type in this assembly and its JSON metadata.
    relayer.AddMessageTypesFromThisAssembly();

    relayer.AddTransport<BrokerOptions, RouteOptions, ConsumerOptions>("events", broker =>
    {
        broker.Options.Bind(configuration.GetSection("Events"));

        broker.Send<OrderPlaced>("orders.placed",
            route => route.OrderingKey(static m => m.CustomerId));

        broker.Consume("orders.placed",
            consumer => consumer.Handler<OrderPlacedHandler>().Concurrency(8));
    });
});
```

At host startup Relayer checks the whole configuration before any consumer starts: transport names, destinations, message identities, duplicate routes, handlers, option validators and endpoint capabilities. Every error is reported together. For example, an ordering key on an endpoint that cannot preserve order fails startup rather than being ignored.

## 4. Send a message

`ISender` is a scoped service. Inject it anywhere:

```csharp
public sealed class CheckoutService(ISender sender)
{
    public ValueTask PlaceAsync(Order order, CancellationToken cancellationToken) =>
        sender.SendAsync(new OrderPlaced(order.Id, order.CustomerId), cancellationToken);
}
```

The core serializes the message with source-generated System.Text.Json, writes the [CloudEvents envelope](transports/envelopes.md) as native headers, derives the ordering key and hands the result to the transport. When an outbox transaction is active in the scope, the send is enlisted in it instead.

## Next steps

- [Messages and serialization](guides/messages.md)
- [Handlers](guides/handlers.md)
- [Sending messages](guides/sending.md)
- [Failure handling](guides/failure-handling.md)
