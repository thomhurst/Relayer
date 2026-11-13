---
title: Getting started
description: "Run a process-local checkout and billing example using the implemented Relayer API."
---

Start with the runnable [modular monolith sample](https://github.com/thomhurst/Relayer/tree/main/samples/ModularMonolith). Checkout sends an order event to billing through the implemented in-memory transport. No broker is required.

## Requirements and run

Relayer targets `net10.0` and `net11.0` and needs C# 14 or later. Its Microsoft.Extensions dependencies use version 10.0.15 for `net10.0` and version 11.0.0 for `net11.0`, preserving the .NET 10 dependency floor. The repository builds with the .NET 11 SDK, and its samples run on the .NET 10 runtime. Packages are not published yet: clone the repository and run from its root:

```shell
dotnet run -c Release --project samples/ModularMonolith
```

With Microsoft.Extensions.Hosting 11 on `net11.0`, `Host.StopAsync` also rethrows background-service faults that triggered the default `StopHost` policy. This includes permanent consumer or PostgreSQL change-capture failures; callers that stop the host directly should allow these failures to reach their application error handling.

The output includes `Invoice created for order-1 (customer-1)` and the process exits. The sample project references `src/Relayer/Relayer.csproj` and the source generator project explicitly; project-reference consumers need that analyzer reference. Published packages will bundle it.

## Declare a message

Use a stable wire name and version rather than a CLR type name:

```csharp
using Relayer;

[Message("samples.orders.placed", Version = 1)]
public sealed record OrderPlaced(string OrderId, string CustomerId);
```

Analyzer [RLY001](reference/diagnostics.md#rly001) reports missing message identities.

## Write a handler

Handlers implement `IHandler<T>` and are resolved in a fresh scope per delivery. The sample's `BillingModule` prints the invoice and signals completion to the console host:

```csharp
public sealed class OrderPlacedHandler(BillingModule billing) : IHandler<OrderPlaced>
{
    public ValueTask HandleAsync(OrderPlaced message, MessageContext context, CancellationToken cancellationToken)
    {
        billing.CreateInvoice(message);
        return ValueTask.CompletedTask;
    }
}
```

This demonstration has no durable invoice storage. In production, complete durable work before returning. Handler failures enter the [failure pipeline](guides/failure-handling.md); exhausted failures dead-letter rather than stalling healthy messages.

## Configure the host

The sample uses `Host.CreateApplicationBuilder(args)` and registers its billing module before Relayer:

```csharp
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.DependencyInjection;
using Relayer;
using Relayer.InMemory;

var builder = Host.CreateApplicationBuilder(args);
builder.Services.AddSingleton<BillingModule>();
builder.Services.AddRelayer(relayer =>
{
    relayer.AddMessageTypesFromThisAssembly();
    relayer.InMemory("local", local =>
    {
        local.Send<OrderPlaced>("orders");
        local.Consume("orders", consumer => consumer.Handler<OrderPlacedHandler>());
    });
});
```

Generated registration supplies message identities, JSON metadata and handler dispatch. Startup validates routes, handlers and endpoint capabilities before receivers start. This queue needs no ordering key or external topology. See [in-memory transport](transports/in-memory.md) for ordered queues and logs.

## Send from a scope

Start the host, create a scope, then resolve `ISender`. The sample's checkout module uses the same scoped service:

```csharp
public sealed class CheckoutModule(ISender sender)
{
    public ValueTask PlaceAsync(OrderPlaced order, CancellationToken cancellationToken) =>
        sender.SendAsync(order, cancellationToken);
}
```

The send serializes with source-generated JSON and adds the [CloudEvents envelope](transports/envelopes.md). In-memory send completion means queue admission, not handler completion. The runnable sample waits for billing's completion signal with a deadline and stops the host in `finally`.

The in-memory queue and printed invoice disappear when the process ends. Broker transports including [Kafka](transports/kafka.md) and [Service Bus](transports/service-bus.md) are implemented; they use native persistence and settlement. With an active [EF outbox transaction](reference/ef-core-outbox.md), `SendAsync` persists outgoing messages with business data instead of sending immediately.

## Next steps

- [Runnable samples](guides/samples.md)
- [Messages and serialization](guides/messages.md)
- [Sending messages](guides/sending.md)
- [Migration from MassTransit](migration/masstransit.md), [Rebus](migration/rebus.md) or [CAP](migration/cap.md)
