---
title: Testing
description: "Observe message handling with a host-local harness or invoke handlers with a standalone context."
---

`Relayer.Testing` supplies `RelayerHarness`, `TestMessageContext` and the [transport conformance suite](../transports/conformance.md). The harness is independent of the test framework. Relayer tests it with parallel TUnit tests.

## Test that a handler processes an order

Register the harness **after** `AddRelayer` and all transport registrations. Configure real message identities, generated handlers and routes as usual:

```csharp
using Microsoft.Extensions.DependencyInjection;
using Relayer;
using Relayer.InMemory;
using Relayer.Testing;

services.AddRelayer(builder =>
{
    builder.AddMessageTypesFromThisAssembly();
    builder.InMemory("tests", transport =>
    {
        transport.Send<OrderPlaced>("orders");
        transport.Consume<OrderHandler>("orders");
    });
});
services.AddRelayerHarness();
```

Start the host before sending. Resolve the scoped `ISender` from a scope and the singleton harness from the host:

```csharp
var harness = host.Services.GetRequiredService<RelayerHarness>();
using var scope = host.Services.CreateScope();
await scope.ServiceProvider.GetRequiredService<ISender>()
    .SendAsync(new OrderPlaced("A-1001"));

using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(10));
var handled = await harness.WaitForConsumedAsync<OrderPlaced>(
    order => order.OrderId == "A-1001", deadline.Token);
```

The wait matches a successfully handled delivery, even if it completed before the wait began.
The same test works with a real broker. The deadline bounds the wait if the handler never processes the order.

## Inspect a whole message flow

For an in-memory test that asserts on every message in a flow, wait for idle before reading the snapshots:

```csharp
await harness.WaitForIdleAsync(deadline.Token);
var logicalSend = harness.Sent[0];
var received = harness.Consumed[0];
var orders = harness.GetConsumed<OrderPlaced>();
var sentOrders = harness.GetSent<OrderPlaced>();
var order = harness.Read<OrderPlaced>(received);
```

The collections return snapshots safe to enumerate while other work runs:

| Collection | Records |
|---|---|
| `Sent` | Successful logical sends, including sends enlisted in an outbox |
| `Dispatched` | Successful native send calls, including outbox dispatch through the registered `ITransport` services |
| `Consumed` | Deliveries entering the consume pipeline, including failed deliveries and native redeliveries |
| `Failed` | Exceptions at logical send, native dispatch and consume boundaries, identified by `HarnessOperation` |
| `DeadLettered` | Successful native dead-letter operations, including manual in-memory native settlement |

Dead-letter error headers preserve text values and own a copy of each byte-valued header.

Publish/subscribe observation uses the same `ISender.SendAsync` API. A send routed to an in-memory log appears once in `Sent` and once per consumer group in `Consumed`. The harness does not change routing, settlement, ordering or native batching. If a native batch fails after partial delivery, `Dispatched` does not claim that the batch succeeded; `Failed` records the failed batch operation for each input.

Consume failures remain observable when native settlement already logged the exception; logging configuration does not change `Failed`. A traced settlement failure is recorded once per consume boundary. Dispatch observation reads each route's configured envelope header names, including routes that share a destination with different bindings. When the failure pipeline sends a dead-letter copy through the harness, dispatch observation uses the original delivery's header binding. For example, a confirmed JetStream copy appears in `GetDispatched<T>()` even when its subject is configured only as a `DeadLetterEndpoint`.

`GetSent<T>()`, `GetDispatched<T>()` and `GetConsumed<T>()` return materialized typed snapshots. They filter by the exact registered wire name and version before deserializing, so mixed message types, unknown types and missing identities are skipped. Payloads and `Tombstone<T>` deletions sharing a wire identity are queried separately. Serialized deletion snapshots retain the native tombstone flag and an owned, byte-exact key; typed reads reconstruct the deletion without deserializing its empty body. Individual reads reject the wrong value kind. Register the requested type through `AddMessageTypesFromThisAssembly` or `MessageTypes.Add<T>()`; querying an unregistered type throws.

Use `harness.Read<T>(observation)` for an individual observation, including `Failed[i].Message` or `DeadLettered[i].Message`. Its wire identity must match the requested registered type, and a retained logical object must be assignable to that type; otherwise the read throws. Typed reads use the observation's named transport serializer, falling back to the provider's default when no name is available. `HarnessMessage.Read<T>(serializer)` remains available when you want to select the serializer explicitly.

Receive and dispatch bodies are copied before native storage can be reused. Typed results and retained observations survive `Reset()`. Logical sends retain the original message object in `Message`; do not mutate that object during assertions. Wire IDs and types can be null for malformed or raw native messages. Receive observations use the receiving transport and source endpoint, including when outbox fan-out delivers the same message ID to several transports. When a manually composed pipeline has no receive endpoint metadata, the harness falls back to correlating the delivery with a send in the same provider.

## Deterministic idle and reset

`WaitForIdleAsync` tracks active sends, dispatches and consumption. With the registered in-memory transport, it also tracks queued, scheduled and redelivered messages until every consumer group releases them. It uses completion signals, not sleeps or polling. An endpoint with no consumer retains its queued messages and therefore does not become idle.

Committed outbox rows also count as outstanding work. A row committed through this provider's outbox stays outstanding until its dispatcher settles it: sent, discarded by failure classification, moved to dead storage, or parked on an ordered lane. Rows behind a parked row on the same ordering key stay outstanding, because they are not delivered until you redrive or purge the parked row. A row whose send failed and will be retried stays outstanding until a later attempt settles it, so a test that expects a retry should wait with a deadline. This applies to every store's direct dispatch, ordered dispatch and recovery ([EF Core enlistment](../reference/ef-core-outbox.md), [direct dispatch](../reference/direct-dispatch.md), [ordered dispatch](../reference/ordered-dispatch.md), [leased recovery](../reference/postgres-maintenance.md)). The count is per provider and has no row identity: when several hosts share one outbox table, one host's recovery can settle a row that another host committed and leave the wrong host waiting or idle. Give each test its own table or schema. The native dispatch must use the registered `ITransport` services for observation.

```csharp
await using (var scope = provider.CreateAsyncScope())
{
    var db = scope.ServiceProvider.GetRequiredService<OrdersDbContext>();
    using var enlistment = db.EnlistRelayerOutbox();
    await enlistment.EnqueueAsync(new OrderPlaced(id));
    await db.SaveChangesAsync();
}
// Completes after the direct dispatcher has sent the row and its in-memory consumers have run.
await harness.WaitForIdleAsync(deadline.Token);
```

Start producers before awaiting idle. Future external broker arrivals cannot be inferred from an idle pipeline ([wait for those instead](#wait-for-a-broker-round-trip)). Include an application-owned producer that the harness cannot observe explicitly. In this example, `ProduceAsync` is your application's method:

```csharp
var work = harness.TrackWork();
try
{
    await application.ProduceAsync();
}
finally
{
    work.Dispose();
}
await harness.WaitForIdleAsync(deadline.Token);
```

Cancelling an idle wait cancels only that caller's wait. It does not stop processing or other waiters. Use a test deadline so poison messages configured for indefinite redelivery cannot hang a test.

Call `harness.Reset()` only after idle. It clears all observations and correlation state. Calling it while work is pending throws an actionable exception. Reset does not purge native queues, broker state or outbox rows.

Each service provider owns its harness. Multiple named transports in one host share that host's observations and work counter. Separate hosts have independent harnesses and can run concurrently; no mutable process-wide state is used. `AddRelayer` currently has one registration per provider: configure all named transports there rather than calling it again for another bus.

## Wait for a broker round-trip

Against a real broker or emulator, a message that has left the host is not outstanding work, so idle can complete before the delivery comes back. Wait for the consumed message instead:

```csharp
using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(30));
await sender.SendAsync(new OrderPlaced(orderId));

var placed = await harness.WaitForConsumedAsync<OrderPlaced>(
    message => message.OrderId == orderId, deadline.Token);
```

`WaitForConsumedAsync` completes with the first delivery of that registered message type that the consume pipeline handled successfully and that matches the optional predicate. It includes deliveries handled before the call, so start it before or after sending. A failed attempt does not match, nor does a delivery that failure handling redelivers, discards or dead-letters, or one whose handler requests a retry or dead-letter; a later successful redelivery does. The wait is signalled when processing finishes, not by polling. To wait for a cascade, wait for the last message in the chain.

In-memory handlers that settle a native delivery themselves count as handled when they call `Complete()`. A native `Redeliver()` keeps the wait pending until a successful delivery, and a native `DeadLetter()` appears in `DeadLettered` without satisfying the wait. For other transports, the harness counts an otherwise successful attempt marked `SettledByHandler` as handled.

The wait matches the delivery's original wire name and version and returns the original contract. If a consumer handles `Legacy` as `Current` through `Accepts<Current>` or an upcaster, `WaitForConsumedAsync<Legacy>()` matches that delivery and returns `Legacy`. `WaitForConsumedAsync<Current>()` waits for a delivery with the `Current` wire identity. To assert which handler ran or inspect the converted value, signal from the handler or one of its dependencies.

The predicate runs on the consuming thread, so keep it short. An exception from the predicate or from deserialization fails the wait, not the delivery. Cancellation cancels only that wait. `Reset()` clears the handled history, so a later wait only matches deliveries handled after the reset.

## Invoke a handler without a host

Handlers are plain objects. Construct `TestMessageContext` with a stable wire identity and any required services, headers, attempt or cancellation token:

```csharp
var context = new TestMessageContext(
    new MessageType("orders.placed", 1),
    services: provider,
    headers: new Dictionary<string, string> { ["tenant"] = "north" });

context.Features.Set(new NativeMessage<RabbitMqDelivery>(delivery));
await handler.HandleAsync(new OrderPlaced("A-1001"), context, context.CancellationToken);
```

`Features` accepts arbitrary typed features. Construct `NativeMessage<T>` around the native object the transport would supply, so code that reads `context.Features.Get<NativeMessage<T>>()` or an extension such as `context.RabbitMQ()` runs unchanged. The default context has a new UUIDv7 ID, attempt one, empty headers and an empty service provider. Header input is copied. The context does not create or dispose DI scopes; the test owns any provider or scope it supplies.

Harness snapshots intentionally allocate and retain history until reset. Register the harness only in tests.
