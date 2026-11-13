---
title: Testing
description: "Observe message handling with a host-local harness or invoke handlers with a standalone context."
---

`Relayer.Testing` supplies `RelayerHarness`, `TestMessageContext` and the [transport conformance suite](../transports/conformance.md). The harness is independent of the test framework. Relayer tests it with parallel TUnit tests.

## Observe a host

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
        transport.Consume("orders", consumer => consumer.Handler<OrderHandler>());
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

Publish/subscribe observation uses the same `ISender.SendAsync` API. A send routed to an in-memory log appears once in `Sent` and once per consumer group in `Consumed`. The harness does not change routing, settlement, ordering or native batching. If a native batch fails after partial delivery, `Dispatched` does not claim that the batch succeeded; `Failed` records the failed batch operation for each input.

Consume failures remain observable when native settlement already logged the exception; logging configuration does not change `Failed`. A traced settlement failure is recorded once per consume boundary. Dispatch observation reads each route's configured envelope header names, including routes that share a destination with different bindings.

`GetSent<T>()`, `GetDispatched<T>()` and `GetConsumed<T>()` return materialized typed snapshots. They filter by the exact registered wire name and version before deserializing, so mixed message types, unknown types and missing identities are skipped. Logical sends also match the retained object type, so payloads and `Tombstone<T>` deletions sharing a wire identity can be queried separately. Register the requested type through `AddMessageTypesFromThisAssembly` or `MessageTypes.Add<T>()`; querying an unregistered type throws.

Use `harness.Read<T>(observation)` for an individual observation, including `Failed[i].Message` or `DeadLettered[i].Message`. Its wire identity must match the requested registered type, and a retained logical object must be assignable to that type; otherwise the read throws. Typed reads use the observation's named transport serializer, falling back to the provider's default when no name is available. `HarnessMessage.Read<T>(serializer)` remains available when you want to select the serializer explicitly.

Receive and dispatch bodies are copied before native storage can be reused. Typed results and retained observations survive `Reset()`. Logical sends retain the original message object in `Message`; do not mutate that object during assertions. Wire IDs and types can be null for malformed or raw native messages. Transport and destination names on receive observations are correlated with sends in the same provider; unsolicited external deliveries may not have those names.

## Deterministic idle and reset

`WaitForIdleAsync` tracks active sends, dispatches and consumption. With the registered in-memory transport, it also tracks queued, scheduled and redelivered messages until every consumer group releases them. It uses completion signals, not sleeps or polling. An endpoint with no consumer retains its queued messages and therefore does not become idle.

Start producers before awaiting idle. Future external broker arrivals and durable outbox rows not yet being dispatched cannot be inferred from an idle pipeline. Include an application-owned producer or dispatch cycle explicitly. In this example, `CommitAndDispatchAsync` is your application's method:

```csharp
var cycle = harness.TrackWork();
try
{
    await application.CommitAndDispatchAsync();
}
finally
{
    cycle.Dispose();
}
await harness.WaitForIdleAsync(deadline.Token);
```

Keep the lease from before the producer or outbox commit until dispatch finishes or the transaction rolls back. The native dispatch must use the registered `ITransport` services for observation. [EF Core enlistment](../reference/ef-core-outbox.md) and [unordered PostgreSQL direct dispatch](../reference/direct-dispatch.md) are available separately; [ordered dispatch](../reference/ordered-dispatch.md) and [leased recovery](../reference/postgres-maintenance.md) are also available. The harness does not provide an outbox implementation or automatically track a direct dispatch queue as outstanding work.

Cancelling an idle wait cancels only that caller's wait. It does not stop processing or other waiters. Use a test deadline so poison messages configured for indefinite redelivery cannot hang a test.

Call `harness.Reset()` only after idle. It clears all observations and correlation state. Calling it while work is pending throws an actionable exception. Reset does not purge native queues, broker state or outbox rows.

Each service provider owns its harness. Multiple named transports in one host share that host's observations and work counter. Separate hosts have independent harnesses and can run concurrently; no mutable process-wide state is used. `AddRelayer` currently has one registration per provider: configure all named transports there rather than calling it again for another bus.

## Invoke a handler without a host

Handlers are plain objects. Construct `TestMessageContext` with a stable wire identity and any required services, headers, attempt or cancellation token:

```csharp
var context = new TestMessageContext(
    new MessageType("orders.placed", 1),
    services: provider,
    headers: new Dictionary<string, string> { ["tenant"] = "north" });

context.Features.Set(new FakeNativeFeature());
await handler.HandleAsync(new OrderPlaced("A-1001"), context, context.CancellationToken);
```

`Features` accepts arbitrary typed features, including fake native features. The default context has a new UUIDv7 ID, attempt one, empty headers and an empty service provider. Header input is copied. The context does not create or dispose DI scopes; the test owns any provider or scope it supplies.

Harness snapshots intentionally allocate and retain history until reset. Register the harness only in tests.
