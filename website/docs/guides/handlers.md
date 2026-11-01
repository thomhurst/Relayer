---
title: Handlers
description: "Write single-message and batch handlers, register them explicitly or with source-generated registration, and use the message context."
---

## Single-message handlers

A handler implements `IHandler<TMessage>`. It is resolved from a fresh DI scope for every message, so it can inject scoped services:

```csharp
public sealed class OrderPlacedHandler(AppDbContext db, ISender sender) : IHandler<OrderPlaced>
{
    public async ValueTask HandleAsync(OrderPlaced message, MessageContext context, CancellationToken ct)
    {
        db.Invoices.Add(Invoice.For(message));
        await sender.SendAsync(new ChargeCard(message.OrderId, message.Total), ct);
        await db.SaveChangesAsync(ct);
    }
}
```

`MessageContext` gives you the message ID, the stable `MessageType`, lazily read `Headers`, the delivery `Attempt` (starting at 1), the scoped `Services`, the `CancellationToken` and the `Features` collection that exposes the native message. The context is borrowed for the duration of the call; do not keep a reference to it.

## Batch handlers

A batch handler receives several messages at once. Each message can succeed or fail independently. Without buffered direct sends, only failed items enter the failure pipeline:

```csharp
public sealed class AuditHandler(AuditStore store) : IBatchHandler<AuditEvent>
{
    public async ValueTask<BatchResult> HandleAsync(Batch<AuditEvent> batch, CancellationToken ct)
    {
        IReadOnlyDictionary<int, Exception> failures = await store.WriteAsync(batch.Items, ct);
        return BatchResult.PartialFailure(failures); // keyed by index within the batch
    }
}
```

Return `BatchResult.Success` when every message succeeded.

Configure the existing consumer declaration with `.Batch(maxSize, maxWait)`.
The core composes generated batch dispatch in one fresh scope per batch attempt.
Normally only failed items are retried; an exception from the handler fails every item in
that invocation. Success waits for asynchronous scope disposal and durable work
before the adapter receives settlement decisions.

Direct sends from the scoped `ISender` are buffered until the entire attempt
succeeds. A partial failure with buffered sends discards those sends and fails
every item in the attempt because sends cannot be attributed to individual items.
The normal retry, redelivery and dead-letter policy handles every item, including
otherwise successful items, so poison messages cannot stall the batch indefinitely.
Use explicit per-message
durable outbox transactions when partial success also produces messages. Do not
retain the borrowed batch or its item contexts after handler completion.

Adapters must install a native or core client-side receive executor before
starting consumers; declaring a size does not supply one. The core execution
seam is available now; the in-memory hosted adapter still explicitly rejects
batch-handler composition. See [batch composition](../transports/consume-pipeline.md#batch-consumers)
for transport-author integration and native count/wait limits.

## Registration without scanning

Relayer never scans assemblies at runtime. Register handlers inside `AddRelayer`:

```csharp
services.AddRelayer(relayer => relayer.Handler<OrderPlacedHandler>());

// Or register every concrete, assembly-accessible handler declared in this assembly:
services.AddRelayer(relayer => relayer.AddHandlersFromThisAssembly());
```

`AddHandlersFromThisAssembly()` is written by the source generator at compile time. It is a plain list of `Handler<T>()` calls you can open and read, and it works under Native AOT.

Discovery includes inherited contracts, explicit interface implementations, and handlers with several message or batch contracts. Open generic, abstract, private nested and file-local classes are excluded. Explicit `Handler<ClosedHandler<OrderPlaced>>()` supports closed generic handlers and handlers in referenced assemblies. Repeated registration is idempotent, and handlers default to a scoped registration unless you registered them yourself.

Message types must be accessible to the generated code; [RLYH001](../reference/diagnostics.md#suppression-and-generated-code) reports ones that are not. Analyzer [RLY003](../reference/diagnostics.md#rly003) warns about a handler that is never registered, and [RLY004](../reference/diagnostics.md#rly004) about a consumer with no handler.

## Dispatch

Dispatch is source-generated. When the pipeline is composed at startup, it selects each handler's generated descriptor once. For every message, the descriptor resolves the handler with typed DI resolution and calls it through its exact interface. There is no type lookup, reflection or delegate allocation per message.
