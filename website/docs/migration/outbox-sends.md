---
title: Upgrade outbox sends
description: "Replace the inactive-outbox direct-send fallback with an explicit durability choice."
---

`ISender.SendAsync` now throws `InvalidOperationException` when an outbox integration is registered
but no outbox transaction or pending unit of work is active in the current scope. Previously it sent
directly to the broker. This is an approved breaking change for the next pre-release version after v0.1.2.
The check applies to both `SendAsync` overloads, before per-send configuration, middleware or serialization.

`MessageContext.ReplyAsync` also requires an active outbox transaction or selected EF Core unit of work
when an outbox is registered. Activate the outbox in the responder's scope before replying.
See [reply durability](../guides/request-response.md#metadata-and-durability).

## Keep business data and messages atomic

Keep `SendAsync`, but activate the outbox before sending. For EF Core, resolve the context and sender
from the same scope, then use an explicit EF transaction:

```csharp
await using var transaction = await db.Database.BeginTransactionAsync(cancellationToken);
db.Orders.Add(order);
await sender.SendAsync(new OrderPlaced(order.Id), cancellationToken);
await db.SaveChangesAsync(cancellationToken);
await transaction.CommitAsync(cancellationToken);
```

For an implicit save, select the context with `using var enlistment = db.EnlistRelayerOutbox();`
before sending, and keep that selection alive through `SaveChangesAsync`. A consumer's configured
database boundary also activates its scoped transaction. ADO.NET outbox integrations require their
existing transaction API. Registration alone does not start a transaction.
See [EF Core transactions](../reference/ef-core-outbox.md#explicit-transactions) and
[the outbox guide](../guides/outbox.md).

## Choose intentional direct sends

To bypass outbox enlistment, use the explicit API:

```csharp
await sender.SendWithoutOutboxAsync(message, cancellationToken);
// The configuration overload is also supported.
await sender.SendWithoutOutboxAsync(message, options => options.Headers["origin"] = "worker", cancellationToken);
```

Inside a handler, these sends still wait in the attempt buffer until the handler, middleware and
scope disposal succeed. They are not atomic with database changes. Route requirements still apply:
`TransactionRequirement.Outbox` rejects direct sends, and several send routes require an active outbox.

## EF Core context selections

`db.EnlistRelayerOutbox()` now returns `IOutboxSelection` from the extension on `DbContextOutboxExtensions`.
Use `selection.EnqueueAsync(message, cancellationToken)` to enlist in that particular context, including when another selection is active.
Previously compiled callers can still use the original static method on `EntityFrameworkCoreOutboxExtensions`.

## Unchanged behavior

- Without an outbox registration, `SendAsync` sends directly.
- With an active outbox transaction or selected EF Core pending unit of work, `SendAsync` enlists.
- `SendViaOutboxAsync` requires both an outbox registration and active enlistment.
- `SendRouteOptions.TransactionRequirement` still defaults to `None`; it does not opt out of this safety check.

Check controller, background service and handler call sites when upgrading. Activate the business
transaction to commit business changes and outgoing messages together. Use `SendWithoutOutboxAsync`
for intentional direct sends.
