---
title: Durable saga timeouts
description: "Schedule typed process timeout messages atomically with state through the existing outbox."
---

:::warning Preview
Saga timeouts is preview and outside the first stable release compatibility promise. Using its API requires explicit compile-time opt-in by suppressing [RLYEXP001](../reference/diagnostics.md#rlyexp001) (for example, add `<NoWarn>$(NoWarn);RLYEXP001</NoWarn>` to your project).
:::


A process timeout is an ordinary typed message sent with `SendOptions.DeliverAt` inside a
[saga state transition](sagas.md). The existing EF Core consumer transaction commits the state,
inbox identity and scheduled outbox row together. The existing durable dispatcher sends the
message when due, including after a host restart. There is no separate saga scheduler or store.

## Correlate each wait explicitly

Use an application-owned wait identity in both the state and timeout contract. Replace it when
starting a new wait. Clear it when the wait finishes. An old timeout then cannot expire a newer
wait, even when it has a different message ID or arrives after inbox retention expires.

The following process accepts `BeginWait` to start or replace a wait and `WaitExpired` to end it:

```csharp
using Microsoft.EntityFrameworkCore;
using Relayer;
using Relayer.Sagas.EntityFrameworkCore;

[Message("orders.begin-wait")]
public sealed record BeginWait(Guid OrderId);
[Message("orders.wait-expired")]
public sealed record WaitExpired(Guid OrderId, Guid WaitId);

public sealed class OrderState : EntityFrameworkCoreSagaData
{
    public Guid WaitId { get; set; }
    public int TimeoutCount { get; set; }
}

public sealed class BeginOrderWait(OrdersDbContext db, ISender sender, TimeProvider time)
    : EntityFrameworkCoreSaga<OrdersDbContext, OrderState, BeginWait>(db)
{
    protected override ValueTask<OrderState?> LoadAsync(BeginWait message, CancellationToken cancellationToken)
        => new(Database.Orders.SingleOrDefaultAsync(s => s.Id == message.OrderId, cancellationToken));

    protected override OrderState Create(BeginWait message) => new() { Id = message.OrderId };

    protected override ValueTask TransitionAsync(OrderState state, BeginWait message,
        MessageContext context, CancellationToken cancellationToken)
    {
        state.WaitId = Guid.NewGuid();
        var due = time.GetUtcNow().AddDays(1);
        return sender.SendAsync(new WaitExpired(state.Id, state.WaitId),
            options => options.DeliverAt = due, cancellationToken);
    }
}

public sealed class ExpireOrderWait(OrdersDbContext db)
    : EntityFrameworkCoreSaga<OrdersDbContext, OrderState, WaitExpired>(db)
{
    protected override ValueTask<OrderState?> LoadAsync(WaitExpired message, CancellationToken cancellationToken)
        => new(Database.Orders.SingleOrDefaultAsync(s => s.Id == message.OrderId, cancellationToken));

    protected override ValueTask TransitionAsync(OrderState state, WaitExpired message,
        MessageContext context, CancellationToken cancellationToken)
    {
        if (state.WaitId == Guid.Empty || state.WaitId != message.WaitId) return default;
        state.WaitId = Guid.Empty;
        state.TimeoutCount++;
        // Perform durable business changes here, or send through the scoped ISender.
        return default;
    }
}
```

`OrdersDbContext.Orders` is a tracked `DbSet<OrderState>`. Configure its explicit key and native
`xmin` row version as shown in the [state model](sagas.md#explicit-registration). Use
application-owned migrations for the wait fields. A process that supports several simultaneous
waits needs an explicit identity for each wait; the runtime does not infer correlation or state
from message names.

## Registration and delivery

Use the EF Core outbox and context registration from the [saga guide](sagas.md#explicit-registration).
Register a matching due-row worker against the same database, schema and table prefix:

```csharp
using Relayer.Transports;
using Relayer.Outbox.PostgreSql;
using Relayer.Kafka;

services.AddLogging();
services.AddSingleton<TimeProvider>(TimeProvider.System);
// dataSource targets the same database as OrdersDbContext; the host owns its lifetime.
services.AddRelayerPostgreSqlMaintenance("process-timeouts", dataSource);
services.AddRelayer(relayer =>
{
    relayer.Infrastructure().MessageTypes.Add<BeginWait>("orders.begin-wait");
    relayer.Infrastructure().MessageTypes.Add<WaitExpired>("orders.wait-expired");
    relayer.UseJsonSerializer(OrderJsonContext.Default); // Source-generated metadata for both contracts.
    relayer.Kafka("events", kafka =>
    {
        // Provision topics externally; default Validate checks that they exist.
        kafka.Options.Configure(options => options.BootstrapServers.Add("localhost:9092"));
        kafka.Consume("order-starts", consumer =>
        {
            consumer.Options.Group = "orders-process-starts";
            consumer.Handler<BeginOrderWait>()
                .Saga<OrdersDbContext, OrderState>("orders.process");
        });
        kafka.Consume("order-timeouts", consumer =>
        {
            consumer.Options.Group = "orders-process-timeouts";
            consumer.Handler<ExpireOrderWait>()
                .Saga<OrdersDbContext, OrderState>("orders.process");
        });
        kafka.Send<WaitExpired>("order-timeouts", route => route.SendOptions = new()
        {
            RequiredDeliveryDelay = TimeSpan.FromDays(1),
            TransactionRequirement = SendTransactionRequirement.Outbox
        });
    });
});
```

The Kafka consumer declarations register both handlers and configure the existing saga receive pipeline at startup. Other transports use the same consumer handler and pipeline callbacks with their native registration.

Native acknowledgement follows the saga transaction commit.
Define `OrderJsonContext` with System.Text.Json source generation, or use generated
`AddMessageTypesFromThisAssembly()` registration as described in the [messages guide](messages.md).

Maintenance handles unordered schedules. An ordered route needs the matching
[ordered lane dispatcher](../reference/ordered-dispatch.md), including the writer's lane count.
A commit-only direct dispatcher cannot deliver future rows. Startup and send-time validation
reject missing or mismatched scheduling support. Even a broker with native scheduling needs the
due-row worker here: `ISender` enlists the schedule in the database transaction. Direct native
scheduling cannot atomically commit the process state with the timeout.

Delivery is at least once and no earlier than `DeliverAt`; it is not an exact deadline. Worker
polling, recovery delay, broker availability and consumer load can delay processing. The worker
uses PostgreSQL time for eligibility. Use UTC and keep application and database clocks aligned.
If the process must distinguish late delivery from a business deadline, persist that deadline
in its state and evaluate it explicitly.

## Completion, duplicates and failure

- The consumer inbox skips redelivery of the same message ID within its configured retention.
  The wait identity also prevents a matching timeout from applying twice after that window.
- A timeout for a superseded or cleared wait does no business work. Its ordinary saga save still
  checks optimistic concurrency. Conflicts retry the complete transition in a fresh scope through
  the existing failure policy.
- Completed processes retain their rows with `IsCompleted = true`. The runtime skips their
  timeout transition, and the inbox records the delivery. Completion does not remove an already
  committed scheduled message; it becomes harmless when delivered.
- Missing state fails explicitly with `InvalidOperationException`. Do not override `Create` on
  the timeout handler: timeouts must never start processes. Retain completed rows and configure
  the existing failure policy to dead-letter missing-state messages when they indicate invalid
  correlation or deliberate state deletion.
- An exception, cancellation or retry/dead-letter request before commit rolls back state,
  inbox identity and the scheduled send together. Redelivery can schedule the wait once. Errors
  during timeout processing also roll back its state and inbox identity together.

For cancellation or rescheduling, change the wait identity in the same consumer transaction as
other state changes. This logically invalidates the previous timeout without racing a physical
scheduler cancellation. Do not delete outbox rows or add an application timer, second inbox or
retry loop. See [sending](sending.md) for the existing scheduling and transaction requirements.
