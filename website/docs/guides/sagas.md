---
title: Saga state transitions
description: "Explicit PostgreSQL process state using the EF Core consumer inbox and outbox."
---

`Relayer.Sagas` supplies typed process execution. Install `Relayer.Sagas.EntityFrameworkCore`
to persist explicit process managers inside the existing
[EF Core consumer transaction](../reference/ef-core-inbox.md). A process is an ordinary state
class and a generated `IHandler<T>` dispatcher. There is no scanning, independent transaction,
lock provider, second inbox or second outbox.

Register process handlers through the generated `Handler<T>()` API. This also registers their
typed execution services. Use the [test harness](testing.md) or a composed consumer to invoke them
with the delivery's service scope.

## Handler and state

```csharp
using Microsoft.EntityFrameworkCore;
using Relayer;
using Relayer.Kafka;
using Relayer.Sagas.EntityFrameworkCore;

public sealed class OrderState : EntityFrameworkCoreSagaData
{
    public int ItemsReceived { get; set; }
}

public sealed class OrderProcess(OrdersDbContext db, ISender sender)
    : EntityFrameworkCoreSaga<OrdersDbContext, OrderState, ItemReceived>(db)
{
    protected override ValueTask<OrderState?> LoadAsync(ItemReceived message, CancellationToken cancellationToken)
        => new(Database.Orders.SingleOrDefaultAsync(s => s.Id == message.OrderId, cancellationToken));

    // Only starting messages override Create. Missing state otherwise fails explicitly.
    protected override OrderState Create(ItemReceived message) => new() { Id = message.OrderId };

    protected override async ValueTask TransitionAsync(OrderState state, ItemReceived message,
        MessageContext context, CancellationToken cancellationToken)
    {
        state.ItemsReceived++;
        if (state.ItemsReceived == message.ExpectedItems)
        {
            state.IsCompleted = true;
            await sender.SendAsync(new OrderReady(state.Id), cancellationToken);
        }
    }
}

[Message("orders.item-received")]
public sealed record ItemReceived(Guid OrderId, int ExpectedItems);
[Message("orders.ready")]
public sealed record OrderReady(Guid OrderId);
```

The runtime loads tracked state, creates it when absent, skips completed state, invokes the
transition, and calls `SaveChangesAsync`. Retain completed rows as tombstones: deleting a row
allows a new starting message to recreate that process. Missing non-starting state throws
`InvalidOperationException` and enters the configured failure policy. Override `Create` only
for messages allowed to start a process. Multiple message handlers can use the same state type.

## Explicit registration

Configure the native EF model without a Relayer model builder:

```csharp
public sealed class OrdersDbContext(DbContextOptions<OrdersDbContext> options) : DbContext(options)
{
    public DbSet<OrderState> Orders => Set<OrderState>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        var state = modelBuilder.Entity<OrderState>();
        state.HasKey(s => s.Id);
        state.Property(s => s.Id).ValueGeneratedNever();
        state.Property(s => s.Version).IsRowVersion();
    }
}
```

Npgsql maps the `uint` row version to PostgreSQL's native `xmin` column. EF checks that version
when saving updates. Use application-owned migrations for the state table and the existing
[PostgreSQL schema](../reference/postgres-schema.md) for inbox and outbox tables.

```csharp
using Relayer.Transports;
services.AddRelayerEntityFrameworkCoreOutbox();
services.AddDbContext<OrdersDbContext>((services, options) => options
    .UseNpgsql(connectionString)
    .AddRelayerOutbox(services));
services.AddRelayer(relayer =>
{
    relayer.Infrastructure().MessageTypes.Add<ItemReceived>("orders.item-received");
    relayer.Infrastructure().MessageTypes.Add<OrderReady>("orders.ready");
    relayer.Kafka("events", kafka =>
    {
        kafka.Options.Configure(o => o.BootstrapServers.Add("localhost:9092"));
        kafka.Send<OrderReady>("orders.ready");
        kafka.Consume("orders.item-received", consumer =>
        {
            consumer.Options.Group = "order-process";
            consumer.Saga<OrdersDbContext, OrderState>("orders.process");
            consumer.Handler<OrderProcess>();
        });
    });
});
```

Import `Relayer.Outbox.EntityFrameworkCore` for outbox registration. `Saga<TContext,TState>` is
the first middleware, replaces the ordinary `EntityFrameworkCore<TContext>` declaration, and
validates the state key, native row version and selected outbox at composition. Use one state type
per process consumer; that consumer can handle several message types for the same state.

Use the same scoped context for process state, other business changes and `ISender`. Only the
consumer boundary commits. Errors, cancellation and retry/dead-letter requests roll back state,
inbox identity and outgoing messages together. Native acknowledgement occurs after commit.
Redelivery of the same UUIDv7 message ID skips the transition through the existing inbox.

## Concurrency and retries

Configure the existing `FailurePipeline` with a bounded retry policy. Both
`DbUpdateConcurrencyException` on updates and PostgreSQL unique-key errors on concurrent creation
must be classified as `FailureAction.Retry` (the default classifier already does this). Do not
catch these errors inside the transition. Each retry reloads state and executes the whole transition
in a fresh delivery scope. EF's `EnableRetryOnFailure` is also supported: the
[consumer boundary](../reference/ef-core-inbox.md#ef-core-connection-retries) applies the native
strategy to the complete transition in a fresh scope for transient connection faults. Exhausted retries use
the consumer's existing redelivery/dead-letter policy. External HTTP or native broker calls are
outside this database transaction; send through `ISender` to enlist outgoing work.

State queries remain explicit application code. The provider-independent `Saga<TState,TMessage>`
runtime and generated dispatch support Native AOT without reflection or runtime dispatch generation.
The EF integration inherits EF Core's own trimming and Native AOT restrictions; the runtime does
not make the provider AOT-compatible. EF applications need their provider's supported compiled
model and precompiled-query setup. Native AOT smoke coverage exercises typed execution separately
from EF persistence; PostgreSQL integration tests cover the transaction and concurrency behavior.

Use the existing [process diagram API](saga-diagrams.md) for declarations and export. Diagram
`SagaState` metadata and persisted `SagaData` are separate: diagrams do not generate or enforce
application transitions. [Durable process timeouts](saga-timeouts.md) use ordinary typed messages
and existing transactional scheduled sends.
