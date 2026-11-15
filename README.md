# Relayer

**Messaging for .NET without the boilerplate.** Relayer wraps the native client for your broker and takes care of everything you would otherwise write around it: retries, dead-lettering, the outbox, tracing, serialization, topology and testing.

> **Status:** early development. Pre-1.0 packages are on [NuGet](https://www.nuget.org/packages/Relayer), and APIs may still change. The [roadmap](https://relayer.tomlonghurst.dev/docs/roadmap) lists what works today.

## The name

Relayer is two words in one:

- **Relay**: it passes messages from one part of your system to another.
- **Layer**: it is a thin layer over your broker's own client, which hides the complexity and repetitive code and leaves the broker's behavior alone.

## Why use it

Sending a message is easy. Running messaging in production is not. Around every send and receive you need retries, a place for poison messages to go, a way to commit a message together with a database change, trace propagation, serialization and tests. Most teams rewrite this for every service, slightly differently each time.

Relayer gives you that code once, tested, with safe defaults:

| You get | What it means |
|---|---|
| **Failure handling** | Retries, delayed redelivery and dead-lettering with error history. A bad message never crashes or stalls a consumer. |
| **Outbox and inbox** | Save a database change and its outgoing message in one transaction. Duplicate deliveries are filtered out on the way in. |
| **Observability** | OpenTelemetry traces and metrics, with trace context carried across the broker. |
| **Serialization** | Source-generated System.Text.Json by default, with Protobuf, MessagePack and Avro as options. |
| **Topology** | Declare queues and topics in code. Relayer validates them at startup, or provisions them where the broker allows. |
| **Testing** | An in-memory transport and a test harness, so you can test message flows without a broker. |

## What it looks like

Declare a message with a stable name:

```csharp
[Message("orders.placed", Version = 1)]
public sealed record OrderPlaced(string OrderId, string CustomerId);
```

Write a handler:

```csharp
public sealed class OrderPlacedHandler(Billing billing) : IHandler<OrderPlaced>
{
    public async ValueTask HandleAsync(OrderPlaced message, MessageContext context, CancellationToken cancellationToken)
    {
        await billing.CreateInvoiceAsync(message, cancellationToken);
    }
}
```

Connect them to a broker:

```csharp
builder.Services.AddRelayer(relayer =>
{
    relayer.AddMessageTypesFromThisAssembly();
    relayer.Kafka("events", kafka =>
    {
        kafka.Options.Configure(options => options.BootstrapServers.Add("localhost:9092"));

        kafka.Send<OrderPlaced>("orders", route => route.OrderingKey(order => order.CustomerId));

        kafka.Consume("orders", consumer =>
        {
            consumer.Options.Group = "billing";
            consumer.Concurrency(8);
            consumer.Handler<OrderPlacedHandler>();
        });
    });
});
```

Send from anywhere:

```csharp
await sender.SendAsync(new OrderPlaced(orderId, customerId));
```

That is a complete setup. Failed messages are retried and then dead-lettered, offsets are committed only after the handler finishes, messages with the same key stay in order, and every hop is traced.

Need the message and a database change to commit together? Add the outbox and send inside your transaction:

```csharp
await using var transaction = await db.Database.BeginTransactionAsync(cancellationToken);
db.Orders.Add(order);
await sender.SendViaOutboxAsync(new OrderPlaced(order.Id, order.CustomerId), cancellationToken);
await db.SaveChangesAsync(cancellationToken);
await transaction.CommitAsync(cancellationToken);
```

## How it works

**It wraps, it does not replace.** Each transport is a thin adapter over the broker's native client. Sending, receiving, ordering and acknowledgment work exactly as that client does them. Relayer never emulates a feature the broker already has.

**Nothing is hidden from you.** Native configuration passes through unchanged, and the native message is always one call away. Broker-specific features are typed extensions such as `context.Kafka()`, not a lowest-common-denominator option.

**No magic.** There is no assembly scanning and no reflection on the message path. A source generator writes the registration and dispatch code at compile time, and you can read it. Analyzers catch mistakes as you type, and startup validation catches the rest before the first message moves.

**Performance is a feature.** The budget is at most 5% throughput overhead against the raw client and zero allocations per message from Relayer's own pipeline. Relayer is Native AOT and trimming safe.

## Supported brokers

| Broker | Native client |
|---|---|
| Apache Kafka | [Dekaf](https://github.com/thomhurst/Dekaf) |
| Azure Service Bus | `Azure.Messaging.ServiceBus` |
| Amazon SQS and SNS | `AWSSDK.SQS`, `AWSSDK.SimpleNotificationService` |
| RabbitMQ | `RabbitMQ.Client` |
| NATS JetStream | `NATS.Client.JetStream` |
| Google Cloud Pub/Sub | `Google.Cloud.PubSub.V1` |
| In-memory | Built in, for tests and local development |

Outbox stores: PostgreSQL, SQL Server and MySQL. RabbitMQ Streams and Apache Pulsar are planned. See [transports](https://relayer.tomlonghurst.dev/docs/transports) for the status of each.

## Try it

Relayer targets .NET 10 and later. Add the core package and the transport for your broker:

```shell
dotnet add package Relayer
dotnet add package Relayer.Kafka
```

Or run a sample from the repository, which needs the .NET 11 SDK and the .NET 10 runtime. This one runs without a broker:

```shell
git clone https://github.com/thomhurst/Relayer.git
cd Relayer
dotnet run -c Release --project samples/ModularMonolith
```

More in [`samples/`](https://github.com/thomhurst/Relayer/tree/main/samples): Kafka with an EF Core outbox, an Azure Service Bus web API and an Aspire app host.

## Documentation

- [Getting started](https://relayer.tomlonghurst.dev/docs/getting-started): a message, a handler and a route, step by step
- [Guides and reference](https://relayer.tomlonghurst.dev/): the full documentation site
- Migrating from [MassTransit](https://relayer.tomlonghurst.dev/docs/migration/masstransit), [Rebus](https://relayer.tomlonghurst.dev/docs/migration/rebus) or [CAP](https://relayer.tomlonghurst.dev/docs/migration/cap)
- [Design](https://github.com/thomhurst/Relayer/blob/main/docs/design.md) and [decision record](https://github.com/thomhurst/Relayer/blob/main/docs/decisions.md): how Relayer works, and why

## Contributing

```shell
dotnet build -c Release
dotnet test -c Release --project tests/Relayer.Tests --treenode-filter "/*/*/ClassName/*"
```

See the contributing guides for [testing](https://relayer.tomlonghurst.dev/docs/contributing/testing), [performance](https://relayer.tomlonghurst.dev/docs/contributing/performance) and [documentation](https://relayer.tomlonghurst.dev/docs/contributing/documentation).

## License

MIT
