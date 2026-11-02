---
title: RabbitMQ
description: "Confirmed RabbitMQ publishing, optional native delays and explicit topology validation."
---

`Relayer.RabbitMQ` wraps RabbitMQ.Client 7. Named connection lifecycle and exchange, queue and queue-binding declarations are implemented. Confirmed outgoing sends, binary CloudEvents headers, send telemetry and verified delayed-exchange scheduling are implemented. **Consuming and native health adapters remain planned.** The complete transport conformance suite will run when those adapters are available.

## Provision native topology

```csharp
using RabbitMQ.Client;
using Relayer;
using Relayer.RabbitMQ;

services.AddRelayer(relayer => relayer.RabbitMQ("orders", rabbit =>
{
    rabbit.TopologyMode = TopologyMode.Provision; // Explicit development/bootstrap operation.
    rabbit.Options.Configure(options =>
    {
        options.ConnectionFactory = new ConnectionFactory
        {
            HostName = "localhost",
            UserName = "application",
            Password = "application",
        };
        options.Exchanges.Add(new("orders") { Type = ExchangeType.Topic });
        options.Exchanges.Add(new("orders.dead"));
        options.Queues.Add(new("orders")
        {
            Arguments = new Dictionary<string, object?>
            {
                ["x-dead-letter-exchange"] = "orders.dead",
                ["x-dead-letter-routing-key"] = "failed",
            },
        });
        options.Queues.Add(new("orders.dead"));
        options.Bindings.Add(new("orders", "orders", "placed.*"));
        options.Bindings.Add(new("orders.dead", "orders.dead", "failed"));
    });
}));
```

Run the application host to apply topology during startup. Relayer declares exchanges first, queues second and bindings last. RabbitMQ checks declaration equivalence: an existing entity with incompatible properties produces an actionable startup difference. Relayer never deletes or changes existing entities. Provisioning can create some entities before another declaration fails; it is not transactional.

Unspecified exchange properties provision a durable, non-auto-delete direct exchange. Unspecified queue properties provision a durable, non-exclusive, non-auto-delete queue. Specify native arguments directly, including DLX and queue-type arguments. RabbitMQ owns routing and dead-lettering. Declaring a DLX does not enable Relayer receive settlement or its error-metadata dead-letter capability yet.

Queues need explicit names. Server-generated queue names and binding the default exchange are not supported by these declarations. Routing keys may be empty. Exchange and queue names occupy separate namespaces.

## Validate existing entities

The default [topology mode](../guides/topology.md) is `Validate`. RabbitMQ.Client's passive AMQP declarations can verify **existence**, but cannot read exchange type, durability, exclusivity, auto-delete, arguments or bindings. For existence-only validation, declare just names:

```csharp
services.AddRelayer(relayer => relayer.RabbitMQ("orders", rabbit =>
{
    rabbit.Options.Configure(options =>
    {
        options.ConnectionFactory = nativeConnectionFactory;
        options.Exchanges.Add(new("orders"));
        options.Queues.Add(new("orders"));
    });
}));
```

Null declaration properties mean no property expectation in `Validate`; they select the defaults above in `Provision`. Explicit properties or arguments in `Validate` produce unresolved startup differences even when the entity exists. Every binding declaration also produces an unresolved difference: AMQP has no read-only binding inspection operation. Use infrastructure tooling to verify those settings and bindings, or explicitly run `Provision` with configure permissions for native equivalence checks and binding declarations. Provisioning uses RabbitMQ's own equivalence rules, including any broker-defined exceptions.

Validation does not publish probe messages, consume messages, bind queues or create missing entities. Missing entities close their individual native channels; checks continue on other channels so one missing entity does not hide another. `None` skips topology operations; hosted startup still opens the native connection.

## Connection ownership and native configuration

Each named registration owns one connection, created lazily during topology application or hosted startup. Resolving options, the adapter or `ITopology` does not connect. Host shutdown closes the connection. Other connections remain independent. Each topology operation disposes its channel; RabbitMQ.Client retains its own connection-level topology recovery records.

`ConnectionFactory` is passed through unchanged. Configure TLS, endpoints, heartbeats, native automatic recovery, topology recovery and `ICredentialsProvider` on that factory. Relayer does not implement another credential-refresh or recovery loop. Configure options before startup; live connection reconfiguration is not implemented.

`Consume` declarations fail before opening a connection until the receive adapter is implemented. Error-metadata dead-letter support remains unavailable. Full transport composition is tracked under [RabbitMQ transport #40](https://github.com/thomhurst/Relayer/issues/40).

## Confirmed sends

Declare a route to a queue by name, or use `exchange:routing-key` to publish through an exchange. The first colon separates the exchange from the routing key; use an empty exchange (`:queue`) for the default exchange. An exchange name in this notation cannot contain a colon. Empty routing keys are supported.

```csharp
services.AddRelayer(relayer =>
{
    relayer.AddMessageTypesFromThisAssembly();
    relayer.RabbitMQ("orders", rabbit =>
    {
        rabbit.Options.Configure(options => options.ConnectionFactory = nativeConnectionFactory);
        rabbit.Send<OrderPlaced>("orders:placed.eu");
    });
});

await sender.SendAsync(new OrderPlaced(orderId), options => options.RabbitMQ(properties =>
{
    properties.Priority = 3;
    properties.AppId = "billing";
}));
```

Register the message with a stable `[Message]` identity and a source-generated JSON serializer as in the [sending guide](../guides/sending.md). Topology declarations remain independent: sending does not implicitly create queues or bindings.

Relayer leases one native channel exclusively for each publish and holds it until RabbitMQ.Client completes confirmation tracking. `PublisherCount` defaults to 16. A concurrent sender waits for an available channel with its cancellation token. Relayer does not share a publishing channel between concurrent sends. Sequential batches use ordinary confirmed publishes and can partially succeed; they are not atomic. Each native publish has its own send span and operation duration. Concurrent callers have RabbitMQ's native ordering semantics, with no ordering-by-key guarantee.

Immediate publishes use `mandatory: true` and persistent basic properties. RabbitMQ.Client propagates nacks, unroutable `basic.return` responses, protocol errors and connection failures. Confirmation is the broker's durability boundary, not proof of consumer processing. Queue/exchange durability and queue type still determine the native durability guarantees. Cancellation or a connection failure can leave delivery uncertain; Relayer does not retry a publish automatically. Native automatic recovery remains owned by RabbitMQ.Client. A channel closed by a protocol error is replaced before the next send.

Set native channel settings through `PublisherChannelOptions` (`CreateChannelOptions`), passed unchanged. Both confirmation flags must be enabled; startup rejects disabling either. `SendOptions.RabbitMQ` exposes borrowed native `BasicProperties`. Do not retain that pooled object after the callback/send. The portable headers, serializer content type, delivery time and persistent delivery mode take precedence over conflicting native callbacks. Other native properties pass through.

CloudEvents attributes use AMQP `cloudEvents:` headers. The serializer's `content-type` becomes native `BasicProperties.ContentType`. Existing creation `traceparent` and `tracestate` are preserved even when the native publisher instrumentation is enabled. The pinned client only accepts strings or exact byte arrays for textual field-table values; Relayer reuses exact-length buffers for borrowed UTF-8 slices. Buffers grow on first use of a header position/length and are retained by each publisher. Send completion sources are retained at the observed concurrency high-water mark, including when continuations complete on another thread.

Host shutdown stops new admission and drains publishers before closing their connection. A cancellation during shutdown closes the connection to release native operations. Always start the application host before sending.

## Native delays and the outbox fallback

Native delays require the delayed-message plugin **and a successfully verified delayed exchange**. Configure an alternate delayed exchange for each ordinary exchange that needs scheduling. An empty key selects routes through the default exchange:

```csharp
rabbit.TopologyMode = TopologyMode.Provision;
rabbit.Options.Configure(options =>
{
    options.DelayedExchanges.Add("", new("orders.delayed")
    {
        Type = "x-delayed-message",
        Arguments = new Dictionary<string, object?> { ["x-delayed-type"] = "direct" },
    });
});
rabbit.Send<OrderPlaced>("orders");
```

Bind `orders.delayed` to `orders` with routing key `orders` through infrastructure tooling or native APIs before scheduling. Delayed exchange declarations pass their native durability, auto-delete and argument settings through unchanged. They do not create bindings or change the immediate destination. Do not also declare an optional delayed exchange in `Exchanges`: regular topology declarations are required and a missing plugin makes those declarations fail.

During hosted startup, explicit `Provision` verifies the alternate exchange with native `ExchangeDeclareAsync`. Only the broker's specific unknown-exchange-type response selects fallback. Permission failures, incompatible declarations and other errors fail startup. A successful declaration enables delays less than `2^32` milliseconds, rounded up to the next millisecond; immediate sends continue using the ordinary exchange. Expired delivery times publish immediately.

AMQP has no read-only API to inspect installed plugins or exchange types. `None` and `Validate` therefore do not probe, create or advertise delayed exchanges, even when a plugin is installed. These modes use conservative capabilities and the existing [outbox scheduling](../guides/sending.md) fallback. When the plugin is absent in `Provision`, the alternate exchange is not created and scheduling also uses that fallback. The core requires an active outbox transaction and a due-row dispatch worker; direct future sends fail clearly when no verified native delay is available. Dispatch of a due outbox row publishes immediately through the ordinary destination.

The plugin stores delayed messages using its own native durability and routing guarantees. It cannot report eventual routability at enqueue time: delayed publishes use native confirms with `mandatory: false`. A confirm does not guarantee that a binding will exist when the delay expires, or replication of the plugin's local message store. Choose the outbox fallback when those native plugin guarantees do not meet the application's requirements.

## Send telemetry

Enable source `Relayer` and meter `Relayer`. Send destinations use `{exchange}:{routing key}`, including `:orders` for a default-exchange queue. Routing key, system, destination and configured server address/port are available to samplers when spans start. Single sends preserve message creation links and use the common CloudEvents attributes. Sends record attempt counts and client operation duration, including native confirmation time; failures carry the bounded `_OTHER` error type.

RabbitMQ.Client 7.2.2 already owns transport-level publisher instrumentation. Enable `RabbitMQ.Client.Publisher` for those additional child spans; Relayer neither replaces it nor changes `RabbitMQActivitySource`'s process-wide configuration. Receive instrumentation (`RabbitMQ.Client.Subscriber`) belongs to the native client; Relayer's receive adapter remains planned.
