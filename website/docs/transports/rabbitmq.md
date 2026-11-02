---
title: RabbitMQ
description: "Native RabbitMQ connection and topology management, with explicit validation limits."
---

`Relayer.RabbitMQ` wraps RabbitMQ.Client 7. Named connection lifecycle and exchange, queue and queue-binding declarations are implemented. **Sending, consuming, publisher confirms, delayed-plugin detection, telemetry and native health adapters remain planned.** The complete transport conformance suite will run when those adapters are available.

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

`Send` and `Consume` declarations currently fail before opening a connection, with a message explaining that execution is not implemented. The conservative capability declaration continues to advertise immediate requeue, with no verified delayed delivery or error-metadata dead-letter support. Full outgoing and receive composition are tracked under [RabbitMQ transport #40](https://github.com/thomhurst/Relayer/issues/40).
