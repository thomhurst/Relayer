---
title: NATS JetStream
description: "Native NATS connection ownership and JetStream topology, with message execution still planned."
---

`Relayer.Nats` currently manages named native connections and explicit JetStream stream/consumer topology. **Relayer send and receive execution are not implemented.** Declaring `Send` or `Consume` fails before connecting. Calling `ITransport.SendAsync` or `SendBatchAsync` also fails explicitly.

## Register a connection and topology

```csharp
using Microsoft.Extensions.Options;
using NATS.Client.Core;
using NATS.Client.JetStream.Models;
using Relayer;
using Relayer.Nats;

services.AddRelayer(relayer => relayer.Nats("events", nats =>
{
    nats.TopologyMode = TopologyMode.Provision;
    nats.Options.Configure(options =>
    {
        options.ConnectionOptions = new NatsOpts
        {
            Url = "nats://localhost:4222",
        };
        options.Streams.Add(new("orders")
        {
            Configuration = new StreamConfig("orders", ["orders.>"]),
        });
        options.Consumers.Add(new("orders", "billing")
        {
            Configuration = new ConsumerConfig("billing")
            {
                FilterSubject = "orders.placed",
            },
        });
    });
}));
```

The NATS server must have JetStream enabled. `ConnectionOptions` is the native `NatsOpts` instance, passed unchanged to an owned `NatsConnection`. Its authentication, TLS, logger factory and connection recovery remain native behavior. Optional `JetStreamOptions` passes an unchanged `NatsJSOpts` instance for native domains, API prefixes and request timeouts. DI resolution does not connect. Startup opens the connection with the native ping operation; shutdown and disposal close only that named connection. Caller-owned connections are not accepted.

## Topology modes

`None` performs no topology requests. Starting the host still opens its connection. `Validate` is the default and only reads native stream and consumer information; it never publishes probe messages, creates consumers, or changes broker settings.

For existence-only validation, declare names without `Configuration`:

```csharp
options.Streams.Add(new("orders"));
options.Consumers.Add(new("orders", "billing"));
```

Full read-only configuration equivalence checks remain planned. A declaration with `Configuration` in `Validate` mode reports that limitation as an actionable topology difference and fails startup. It never silently treats matching names as proof that native settings match. Name-only declarations intentionally check existence only.

`Provision` passes each supplied `StreamConfig` to native `CreateStreamAsync` and each supplied `ConsumerConfig` to native `CreateConsumerAsync`. These are create-only operations: Relayer never calls the update, delete, purge, or create-or-update APIs. The server performs its native configuration-equivalence checks for existing entities. Native conflicts appear in the topology report; unrelated declarations can still be provisioned. A name-only entry only checks existence, even in `Provision` mode. Add native `Configuration` to create a missing entity.

Native configuration names must match their declaration. An optional native `DurableName` must also match the declared consumer name. Consumer declarations manage broker entities; they do not start a Relayer message receiver.

## Message execution status

Send/receive pipelines, settlement, dead-letter copies, trace propagation, conformance tests and native-client performance comparisons remain planned under [#313](https://github.com/thomhurst/Relayer/issues/313). No message-path capabilities are advertised by this foundation.

The native outgoing header API currently requires string values. Relayer's default binary CloudEvents path keeps changing header values in UTF-8 buffers; decoding each value into a new string would add allocations per send. [nats.net#1285](https://github.com/nats-io/nats.net/issues/1285) requests a public native path for byte-backed header values. Message execution remains blocked on a suitable native API, rather than bypassing the native publish protocol or weakening Relayer's allocation contract.

This foundation has real-server topology/lifecycle tests and Native AOT registration/native configuration serialization coverage. It does not claim transport conformance or message-path performance compliance.
