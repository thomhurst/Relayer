---
title: RabbitMQ Streams
description: "Native RabbitMQ Stream systems and stream topology, with message execution still planned."
---

`Relayer.RabbitMQ.Streams` manages named native `StreamSystem` instances and stream topology using `RabbitMQ.Stream.Client` 1.12.2. **Relayer sending and receiving are not implemented.** Declaring `Send` or `Consume` fails before connecting. Direct `ITransport.SendAsync` and `SendBatchAsync` calls also fail explicitly.

## Register a connection and topology

Enable the broker's `rabbitmq_stream` plugin and expose its stream protocol port, normally 5552. This package uses the stream protocol, independently of the [RabbitMQ AMQP transport](rabbitmq.md).

```csharp
using System.Net;
using Microsoft.Extensions.Options;
using RabbitMQ.Stream.Client;
using Relayer;
using Relayer.RabbitMQ.Streams;
using Relayer.Transports;

services.AddRelayer(relayer => relayer.RabbitMQStreams("events", streams =>
{
    streams.TopologyMode = TopologyMode.Provision;
    streams.Options.Configure(options =>
    {
        options.Configuration = new StreamSystemConfig
        {
            Endpoints = [new DnsEndPoint("localhost", 5552)],
            UserName = "app",
            Password = "configured-secret",
        };
        options.Streams.Add(new("orders")
        {
            Configuration = new StreamSpec("orders")
            {
                MaxAge = TimeSpan.FromDays(7),
            },
        });
    });
}));
```

`Configuration` is the native `StreamSystemConfig` instance, passed unchanged. Native authentication, TLS, endpoint resolution, connection pools and locator recovery remain native behavior. DI resolution does not connect. Startup creates the owned system and checks native server properties. Shutdown and disposal close only that named system; externally created systems remain caller-owned and cannot be supplied to this registration.

Native operations do not accept a `CancellationToken`. Cancellation is checked before and after each operation; an in-flight operation finishes according to native connection/RPC timeouts before Relayer returns or disposes its connection. Set native timeouts for the deployment. Relayer never abandons a pending connection task.

## Topology modes

`None` performs no topology requests. Host startup still opens its native system. `Validate`, the default, uses native `StreamExists` without creating a producer, consumer or probe message. Name-only declarations check existence:

```csharp
options.Streams.Add(new("orders"));
```

The stream protocol does not expose retention/configuration readback through this client. In `Validate` mode, a declaration with `Configuration` reports that configuration equivalence cannot be checked and fails startup, even when the stream exists. Use a name-only declaration when existence is the intended assertion.

`Provision` passes each supplied `StreamSpec` unchanged to native `CreateStream`. Its name must match the declaration. The broker enforces native argument equivalence for existing streams; conflicts appear in the topology report. Relayer continues with unrelated declarations and never calls update, delete or purge operations. Name-only declarations still check existence and do not create missing streams.

Super-stream provisioning and stream consumer declarations remain planned. A stream declaration does not start a Relayer receiver.

## Message execution status

Full execution remains open under [#316](https://github.com/thomhurst/Relayer/issues/316). The native reliable consumer catches handler exceptions and advances its reconnect offset past failed processing. A failure while persisting handler work or a dead-letter copy must not allow later offsets to skip that work. [rabbitmq-stream-dotnet-client#491](https://github.com/rabbitmq/rabbitmq-stream-dotnet-client/pull/491) merged a native `ConsumerConfig.SubscriptionListener` that can override the subscription offset before each subscription, including reconnects and super-stream partitions, closing [#489](https://github.com/rabbitmq/rabbitmq-stream-dotnet-client/issues/489). The latest released client, 1.12.2, does not include it. Execution remains on hold until a released client supplies that listener; Relayer does not replace native recovery to work around the release gate.

The planned adapter will use native stored offsets to resume after the last durable processing boundary. It must stop processing after an unsettled failure and preserve that boundary when offset lookup fails: a listener exception must not allow native recovery to fall back past unsettled work. These safeguards still require implementation and real-broker validation after adopting the released API.

Streams use log offsets, not per-message acknowledgement, requeue or native dead-letter settlement. This foundation advertises no scheduling, redelivery, dead-letter, ordering-by-key, replay or transaction support. Future execution must validate supported guarantees at startup and use the existing core pipelines and durable failure handling.

Real-broker topology, lifecycle and locator recovery tests cover this foundation. Native AOT smoke coverage checks registration and native message serialization. Transport conformance, creation-context propagation and native-versus-Relayer message benchmarks remain required for future execution; this foundation makes no message-path performance or delivery-guarantee claim.
