---
title: RabbitMQ Streams
description: "Native RabbitMQ Stream systems and stream topology, with message execution still planned."
---

This package is not published until it supports both send and receive ([#316](https://github.com/thomhurst/Relayer/issues/316)). Source integration and tests remain available in the repository.


`Relayer.RabbitMQ.Streams` manages named native `StreamSystem` instances and stream topology using `RabbitMQ.Stream.Client` 1.13.0. **Relayer sending and receiving are not implemented.** Declaring `Send` or `Consume` fails before connecting. Direct `ITransport.SendAsync` and `SendBatchAsync` calls also fail explicitly.

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

Message execution is tracked in [#316](https://github.com/thomhurst/Relayer/issues/316). RabbitMQ.Stream.Client 1.13.0 supplies [`ConsumerConfig.SubscriptionListener`](https://github.com/rabbitmq/rabbitmq-stream-dotnet-client/blob/v1.13.0/RabbitMQ.Stream.Client/Reliable/Consumer.cs#L108-L116), which can override the starting offset on each subscription, including reconnects and super-stream partitions. By default, the reliable consumer resumes after the last consumed offset. The planned receive path must resume from durable settlement so a failed handler or dead-letter write can be retried after reconnect. If the listener throws, the native client keeps its default offset, so recovery must handle offset lookup failures before returning from the listener.

Graceful shutdown remains an upstream prerequisite. In 1.13.0, the reliable consumer can return from close while a message handler is still running. If the handler then tries to store its offset through the consumer, the closed connection can reject the checkpoint. [RabbitMQ Stream .NET Client #498](https://github.com/rabbitmq/rabbitmq-stream-dotnet-client/issues/498) tracks a native drain that lets in-flight handlers finish and store their offsets before disconnecting.

The client's [locator connection can store offsets after consumer close](https://github.com/rabbitmq/rabbitmq-stream-dotnet-client/issues/498#issuecomment-7041377838) through `StreamSystem.StoreOffset(reference, stream, offset)`. On RabbitMQ 4.1.4 with client 1.13.0, this worked for a standard stream and for each partition of a super stream when called with its partition stream name. In both cases, `Consumer.Close()` returned before the running callbacks finished. The caller still had to wait for those callbacks and keep the `StreamSystem` open for their checkpoints.

If startup reports the drain prerequisite, remove send routes and consumers from the RabbitMQ Streams registration to use its connection and topology support.

Streams use log offsets, not per-message acknowledgement, requeue or native dead-letter settlement. This foundation advertises no scheduling, redelivery, dead-letter, ordering-by-key, replay or transaction support. Future execution must validate supported guarantees at startup and use the existing core pipelines and durable failure handling.

Real-broker topology, lifecycle and locator recovery tests cover this foundation. Native AOT smoke coverage checks registration and native message serialization. Transport conformance, creation-context propagation and native-versus-Relayer message benchmarks remain required for future execution; this foundation makes no message-path performance or delivery-guarantee claim.
