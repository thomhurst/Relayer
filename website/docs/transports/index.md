---
title: Transports
description: "The native clients Relayer wraps for each broker, their status, and how to write a transport of your own."
---

A transport is an adapter over one native client. Each transport package depends only on its native client and supports only that client's latest major version.

| Package | Broker | Native client | Status |
|---|---|---|---|
| `Relayer` | [In-memory](in-memory.md), for tests and samples | Built in | Implemented |
| `Relayer.Kafka` | [Apache Kafka](kafka.md) | [Dekaf](https://dekaf.tomlonghurst.dev/), a pure C# Kafka client | Producer implemented; consumers planned |
| `Relayer.AzureServiceBus` | Azure Service Bus | `Azure.Messaging.ServiceBus` | [Native sending and topology](service-bus.md), [receive primitives](service-bus-receive.md) implemented; receiver composition in development |
| `Relayer.AmazonSqs` | Amazon SQS and SNS | `AWSSDK.SQS` and `AWSSDK.SimpleNotificationService` | In development |
| `Relayer.RabbitMQ` | RabbitMQ | `RabbitMQ.Client` | In development |

Every transport, including third-party ones, must pass the transport conformance suite: send, receive, settle, retry, dead-letter, delay, ordering, trace propagation and reconnect.

## Why Kafka uses Dekaf

[Dekaf](https://github.com/thomhurst/Dekaf) has no native dependencies, supports Native AOT and is built for zero-allocation hot paths, which matches Relayer's own goals. Where Dekaf already ships a feature, such as retry topics or OpenTelemetry instrumentation, the Kafka transport reuses it instead of re-implementing it.

## Building a transport

The core is designed so a transport contains native code only. Start with:

- [Writing a transport](authoring.md): registration, runtime adapters and the send seam.
- [Receiving messages](receive.md): the message binding and settlement.
- [Consume pipeline](consume-pipeline.md): how a consumer is composed at startup.
- [Envelopes and headers](envelopes.md): mapping CloudEvents headers to native properties.
- [Capabilities](../concepts/capabilities.md): what each endpoint declares.
