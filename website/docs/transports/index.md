---
title: Transports
description: "The native clients Relayer wraps for each broker, their status, and how to write a transport of your own."
---

A transport is an adapter over one native client. Each transport package depends only on its native client and supports only that client's latest major version.

| Package | Broker | Native client | Status |
|---|---|---|---|
| `Relayer` | [In-memory](in-memory.md), for tests and samples | Built in | Implemented |
| `Relayer.Kafka` | [Apache Kafka](kafka.md) | [Dekaf](https://dekaf.tomlonghurst.dev/), a pure C# Kafka client | Producers and serial consumers implemented; parallel partition scheduling planned |
| `Relayer.AzureServiceBus` | Azure Service Bus | `Azure.Messaging.ServiceBus` | [Native sending, topology and receivers](service-bus.md), native settlement and optional health implemented |
| `Relayer.AmazonSqs` | Amazon SQS and SNS | `AWSSDK.SQS` and `AWSSDK.SimpleNotificationService` | [SQS receiving, native sending and topology](amazon-sqs.md), SNS publishing and optional SQS health implemented |
| `Relayer.RabbitMQ` | RabbitMQ | `RabbitMQ.Client` | [Native topology, confirmed sends, receivers and optional health](rabbitmq.md) implemented; optional native delays supported |
| `Relayer.RabbitMQ.Streams` | RabbitMQ Streams | `RabbitMQ.Stream.Client` | [Native connection and stream topology](rabbitmq-streams.md) implemented; message execution blocked on native failed-offset recovery |
| `Relayer.Nats` | NATS JetStream | `NATS.Client.JetStream` | [Native connection and topology](nats.md) implemented; message execution blocked on native byte-backed headers |
| `Relayer.Pulsar` (planned) | Apache Pulsar | Client assessment: DotPulsar 5.3.4 | [Implementation blocked on native UTF-8 properties and Native AOT compatibility](pulsar.md); no adapter implemented |
| `Relayer.GooglePubSub` | [Google Cloud Pub/Sub](google-pubsub.md) | `Google.Cloud.PubSub.V1` | Native publishing, streaming pull, durable failure copies and topology implemented; prepared-client AOT coverage |

Every transport, including third-party ones, must pass the transport conformance suite: send, receive, settle, retry, dead-letter, delay, ordering, trace propagation and reconnect.

## Why Kafka uses Dekaf

[Dekaf](https://github.com/thomhurst/Dekaf) has no native dependencies, supports Native AOT and is built for zero-allocation hot paths, which matches Relayer's own goals.

Relayer reuses Dekaf's native producer retries, receive batches, checkpoints and offset commits. Consumer handler retries and durable dead-letter copies use Relayer's core failure pipeline. Dekaf's hosted retry-topic and dead-letter helpers remain disabled because they own dispatch and offset settlement, which would bypass that pipeline. See the [Kafka guide](kafka.md) for failure handling and telemetry details.

## Building a transport

The core is designed so a transport contains native code only. Start with:

- [Writing a transport](authoring.md): registration, runtime adapters and the send seam.
- [Receiving messages](receive.md): the message binding and settlement.
- [Consume pipeline](consume-pipeline.md): how a consumer is composed at startup.
- [Envelopes and headers](envelopes.md): mapping CloudEvents headers to native properties.
- [Capabilities](../concepts/capabilities.md): what each endpoint declares.
