---
title: Kafka
description: "Named Kafka producers backed by Dekaf, with ordered sends and native configuration."
---

`Relayer.Kafka` implements the producer side of the Kafka adapter on [Dekaf](https://github.com/thomhurst/Dekaf). Consumer execution, dead-letter topics, health adapters, topology management, transactions and full transport conformance remain planned.

## Register a producer

```csharp
using Relayer;
using Relayer.Kafka;

services.AddRelayer(relayer =>
{
    relayer.AddMessageTypesFromThisAssembly();
    relayer.UseJsonSerializer(AppJsonContext.Default);
    relayer.Kafka("events", kafka =>
    {
        // Create and manage topics externally until topology management is implemented.
        kafka.TopologyMode = TopologyMode.None;
        kafka.Options.Configure(options =>
        {
            options.BootstrapServers.Add("localhost:9092");
            options.ConfigureProducer = producer => producer
                .WithClientId("billing")
                .WithLinger(TimeSpan.FromMilliseconds(5));
        });
        kafka.Send<OrderPlaced>("orders", route =>
            route.OrderingKey(order => order.CustomerId));
    });
});

await sender.SendAsync(new OrderPlaced(customerId));
```

The message must have a stable `[Message]` identity and be included in `AppJsonContext`. Start the application host before sending. Startup initializes Dekaf; shutdown flushes outstanding sends and disposing the provider disposes the producer. Named transport options also support [configuration binding](../guides/configuration.md).

`ConfigureProducer` receives Dekaf's own builder unchanged. Its generic key is `WireString?`; its value is `(ReadOnlyMemory<byte> Body, Dekaf.Serialization.Headers Headers)`. Relayer supplies serializers that write the existing serialized body and headers using Dekaf's record-header serializer hook. This avoids allocating immutable native message objects or copying the body into another array. Keep these serializers when changing native producer settings. Native callbacks passed through `SendOptions.ConfigureNative<Headers>` receive the native header collection before the send. Borrowed header storage must not be retained after the callback.

The application's `ILoggerFactory` is passed to Dekaf. Native security, compression, batching, partitioning, acknowledgment and retry settings remain native settings. A native configuration override changes native behavior, including its delivery guarantees.

## Keys, batches and failures

Route `OrderingKey` selectors accept strings or `IUtf8SpanFormattable` values. Relayer passes the resolved key to Dekaf's partitioner. No key remains a null Kafka key; an empty string remains a non-null empty key.

The serialized transport batch seam delegates same-topic batches to Dekaf's `ProduceAllAsync` in input order and awaits all admitted operations. Dekaf owns batching, per-key ordering and partial failures. Mixed-topic batches send in input order with individual acknowledgments. Native byte limits still apply; a batch is not atomic. Cancellation may stop waiting after Kafka has accepted a message, so an exception does not prove that nothing was delivered.

Kafka has no native scheduled send or dead-letter operation. Scheduled direct sends fail with an actionable error. Consumer declarations currently fail at startup instead of silently doing nothing. Topic validation and provisioning also fail until implemented; use `TopologyMode.None` with externally managed topics.

## Tracing and native feature reuse

Relayer emits `send {topic}` spans with `messaging.system = kafka`. Enable `Relayer` as an activity source; enabling Dekaf's source additionally produces native child spans. The record-header serializer installs the original CloudEvents and creation-context headers after native tracing starts, preserving `traceparent` and `tracestate` rather than propagating the native send span. Broker tests cover both sources together.

Producer delivery retries use Dekaf's native retry machinery; Relayer adds no retry loop. Dekaf's hosted consumer retry and dead-letter topic helpers are evaluated with consumer execution, where they must fit the core failure pipeline. Relayer's cross-transport outbox remains separate.

## Performance validation

`KafkaSendBenchmarks` compares acknowledged single sends and 32-message batches with raw Dekaf, with tracing disabled and `MemoryDiagnoser`. Set `RELAYER_KAFKA_BOOTSTRAP_SERVERS` to a test broker, then run:

```shell
dotnet run -c Release --project benchmarks/Relayer.Benchmarks -- --filter "*KafkaSendBenchmarks*"
```

Each benchmark creates and deletes its own topic. The comparison measures the serialized adapter seam, including header conversion. It does not establish the full default-JSON send pipeline's 5% throughput budget. That acceptance gate and receive benchmarks remain part of the transport rollout.
