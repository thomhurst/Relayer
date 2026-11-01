---
title: Kafka
description: "Named Kafka producers and serial consumers backed by Dekaf, with durable failure handling."
---

`Relayer.Kafka` implements producers and serial consumer groups on [Dekaf](https://github.com/thomhurst/Dekaf). Consumers use the core failure pipeline and acknowledge offsets after durable handling, including dead-letter copies. Parallel partition scheduling, drain-on-revoke, typed tombstones, health adapters, topology management and transactions remain planned.

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

`ConfigureProducer` receives Dekaf's own builder unchanged. Its generic key is `WireString?`; its value is `(ReadOnlyMemory<byte> Body, Dekaf.Serialization.Headers Headers)?`. Relayer supplies serializers that write the existing serialized body and headers using Dekaf's record-header serializer hook. This avoids allocating immutable native message objects or copying the body into another array. The nullable tuple preserves null-valued failure copies as native tombstones. Existing inferred configuration callbacks remain unchanged; explicit native builder types must include the nullable value. Keep these serializers when changing native producer settings. Native callbacks passed through `SendOptions.ConfigureNative<Headers>` receive the native header collection before the send. Borrowed header storage must not be retained after the callback.

The application's `ILoggerFactory` is passed to Dekaf. Native security, compression, batching, partitioning, acknowledgment and retry settings remain native settings. A native configuration override changes native behavior, including its delivery guarantees.

## Keys, batches and failures

Route `OrderingKey` selectors accept strings or `IUtf8SpanFormattable` values. Relayer passes the resolved key to Dekaf's partitioner. No key remains a null Kafka key; an empty string remains a non-null empty key.

The serialized transport batch seam delegates same-topic batches to Dekaf's `ProduceAllAsync` in input order and awaits all admitted operations. Dekaf owns batching, per-key ordering and partial failures. Mixed-topic batches send in input order with individual acknowledgments. Native byte limits still apply; a batch is not atomic. Cancellation may stop waiting after Kafka has accepted a message, so an exception does not prove that nothing was delivered.

Kafka has no native scheduled send or dead-letter operation. Scheduled direct sends fail with an actionable error. Topic validation and provisioning also fail until implemented; use `TopologyMode.None` with externally managed topics.

## Register a consumer

Add a consumer to the same `Kafka` declaration:

```csharp
kafka.Consume("orders", consumer =>
{
    consumer.Options.Group = "billing";
    consumer.Options.Failure = new FailureOptions
    {
        RetryDelays = new[] { TimeSpan.FromMilliseconds(100), TimeSpan.FromSeconds(1) },
        DeadLetterEndpoint = "orders.dead-letter",
    };
    consumer.Handler<OrderPlacedHandler>();
});
```

Register the handler and its message with source generation. Create both topics externally. Without an explicit `DeadLetterEndpoint`, the destination is the source topic plus `.dead-letter`. Immediate retries run inside processing; exhausted retries and invalid envelopes send the original serialized body, headers, key and creation context to that topic. A successful broker acknowledgment of the copy permits committing the original offset. Copy errors or cancellation stop the receiver and leave the incomplete batch uncommitted. Recovery can therefore repeat earlier durable work; handlers must tolerate at-least-once delivery.

`KafkaTransportOptions.ConfigureConsumer` receives Dekaf's `ConsumerBuilder<byte[], byte[]>`. Native security, offset-reset, fetch and group settings remain available. Keep the raw byte deserializers. After that callback, Relayer sets the declared group and source subscription, disables automatic offset storage and commits, and adds its rebalance listener. These ownership settings ensure polling never acknowledges unfinished work. Dekaf's native offset-reset default still applies; set `WithAutoOffsetReset(AutoOffsetReset.Earliest)` explicitly when a new group must read existing records.

One declaration currently processes records serially, with `Concurrency(1)` and single-message handlers. Other values fail at startup with an actionable error. The adapter consumes native fetch batches and commits their native next-offset checkpoints only after every delivered record completes. It abandons an invalidated batch on rebalance, allowing native redelivery. Native batch checkpoints preserve offset gaps and leader epochs. Key-parallel processing, contiguous watermark tracking across parallel work, and bounded drain-on-revoke belong to the planned partition scheduler. Shutdown stops fetching and drains the current batch; its cancellation deadline aborts unfinished handling without committing it. Recreate the host after a failed or aborted receive so a fresh native consumer resumes at the broker's committed offsets.

Inside a handler, `context.Kafka().ConsumeResult` exposes Dekaf's original record. Durable dead-letter copies retain native null values and nullable application headers. The struct view wraps a pooled `NativeMessage<KafkaDelivery>` feature and is valid only during processing. It provides native headers, key, partition, offset and timestamp without allocating a feature for each record. Null-value records receive the tombstone telemetry attribute; typed `Tombstone<T>` dispatch remains planned, so an unsupported tombstone body follows the core deserialization failure policy.

Consumer failures reach the application's background-service supervision immediately, even when another consumer is still running. The host's default policy stops the application. Shutdown signals every consumer before waiting for their current batches, so a failed consumer cannot prevent its siblings from stopping. Override `HostOptions.BackgroundServiceExceptionBehavior` only when the application provides its own recovery policy.

Failure copies discard any incoming `relayer.internal.kafka.tombstone` header. This private send marker cannot turn a non-null empty Kafka value into a tombstone; copies derive null-value state from the native record.

## Tracing and native feature reuse

Relayer emits `send {topic}` spans with `messaging.system = kafka`. Enable `Relayer` as an activity source; enabling Dekaf's source additionally produces native child spans. The record-header serializer installs the original CloudEvents and creation-context headers after native tracing starts, preserving `traceparent` and `tracestate` rather than propagating the native send span. Broker tests cover both sources together.

Consumer process spans include the declared group, cached partition ID and offset. A non-null key is tagged only on recorded spans; the tombstone tag is present only for null values. One `commit {topic}` client span describes each native batch commit, outside process spans, with `messaging.batch.message_count`. Enabling Dekaf's source retains native child spans. Internal polling emits no Relayer receive span.

Producer delivery and protocol retries use Dekaf's native retry machinery. Consumer handler retries and dead-letter copies use Relayer's core failure pipeline. Dekaf's hosted retry-topic and dead-letter helpers own dispatch, retry headers and offset settlement themselves, which would bypass the portable classifier and durable-copy policy. Relayer therefore reuses Dekaf's receive batches, checkpoints, producer acknowledgments and native commits, while leaving those hosted helpers disabled. The cross-transport outbox remains separate; delayed redelivery requires a registered durable `IFailureScheduler` and otherwise fails validation before receives start.

Testcontainers tests run the reusable send/settle/restart, key ordering, trace propagation, stored creation-context and broker reconnect contracts. Separate broker tests cover core retries, durable dead-letter copies, native feature cleanup, and non-commit after failed copies. Native scheduled-delivery, native redelivery and native dead-letter conformance cases do not apply to Kafka.

## Performance validation

`KafkaSendBenchmarks` compares acknowledged single sends and 32-message batches with raw Dekaf, with tracing disabled and `MemoryDiagnoser`. Set `RELAYER_KAFKA_BOOTSTRAP_SERVERS` to a test broker, then run:

```shell
dotnet run -c Release --project benchmarks/Relayer.Benchmarks -- --filter "*KafkaSendBenchmarks*"
```

Each benchmark creates and deletes its own topic. The comparison measures the serialized adapter seam, including header conversion. It does not establish the full default-JSON send pipeline's 5% throughput budget. That acceptance gate remains part of the transport rollout.

`KafkaReceiveBenchmarks` replays a fixed 4096-record topic through raw Dekaf and the production serial receive adapter. Both paths deserialize the same source-generated JSON contract and commit native fetch-batch checkpoints. Tracing is disabled and `MemoryDiagnoser` reports allocations. This comparison includes the core receive pipeline and generated handler dispatch; it does not measure the planned parallel scheduler. With the same broker environment variable set, run `--filter "*KafkaReceiveBenchmarks*"`. Results are local diagnostics, not a claim that the 5% receive budget has passed.
