---
title: Kafka
description: "Named Kafka producers and serial consumers backed by Dekaf, with durable failure handling."
---

`Relayer.Kafka` implements producers and serial consumer groups on [Dekaf](https://github.com/thomhurst/Dekaf). Consumers use the core failure pipeline and acknowledge offsets after durable handling, including dead-letter copies. Optional [consumer circuit breaking](../guides/failure-handling.md#consumer-circuit-breaking) pauses native consumption and retains failed records during dependency outages. Optional health adapters are available in `Relayer.Kafka.HealthChecks`. Typed tombstones use native null values and the original message's wire identity. Parallel partition scheduling, drain-on-revoke, topology management and transactions remain planned.

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

One declaration currently processes records serially, with `Concurrency(1)` and single-message handlers. Other values fail at startup with an actionable error. The adapter consumes native fetch batches and commits their native next-offset checkpoints only after every delivered record completes. It abandons an invalidated batch on rebalance. If the partition remains assigned, it seeks to the batch's first delivered offset and replays the uncommitted batch; a revoked partition resumes through native reassignment. Successful records in that batch may therefore run again. Native batch checkpoints preserve offset gaps and leader epochs. Shutdown stops fetching and drains the current batch; its cancellation deadline aborts unfinished handling without committing it. Each hosted service starts once; recreate the host to restart Kafka consumers, including after a failed or aborted receive. A fresh native consumer resumes at the broker's committed offsets. Partial startup failure disposes the transport and all consumers already created.

Parallel receive is planned in [#22](https://github.com/thomhurst/Relayer/issues/22). It will wrap Dekaf's `RunPartitionedAsync` for key ordering, contiguous completion tracking, backpressure and bounded drain; it will not use Relayer's [fallback partition scheduler](partition-scheduler.md). Relayer will retain its receive/failure pipeline, durable completion rule, commit spans and health reporting. Dekaf 1.26.0 now provides consumer-wide handler/buffer limits, oldest outstanding record health and safe offset/timestamp replay through `PartitionedProcessingOptions`, `PartitionedProcessingMonitor` and `PartitionedProcessingControl`. The Relayer integration remains planned; the dependency upgrade alone does not enable parallel receive.

Inside a handler, `context.Kafka().ConsumeResult` exposes Dekaf's original record. Durable dead-letter copies retain native null values and nullable application headers. The struct view wraps a pooled `NativeMessage<KafkaDelivery>` feature and is valid only during processing. It provides native headers, key, partition, offset and timestamp without allocating a feature for each record. Null-value records receive the tombstone telemetry attribute and dispatch to `IHandler<Tombstone<T>>`. Non-null records, including empty values, dispatch to `IHandler<T>`. Missing handlers or malformed non-null bodies follow the core failure policy.

Dekaf 1.26.0 handles transient empty broker metadata during broker restarts through its native coordinator recovery. It refreshes metadata from the bootstrap servers before coordinator discovery; if no brokers are available yet, the failure follows Dekaf's existing coordinator retry and deadline policy. Relayer keeps the same native consumer and receive stream during this recovery. Native configuration still governs retries and timeouts; this does not guarantee recovery from every outage.

Consumer failures that escape native recovery reach the application's background-service supervision immediately, even when another consumer is still running. The host's default policy stops the application. Shutdown signals every consumer before waiting for their current batches, so a failed consumer cannot prevent its siblings from stopping. Override `HostOptions.BackgroundServiceExceptionBehavior` only when the application provides its own recovery policy.

Relayer discards the legacy `relayer.internal.kafka.tombstone` header, including values added by native callbacks. Null-value state comes only from the native record or the explicit serialized send flag; a header cannot turn an empty value into a tombstone.

## Independent schema registries

Reference `Relayer.Kafka.SchemaRegistry` to use Confluent-compatible JSON Schema Registry framing. Each named transport explicitly selects its own `KafkaJsonSchemaSerializer` and native Dekaf registry client. Broker security remains in `ConfigureProducer` and `ConfigureConsumer`; registry URL, credentials, TLS, proxies and HTTP configuration stay in Dekaf's `SchemaRegistryConfig` and `SchemaRegistryClient`.

```csharp
using Dekaf.SchemaRegistry;
using Microsoft.Extensions.DependencyInjection;
using Relayer;
using Relayer.Kafka;

// DI owns these clients. Read URLs and credentials from application configuration.
services.AddKeyedSingleton<ISchemaRegistryClient>("west", (_, _) =>
    new SchemaRegistryClient(new SchemaRegistryConfig
    {
        Url = westRegistryUrl,
        BasicAuthUserInfo = westRegistryCredentials,
    }));
services.AddKeyedSingleton<ISchemaRegistryClient>("east", (_, _) =>
    new SchemaRegistryClient(new SchemaRegistryConfig
    {
        Url = eastRegistryUrl,
        BasicAuthUserInfo = eastRegistryCredentials,
    }));

services.AddRelayer(relayer =>
{
    relayer.AddMessageTypesFromThisAssembly();
    relayer.Kafka("west", kafka =>
    {
        kafka.TopologyMode = TopologyMode.None;
        kafka.Options.Configure(options => options.BootstrapServers.Add(westBootstrap));
        kafka.UseSerializer(provider => new KafkaJsonSchemaSerializer(
                provider.GetRequiredKeyedService<ISchemaRegistryClient>("west"))
            .Add("orders", orderJsonSchema, ContractsJson.Default.OrderPlaced));
        kafka.Send<OrderPlaced>("orders");
        kafka.Consume("orders", consumer =>
        {
            consumer.Options.Group = "billing";
            consumer.Handler<OrderPlacedHandler>();
        });
    });
    relayer.Kafka("east", kafka =>
    {
        kafka.TopologyMode = TopologyMode.None;
        kafka.Options.Configure(options => options.BootstrapServers.Add(eastBootstrap));
        kafka.UseSerializer(provider => new KafkaJsonSchemaSerializer(
                provider.GetRequiredKeyedService<ISchemaRegistryClient>("east"))
            .Add("invoices", invoiceJsonSchema, ContractsJson.Default.InvoiceCreated,
                new JsonSchemaSerializerConfig { AutoRegisterSchemas = false }));
        kafka.Send<InvoiceCreated>("invoices");
        kafka.Consume("invoices", consumer =>
        {
            consumer.Options.Group = "accounting";
            consumer.Handler<InvoiceCreatedHandler>();
        });
    });
});
```

`orderJsonSchema` and `invoiceJsonSchema` are explicit JSON Schema strings. `ContractsJson` is an application-owned source-generated `JsonSerializerContext` covering both contracts. Declare stable `[Message]` names as usual. A message has one send route; use different message types for different routes. The same CLR contract may be consumed on multiple clusters with independent schema registrations.

The host resolves each writer schema asynchronously before starting receivers. Dekaf handles native subject selection, registration or lookup and the five-byte prefix (magic byte zero plus big-endian schema ID). Pass `JsonSchemaSerializerConfig` unchanged to select native fixed-schema settings, including custom synchronous subject naming, normalization or a fixed schema ID. Match the `Add` topic to the route or consumer topic; one contract declaration per CLR type is supported within a serializer. Different types on a shared topic can use native record-name subjects. Registration and lookup failures stop host startup. The serializer owns its native writer serializers; it does not dispose the supplied registry client.

This package supports JSON payloads with prefix schema IDs and source-generated metadata. It writes `content-type: application/vnd.relayer.kafka.json-schema`; stable CloudEvents identity remains in `ce_type` and is independent of registry IDs. Sends, retries and durable failure copies retain the selected wire body. Default JSON and application-supplied serializers remain available per transport through `UseSerializer`.

Receive accepts only the writer schema ID prepared for that declared message type on that transport. Unknown IDs, another format, truncated prefixes, null JSON and trailing JSON values enter the permanent deserialization failure policy. Declare older wire versions with their own source-generated metadata and explicit schemas, then register [upcasters](../guides/messages.md#versioning-and-unknown-types). Schema discovery during processing, automatic schema evolution, registry validation/rule execution, latest-version selection and header/GUID identities remain planned. Unsupported dynamic settings fail during configuration. The caller supplies schemas matching the declared JSON metadata; registration does not validate each payload against JSON Schema. For Avro, use the optional [Avro/schema registry serializer](../guides/messages.md#avro). The optional [Protobuf serializer](../guides/messages.md#protobuf) and [MessagePack serializer](../guides/messages.md#messagepack) write native bodies without registry framing.

No registry HTTP call occurs during message processing. The reader checks the prepared ID and reads the `ReadOnlySequence<byte>` directly with generated metadata, including segmented bodies. This avoids Dekaf's synchronous schema-fetch fallback on a deserializer cache miss. Do not share one serializer instance across independent registry clients.

## Seek and replay

Resolve the control service with the exact transport name after starting the host. Select the declared consumer group, topic and partition explicitly:

```csharp
using Dekaf;
using Microsoft.Extensions.DependencyInjection;
using Relayer.Kafka;

var control = host.Services.GetRequiredKeyedService<IKafkaConsumerControl>("events");
await control.SeekAsync("billing", new TopicPartitionOffset("orders", 0, 42), cancellationToken);
await control.SeekAsync("billing",
    new TopicPartitionTimestamp(new TopicPartition("orders", 0), DateTimeOffset.UtcNow.AddHours(-1)),
    cancellationToken);
```

Call control from an operational endpoint or service outside the consumer's handlers. The receiver finishes its current batch and durable offset commit, releases the native poll stream, then applies the seek on that same receiver. Idle polling is interrupted immediately. No old batch can commit after the seek is applied. Seeking neither changes the group's committed offset immediately nor bypasses inbox deduplication. After replay, normal durable handling commits new checkpoints; restarting before then resumes from the last committed offset. Replayed work can run again, so handlers must tolerate at-least-once delivery.

Offset seeks accept nonnegative absolute offsets and retain Dekaf's leader epoch and native offset-reset behavior. Timestamp seeks use Dekaf's native lookup; when Kafka returns no matching offset, the call fails without seeking. `TopicPartitionTimestamp.Earliest` and `Latest` retain their native lookup semantics. There is no application-side timestamp filtering or broadcast to other group members.

The partition must be assigned to this local receiver when the command executes. An unknown group/topic, multiple matching local declarations, an unassigned partition, an assignment change during lookup, or a stopped receiver fails with an actionable error. Target the process that owns the partition; a consumer on another cluster is reached through its own keyed service. Pending commands cancel on shutdown. Caller cancellation before execution skips the command; cancellation racing an applied seek cannot undo the native position change.

## Tracing and native feature reuse

Relayer emits `send {topic}` spans with `messaging.system = kafka`. Enable `Relayer` as an activity source; enabling Dekaf's source additionally produces native child spans. The record-header serializer installs the original CloudEvents and creation-context headers after native tracing starts, preserving `traceparent` and `tracestate` rather than propagating the native send span. Broker tests cover both sources together.

Consumer process spans include the declared group, cached partition ID and offset. A non-null key is tagged only on recorded spans; the tombstone tag is present only for null values. One `commit {topic}` client span describes each native batch commit, outside process spans, with `messaging.batch.message_count`. Enabling Dekaf's source retains native child spans. Internal polling emits no Relayer receive span.

Producer delivery and protocol retries use Dekaf's native retry machinery. Consumer handler retries and dead-letter copies use Relayer's core failure pipeline. Dekaf's hosted retry-topic and dead-letter helpers own dispatch, retry headers and offset settlement themselves, which would bypass the portable classifier and durable-copy policy. Relayer therefore reuses Dekaf's receive batches, checkpoints, producer acknowledgments and native commits, while leaving those hosted helpers disabled. The cross-transport outbox remains separate. Kafka durable redelivery is not implemented: nonempty `RedeliveryDelays` fails at startup even with a registered `IFailureScheduler`, because the copy representation cannot yet persist native tombstones and null-valued headers. Use `RetryDelays` for in-process retries followed by durable dead-letter copies. Manual `Retry` also rejects redelivery before changing the settlement request.

Testcontainers tests run the reusable send/settle/restart, key ordering, trace propagation, stored creation-context and broker reconnect contracts. Separate broker tests cover core retries, durable dead-letter copies, native feature cleanup, and non-commit after failed copies. Native scheduled-delivery, native redelivery and native dead-letter conformance cases do not apply to Kafka.

## Health checks

Reference `Relayer.Kafka.HealthChecks` for the optional native adapters:

```csharp
using Relayer.Kafka;
using Relayer.HealthChecks;
using Microsoft.Extensions.Diagnostics.HealthChecks;

builder.Services.AddRelayerKafkaHealthProviders("events", admin =>
{
    admin.WithClientId("orders-health");
    // Configure native TLS/SASL here when required by the broker.
});
builder.Services.AddRelayerHealthChecks(new RelayerHealthCheckOptions
{
    ConsumerStallTimeout = TimeSpan.FromMinutes(2),
});
builder.Services.Configure<HealthCheckServiceOptions>(options =>
    options.Registrations.Single(check => check.Name == "relayer.transport").Timeout = TimeSpan.FromSeconds(5));
```

Use the exact case-sensitive name from `relayer.Kafka("events", ...)`. Registration can precede or follow
`AddRelayer`. Each name gets one reusable, DI-owned Dekaf admin client with the transport's bootstrap servers
and application's `ILoggerFactory`. The callback receives Dekaf's native `AdminClientBuilder`; configure its
security settings explicitly because producer and consumer callbacks do not configure the admin client.
Repeated registration preserves existing adapters, including application-supplied adapters.

Connectivity uses Dekaf's live `DescribeClusterAsync(new DescribeClusterOptions(), token)` operation on every
poll. The no-options overload uses cached metadata and is unsuitable for connectivity. The probe neither
sends nor consumes messages, creates topics nor commits offsets. Broker errors retain their exception and
failure text; caller cancellation propagates. Apply the native admin client's connection settings and the
application's health-check timeout policy to bound probe latency, as in the example above. A connection
timeout bounds one native connection attempt; it does not bound all retries of the admin operation.
The health service reports a timed-out probe as unhealthy and passes cancellation to the native client.

Liveness comes from each exact consumer declaration's receiver state. Workers waiting for native batches
are idle and healthy regardless of elapsed time. Processing remains active through retries, durable failure
copies and the batch commit; completed records refresh progress. The oldest active worker determines
liveness, so idle workers and healthy declarations on the same source cannot mask a stall. Before receiver
startup and after receiver shutdown or failure, state is unavailable and health is unhealthy. Connectivity
and liveness are separate checks: an idle receiver does not prove broker connectivity. Parallel workers
remain planned, but the health snapshot already aggregates every registered worker.

Without this optional registration, Kafka consumers do not read the health clock or record health state.
The Kafka transport itself has no dependency on the health-check packages. See [health integration](../guides/health-checks.md)
for endpoint registration, thresholds and the adapter contracts.

## Performance validation

`KafkaSendBenchmarks` compares acknowledged single sends and 32-message batches with raw Dekaf, with tracing disabled and `MemoryDiagnoser`. The `Tombstone` parameter covers native null values alongside ordinary records. `SendPipelineBenchmarks.TombstoneSend` and `ReceivePipelineBenchmarks.NativeTombstone` isolate typed pipeline cost. Set `RELAYER_KAFKA_BOOTSTRAP_SERVERS` to a test broker, then run:

```shell
dotnet run -c Release --project benchmarks/Relayer.Benchmarks -- --filter "*KafkaSendBenchmarks*"
```

Each benchmark creates and deletes its own topic. The comparison measures the serialized adapter seam, including header conversion. It does not establish the full default-JSON send pipeline's 5% throughput budget. That acceptance gate remains part of the transport rollout.

`KafkaReceiveBenchmarks` replays a fixed 4096-record topic through raw Dekaf and the production serial receive adapter. Both paths deserialize the same source-generated JSON contract and commit native fetch-batch checkpoints. `HealthEnabled` compares receiver health tracking enabled and disabled against raw Dekaf. Tracing is disabled and `MemoryDiagnoser` reports allocations. This comparison includes the core receive pipeline and generated handler dispatch; it does not measure the planned parallel scheduler. With the same broker environment variable set, run `--filter "*KafkaReceiveBenchmarks*"`. Results are local diagnostics, not a claim that the 5% receive budget has passed.

## Typed tombstones

Declare the send route for the original message type. `Tombstone.For<T>(key)` uses that route and T's stable `[Message]` name and version, bypasses body serialization, and sends a native null Kafka value. Text and borrowed UTF-8 keys are supported; empty keys remain distinct from null keys.

```csharp
kafka.Send<Customer>("customers");
kafka.Consume("customers", consumer =>
{
    consumer.Options.Group = "customer-cache";
    consumer.Handler<CustomerDeletedHandler>();
});

await sender.SendAsync(Tombstone.For<Customer>("customer-42"), cancellationToken);

public sealed class CustomerDeletedHandler(ICustomerCache cache) : IHandler<Tombstone<Customer>>
{
    public ValueTask HandleAsync(Tombstone<Customer> message, MessageContext context,
        CancellationToken cancellationToken)
    {
        // Delete the keyed state durably before returning. The key is borrowed.
        return cache.DeleteAsync(message.Key, cancellationToken);
    }
}
```

Register `Customer` with `AddMessageTypesFromThisAssembly()` or `MessageTypes.Add<Customer>(...)`; do not register a separate identity or route for `Tombstone<Customer>`. A consumer can register both its regular handler and its tombstone handler. Tombstones never enter the JSON deserializer or upcaster chain. Native null records require a non-null key for typed dispatch; unkeyed null records follow the failure policy. The Kafka feature remains available and offsets commit only after durable handling.

This API requires a transport whose `TransportCapabilities.Tombstones` is true (currently Kafka). It does not enable broker log compaction; configure the topic's native cleanup policy separately. Tombstones can be buffered by a handler's send pipeline and retained in dead-letter copies. Database outbox persistence and durable redelivery of native null values remain unsupported: sends through an active outbox fail before enlistment. Use `SendNowAsync` for a direct native send when an outbox transaction is active.


## Inspect and redrive dead letters

Resolve the management service for a named Kafka transport. It lists only configured consumer dead-letter topics (including the default `<source>.dead-letter`), rather than discovering broker-wide topics:

```csharp
var manager = services.GetRequiredKeyedService<IDeadLetterManager>("events");
var sources = manager.List();
var page = await manager.PeekAsync("orders.dead-letter", maxMessages: 20,
    cancellationToken: cancellationToken);
if (page.NextCursor is { } cursor)
{
    var next = await manager.PeekAsync("orders.dead-letter", maxMessages: 20,
        cursor: cursor, cancellationToken: cancellationToken);
}
var redriven = await manager.RedriveAsync("orders.dead-letter",
    filter: message => message.Features.GetRequired<KafkaDeadLetterFeature>()
        .Record.Headers.Any(header => header.Key == "ce_id"),
    maxMessages: 20, cancellationToken: cancellationToken);
```

All operations accept cancellation and a limit from 1 through 100. Peek and purge wait for native fetches until their record limit or partition EOF; a slow fetch is not treated as an empty partition. Pass a cancellation token to bound these waits. Peek does not commit or delete records. Each page reads one partition; a continuation can lead to an empty page before advancing to the next partition. Continue while `NextCursor` is non-null, and keep cursors scoped to their source. Broker retention, compaction and concurrent purge can change what a continuation returns. Snapshots own their storage: `KafkaDeadLetterFeature.Record` exposes the native key, offset, timestamp and ordered headers after the call returns. Its null `Value` denotes a tombstone, distinct from empty bytes; null keys and null header values are retained too.

Redrive sends to the configured original source topic using Dekaf and waits for a broker acknowledgment before committing the next offset in a dedicated native consumer group. The group is `relayer.deadletters.<name length>.<transport name>.<dead-letter topic>`; keep transport names stable across restarts to resume progress. Concurrent redrive calls use Kafka's native group partition ownership. Do not run unrelated consumers in that group. Resetting or deleting the group replays retained dead letters. Native group-offset expiration can also cause replay. Redrive preserves serialized bodies, binary keys, tombstones, duplicate/null headers and the original message identity; Kafka chooses the destination partition using the native producer configuration. Both non-null values (including empty bodies) and tombstones retain these headers. It does not preserve the original offset or append timestamp.

Redrive waits for native group assignment and reads until the scan limit or until every assigned partition reaches EOF or is paused by the filter. A slow group join or fetch is not treated as an empty topic. Pass a cancellation token to bound the operation, including when another group member owns all partitions.

Redrive is at-least-once: a crash, rebalance, cancellation or commit failure after an acknowledged send can produce another copy. It does **not** delete originals from the topic. Other groups can still read them until native retention, compaction or purge removes them. A filter scans at most the supplied limit. An unmatched record pauses its partition for that call, because Kafka cannot commit past it without losing it from future redrive. Later records in that partition remain behind the unmatched record; change the filter or redrive that prefix to reach them. Filter exceptions and failed sends do not advance progress for the failing record. Redriving to an actively consumed source can send a persistently failing record back to the dead-letter topic; fix the failure first.

`PurgeAsync(topic, maxMessages, cancellationToken)` scans bounded earliest partition prefixes and uses Dekaf's native `DeleteRecordsAsync` to advance their log-start offsets. This is permanent for **all** consumer groups, including ordinary inspectors and redrive. Kafka deletes every offset below each requested boundary, not selected records; the limit counts delivered records rather than offsets. Offset gaps, compacted records and records hidden by the configured isolation level do not count as scanned messages, but deletion still affects the entire prefix. Concurrent purge/retention may remove data while redrive runs. Purge can partially succeed across partitions before an error. No per-record deletion is emulated, and redrive never uses purge for settlement.

Management reuses `ConfigureConsumer` and `ConfigureProducer` for native authentication and connection configuration, while enforcing raw byte reads, manual commits, earliest reset and acknowledged redrive sends. Configure the admin client separately for metadata and purge:

```csharp
kafka.Options.Configure(options =>
{
    options.ConfigureAdmin = native => native.WithClientId("dead-letter-admin");
});
```

Use the same native TLS/SASL settings on all three builders when the cluster requires them. Provision topics externally and grant the native read, group-offset, produce, metadata and DeleteRecords permissions needed by the operations you expose. A dead-letter topic shared by different source topics has an ambiguous redrive destination, so resolving its manager fails with an actionable error; configure separate dead-letter topics for those consumers.
