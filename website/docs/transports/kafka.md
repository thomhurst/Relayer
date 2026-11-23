---
title: Kafka
description: "Named Kafka producers and key-ordered parallel consumers backed by Dekaf, with durable failure handling."
---

`Relayer.Kafka` implements producers and consumer groups on [Dekaf](https://github.com/thomhurst/Dekaf). Consumers run on Dekaf's partitioned runtime with key-ordered parallelism, contiguous offset commits and bounded drain on revoke and shutdown. They use the core failure pipeline and mark records processed only after durable handling, including dead-letter copies. Optional [consumer circuit breaking](../guides/failure-handling.md#consumer-circuit-breaking) holds failed records during dependency outages. Optional health adapters are available in `Relayer.Kafka.HealthChecks`. Typed tombstones use native null values and the original message's wire identity. [Kafka-to-Kafka exactly-once](#kafka-to-kafka-exactly-once) uses native producer transactions. [Topic topology](#topic-topology) supports read-only validation and create-only provisioning. Batch handlers remain planned.

## Register a producer

```csharp
using Relayer;
using Relayer.Kafka;

services.AddRelayer(relayer =>
{
    relayer.AddMessageTypesFromThisAssembly();
    relayer.Kafka("events", kafka =>
    {
        // Create topics externally; the default Validate mode checks their existence.
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

Give the message a stable `[Message]` identity. `AddMessageTypesFromThisAssembly()` registers its generated JSON metadata. Use [`UseJsonSerializer`](../guides/messages.md#serialization) to supply an optional application-owned context. Start the application host before sending. Startup initializes Dekaf; shutdown flushes outstanding sends and disposing the provider disposes the producer. Named transport options also support [configuration binding](../guides/configuration.md).

If Dekaf rejects the producer settings while building the client at startup, correct and reload the configuration and call `StartAsync` again on the same host. Relayer uses the updated named options to build the producer on that attempt.

`ConfigureProducer` receives Dekaf's own builder unchanged. Its generic key is `WireString?`; its value is `(ReadOnlyMemory<byte> Body, Dekaf.Serialization.Headers Headers)?`. Relayer supplies serializers that write the existing serialized body and headers using Dekaf's record-header serializer hook. This avoids allocating immutable native message objects or copying the body into another array. The nullable tuple preserves null-valued failure copies as native tombstones. Existing inferred configuration callbacks remain unchanged; explicit native builder types must include the nullable value. Keep these serializers when changing native producer settings. Native callbacks passed through `SendOptions.ConfigureNative<Headers>` receive the native header collection before the send. Borrowed header storage must not be retained after the callback.

The application's `ILoggerFactory` is passed to Dekaf. Native security, compression, batching, partitioning, acknowledgment and retry settings remain native settings. A native configuration override changes native behavior, including its delivery guarantees.

## Keys, batches and failures

Route `OrderingKey` selectors accept strings or `IUtf8SpanFormattable` values. Relayer passes the resolved key to Dekaf's partitioner. No key remains a null Kafka key; an empty string remains a non-null empty key.

The serialized transport batch seam delegates same-topic batches to Dekaf's `ProduceAllAsync` in input order and awaits all admitted operations. Dekaf owns batching, per-key ordering and partial failures. Mixed-topic batches send in input order with individual acknowledgments. Native byte limits still apply; a batch is not atomic. Cancellation may stop waiting after Kafka has accepted a message, so an exception does not prove that nothing was delivered.

Kafka has no native scheduled send or dead-letter operation. Scheduled direct sends fail with an actionable error.

## Topic topology

The default `TopologyMode.Validate` checks send, receive and dead-letter topics through Dekaf's native admin metadata without creating missing topics. Correct invalid topic names in your routes or declarations. Kafka names use 1 to 249 ASCII letters, digits, periods, underscores or hyphens, and exclude `.` and `..`. For valid topics that are missing or inaccessible, check topic existence and the admin client's metadata access. `TopologyMode.None` skips topology checks and provisioning. Configure admin authentication with `options.ConfigureAdmin`; producer and consumer authentication settings do not configure the admin client.

Use `TopologyMode.Provision` explicitly for development or bootstrap. Relayer creates missing topics through Dekaf's `CreateTopicsAsync`, including consumer dead-letter topics. Topics named only by routes or consumers use Dekaf's `NewTopic` defaults: one partition and replication factor one. Existing topics are never updated or deleted.

Add native `NewTopic` declarations to override creation settings or declare additional topics:

```csharp
using Dekaf.Admin;

relayer.Kafka("events", kafka =>
{
    kafka.TopologyMode = TopologyMode.Provision;
    kafka.Options.Configure(options =>
    {
        options.BootstrapServers.Add("localhost:9092");
        options.Topics.Add(new NewTopic
        {
            Name = "orders",
            NumPartitions = 3,
            ReplicationFactor = 1,
            Configs = new Dictionary<string, string> { ["retention.ms"] = "86400000" },
        });
    });
    kafka.Send<OrderPlaced>("orders");
});
```

Explicit declarations are passed to Dekaf unchanged. Both Validate and Provision compare their partition counts, replication factors, manual replica assignments and declared configuration values. Native `-1` counts request broker defaults and are not compared; non-empty `ReplicaAssignments` take precedence over counts. Configuration values are compared as strings. Topics without explicit declarations get existence checks only, including topics that already exist when Provision runs. Relayer reports unresolved differences with expected and actual values and fails host startup before consumers start.

`options.TopicCreationOptions` passes native `CreateTopicsOptions` through to Dekaf. Its `TimeoutMs` also bounds the verification phase. A value of `0` raises `TimeoutException` as soon as Provision reaches verification, even if all topics already exist. Null options use Dekaf's defaults. Provision requires `ValidateOnly = false`. A concurrent creator is accepted only after reading and checking the resulting topic. Broker errors and cancellation propagate; provisioning across topics is not transactional.

## Register a consumer

Add a consumer to the same `Kafka` declaration:

```csharp
kafka.Consume("orders", consumer =>
{
    consumer.Options.Group = "billing";
    consumer.Concurrency(8); // Up to 8 handlers; records with the same key stay in order.
    consumer.Failures(new FailureOptions
    {
        RetryDelays = new[] { TimeSpan.FromMilliseconds(100), TimeSpan.FromSeconds(1) },
        DeadLetterEndpoint = "orders.dead-letter",
    });
    consumer.Handler<OrderPlacedHandler>();
});
```

Register the handler and its message with source generation. Create both topics externally. Without an explicit `DeadLetterEndpoint`, the destination is the source topic plus `.dead-letter`. An explicit `DeadLetterEndpoint` must differ from the consumer's source topic. Relayer rejects a match at startup and when resolving the dead-letter manager directly.

Immediate retries run inside processing; exhausted retries and invalid envelopes send the original serialized body, headers, key and creation context to the dead-letter topic. A successful broker acknowledgment of the copy marks the original record processed. When `ConfigureProducer` is set, non-transactional consumers use a dedicated producer per transport for failure copies. It uses the same native configuration with `Acks.All`, so ordinary sends can still use `Acks.None`. [Exactly-once consumers](#kafka-to-kafka-exactly-once) send failure copies in their transaction. Copy errors stop the receiver without marking that record, so its offset and every later offset in the partition stay uncommitted. Recovery can therefore repeat earlier durable work; handlers must tolerate at-least-once delivery.

`KafkaTransportOptions.ConfigureConsumer` receives Dekaf's `ConsumerBuilder<byte[], byte[]>`. Native security, offset-reset, fetch and group settings remain available. Keep the raw byte deserializers. Relayer sets the declared group and replaces any subscription set by the callback with the declared source topic. Relayer also disables the consumer's own automatic offset storage and commits, so polling never acknowledges unfinished work. Rebalance listeners added in the callback are preserved. Dekaf's native offset-reset default still applies; set `WithAutoOffsetReset(AutoOffsetReset.Earliest)` explicitly when a new group must read existing records.

### Parallelism, commits and rebalances

Each declaration wraps Dekaf's `RunPartitionedAsync` record handler. By default it uses `PartitionedProcessingOrder.Key`: records with equal keys (compared by content, with null keys sharing one lane) run in offset order, while different keys in the same partition run concurrently. `Concurrency(n)` sets Dekaf's consumer-wide `MaxConcurrentHandlers`, and by default also `MaxConcurrentHandlersPerPartition`, so `Concurrency(1)` processes one record at a time across the consumer. Batch handlers fail at startup with an actionable error.

A record is marked processed only when its receive returns `Complete` or `SettledByHandler`, which means retries, dead-letter copies and handler settlement are durable. Dekaf tracks the highest contiguous completed offset per partition, so a slow or failed record holds back its partition's commit even when later keys finish first. With the default `PartitionCommitPolicy.CommitCompletedPeriodically`, Dekaf commits those offsets every `CommitInterval` (5 seconds by default) and again when a partition is revoked or stopped. Commits preserve offset gaps from compaction and transaction markers, and leader epochs. Commits are amortized, so a crash can replay records completed since the last commit.

Dekaf owns assignment, pause and resume, backpressure (`PartitionBackpressureMode.PauseResume`) and drain. When a partition is revoked or the host stops, its lane stops dispatching, in-flight handlers finish (`PartitionStopPolicy.Drain`), and completed offsets are committed. After `StopTimeout` (30 seconds by default) Dekaf cancels the handler's token; the cancelled record is not marked processed and is replayed by the next owner. Retained partitions keep processing during a cooperative rebalance. A handler failure that escapes the failure pipeline stops the consumer (`PartitionWorkerErrorPolicy.StopConsumer`) without committing the failed record.

Pass Dekaf's own options through `consumer.Options.Processing` to change ordering, buffers, commit interval or stop behavior:

```csharp
using Dekaf.Consumer;

kafka.Consume("orders", consumer =>
{
    consumer.Options.Group = "billing";
    consumer.Concurrency(16);
    consumer.Options.Processing = new PartitionedProcessingOptions
    {
        Ordering = PartitionedProcessingOrder.Key,
        MaxConcurrentHandlersPerPartition = 4,
        MaxBufferedRecords = 2048,
        CommitPolicy = PartitionCommitPolicy.CommitCompletedPeriodically,
        CommitInterval = TimeSpan.FromSeconds(1),
        StopTimeout = TimeSpan.FromSeconds(10),
    };
    consumer.Handler<OrderPlacedHandler>();
});
```

Relayer passes these settings to Dekaf unchanged and supplies `Monitor`, `Control` and `CommitObserver` itself. Startup rejects options that would break durable completion: `MaxConcurrentHandlers` other than `Concurrency`, `PartitionCommitPolicy.UserManaged`, an `ErrorPolicy` other than `StopConsumer`, a `MaxHandlerBatchSize` other than 1, or a supplied monitor, control or commit observer.

Every start of a consumer, including a restart through [`IConsumerControl`](#start-consumers-later), runs a new native Dekaf consumer that joins the group and resumes at the broker's committed offsets; a stopped consumer leaves the group after its drain. Recreate the host to restart consumers after a failed receive. Cancelling the host's stop token stops waiting for the drain but does not cancel handlers early; Dekaf still bounds the drain by `StopTimeout`. When one consumer fails to start, the consumers already started are stopped again.

Inside a handler, `context.Kafka().ConsumeResult` exposes Dekaf's original record. Durable dead-letter copies retain native null values. When application headers contain native nulls, the copy preserves their full sequence, including interleaved names and duplicates. The struct view wraps a pooled `NativeMessage<KafkaDelivery>` feature and is valid only during processing. It provides native headers, key, partition, offset and timestamp without allocating a feature for each record. Null-value records receive the tombstone telemetry attribute and dispatch to `IHandler<Tombstone<T>>`. Non-null records, including empty values, dispatch to `IHandler<T>`. Missing handlers or malformed non-null bodies follow the core failure policy.

Dekaf 1.26.0 handles transient empty broker metadata during broker restarts through its native coordinator recovery. It refreshes metadata from the bootstrap servers before coordinator discovery; if no brokers are available yet, the failure follows Dekaf's existing coordinator retry and deadline policy. Relayer keeps the same native consumer and receive stream during this recovery. Native configuration still governs retries and timeouts; this does not guarantee recovery from every outage.

Consumer failures that escape native recovery reach the application's background-service supervision immediately, even when another consumer is still running. The host's default policy stops the application. Shutdown signals every consumer before waiting for their drains, so a failed consumer cannot prevent its siblings from stopping. Override `HostOptions.BackgroundServiceExceptionBehavior` only when the application provides its own recovery policy.

Relayer discards the legacy `relayer.internal.kafka.tombstone` header, including values added by native callbacks. Null-value state comes only from the native record or the explicit serialized send flag; a header cannot turn an empty value into a tombstone.

### Start consumers later

Consumers start with the host by default. Set `AutoStartConsumers = false` to bring the host up first and start the consumers yourself through the transport's keyed `IConsumerControl`:

```csharp
using Microsoft.Extensions.DependencyInjection;
using Relayer.Transports;

kafka.Options.Configure(options => options.AutoStartConsumers = false);

// Later, when the application is ready to receive:
var consumers = app.Services.GetRequiredKeyedService<IConsumerControl>("events");
await consumers.StartAsync(cancellationToken);
```

The host still composes and validates every consumer and starts the producer, so sending works while the consumers are stopped. `StartAsync` fails before the host has started or after it stops. `StopAsync` drains in-flight records and commits completed offsets like host shutdown, then leaves the consumer group so other members take over its partitions; a later `StartAsync` rejoins the group at the committed offsets. Both calls do nothing when the consumers are already in that state. A start while an abandoned stop is still draining fails until the drain finishes. Host shutdown stops consumers that the control started. While stopped, consumer [health](#health-checks) reports them as unavailable.

### Kafka to a database

To write database changes and outgoing messages for each record, attach the [EF Core consumer inbox](../reference/ef-core-inbox.md) with `consumer.EntityFrameworkCore<TContext>(...)`. The inbox row, business changes and outbox rows commit in one PostgreSQL transaction, and the receive returns `Complete` only after that commit, so the record's offset is never committed ahead of its database work. A crash before the commit rolls everything back and the record is replayed. A crash after the commit but before Dekaf's next offset commit also replays the record, and the inbox row makes the replay skip the handler, so business rows and outbox rows are written once. A failed attempt rolls back before its retry, and a handler cancelled at `StopTimeout` rolls back and leaves its record uncommitted.

Delivery is still at least once: only the database work is deduplicated. Side effects outside the transaction, such as HTTP calls or direct `SendWithoutOutboxAsync` sends, can repeat on replay. For Kafka output, use [Kafka-to-Kafka exactly-once](#kafka-to-kafka-exactly-once) instead.

### Kafka to Kafka exactly-once

Set Dekaf's `PartitionCommitPolicy.CommitCompletedInTransaction` to commit the consumed offsets and the handler's Kafka output in one native transaction:

```csharp
using Dekaf.Consumer;

kafka.Send<OrderShipped>("shipments");
kafka.Consume("orders", consumer =>
{
    consumer.Options.Group = "shipping";
    consumer.Options.Processing = new()
    {
        Ordering = PartitionedProcessingOrder.Key,
        CommitPolicy = PartitionCommitPolicy.CommitCompletedInTransaction,
    };
    consumer.Options.TransactionalId = "shipping-" + Environment.MachineName; // optional
    consumer.Handler<ShipOrder>();
});

public sealed class ShipOrder(ISender sender) : IHandler<Order>
{
    public ValueTask HandleAsync(Order order, MessageContext context, CancellationToken cancellationToken) =>
        sender.SendAsync(new OrderShipped(order.Id), cancellationToken);
}
```

Each run of the consumer creates its own transactional producer from the transport's `ConfigureProducer`, and Relayer passes it to Dekaf as `TransactionalProducer`. Dekaf keeps one transaction open across the consumer's partitions. At each commit (every `CommitInterval`, on revoke, before a seek and at shutdown) it waits for the records its handlers already took, then sends the completed offsets with the group metadata and commits. Readers of the output topics must use `IsolationLevel.ReadCommitted`; the consumer itself reads with `ReadCommitted` unless `ConfigureConsumer` sets another level.

What joins the transaction:

- Sends a handler makes through `ISender` to a route on the same Kafka transport, including `SendWithoutOutboxAsync`. They are buffered until the attempt succeeds, as always, and then produced through the transactional producer.
- [Dead-letter copies](../guides/failure-handling.md) of the consumer's failed records, so a poison record's copy and its offset commit together.

The [test harness](../guides/testing.md) preserves this transaction boundary for both handler sends and dead-letter copies. Its `Dispatched` observations record successful producer calls, which can precede a commit. Use a `ReadCommitted` reader to verify which records committed.

What does not: sends to other transports or Kafka transports, database outbox rows, and any other side effect. Those stay at least once and can repeat when a transaction aborts and its records are processed again.

When a commit fails, a partition is lost, or a failure escapes the failure pipeline (for example a dead-letter copy that cannot be written), Dekaf aborts the transaction, so none of its output becomes visible, and the records are processed again from the committed offsets in a new transaction or by the partition's next owner. `CommitInterval` sets how long each transaction lasts, and so how late downstream readers see the output.

`TransactionalId` defaults to the consumer group followed by a random suffix for each run. After a crash, the broker aborts the crashed run's open transaction only when the broker's transaction timeout expires, and read-committed readers of the affected partitions wait until then. Set an ID that is stable for each application instance, such as its pod name, to have a restarted instance abort it at once. Two running instances must never share an ID. A [consumer circuit breaker](../guides/failure-handling.md#consumer-circuit-breaking) cannot be combined with this policy: a held record would stall every commit, so startup rejects it. Startup also rejects a database consumer boundary with an inbox, such as the [EF Core consumer inbox](../reference/ef-core-inbox.md). Its row commits before the Kafka transaction, so after an abort the replay would skip the handler and the aborted output would never be produced. With `AddRelayerTenancy`, the EF Core boundary counts as having an inbox, because the context selected for each delivery turns one on when it has `AddRelayerOutbox`. A boundary without an inbox is allowed; its database writes stay at least once.

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
        // Provision topics externally; default Validate checks that they exist.
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
        // Provision topics externally; default Validate checks that they exist.
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

`orderJsonSchema` and `invoiceJsonSchema` are explicit JSON Schema strings. `ContractsJson` is an application-owned source-generated `JsonSerializerContext` covering both contracts. Declare stable `[Message]` names as usual. Each `KafkaJsonSchemaSerializer` accepts one schema declaration per CLR type. You can declare the same CLR type on multiple named Kafka transports, with a separate serializer and schema registration for each. To send that type through more than one route, use [atomic outbox fan-out](../guides/sending.md#sending-to-several-routes) and send within an active outbox transaction.

The host resolves each writer schema asynchronously before starting receivers. Dekaf handles native subject selection, registration or lookup and the five-byte prefix (magic byte zero plus big-endian schema ID). Pass `JsonSchemaSerializerConfig` unchanged to select native fixed-schema settings, including custom synchronous subject naming, normalization or a fixed schema ID. Match the `Add` topic to the route or consumer topic. Different types on a shared topic can use native record-name subjects. Registration and lookup failures stop host startup. The serializer owns its native writer serializers; it does not dispose the supplied registry client.

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

Call control from an operational endpoint or service outside the consumer's handlers. The seek uses Dekaf's `PartitionedProcessingControl`, which applies it on the consumer's runtime loop shortly after the call, so records dispatched before then still run. When it applies, the partition's lane stops dispatching and discards records buffered behind in-flight handlers, waits for those handlers under `StopPolicy` and `StopTimeout`, commits the lower of the partition's completed offset and the seek offset, and restarts the lane at the seek offset. Other partitions keep processing. Seeking does not bypass inbox deduplication. Replayed work can run again, so handlers must tolerate at-least-once delivery.

Offset seeks accept nonnegative absolute offsets and retain Dekaf's leader epoch and native offset-reset behavior. Timestamp seeks use Dekaf's native lookup; when Kafka returns no matching offset, the call fails without seeking rather than moving to the end of the partition. `TopicPartitionTimestamp.Earliest` and `Latest` retain their native lookup semantics. There is no application-side timestamp filtering or broadcast to other group members.

The partition must be assigned to this local receiver when the command executes. An unknown group/topic, multiple matching local declarations, an unassigned partition or a stopped receiver fails with an actionable error. Target the process that owns the partition; a consumer on another cluster is reached through its own keyed service. A seek still pending at shutdown fails. Caller cancellation before the runtime starts the seek skips it; later cancellation only stops the caller's wait.

## Tracing and native feature reuse

Relayer emits `send {topic}` spans with `messaging.system = kafka`. Enable `Relayer` as an activity source; enabling Dekaf's source additionally produces native child spans. The record-header serializer installs the original CloudEvents and creation-context headers after native tracing starts, preserving `traceparent` and `tracestate` rather than propagating the native send span. Broker tests cover both sources together.

Consumer process spans include the declared group, cached partition ID and offset. A non-null key is tagged only on recorded spans; the tombstone tag is present only for null values. One `commit {topic}` client span describes each runtime-managed commit (periodic, revoke, stop and seek), outside process spans, with `messaging.batch.message_count` set to the completed records it newly covers. Dekaf's `IOffsetCommitObserver` supplies the start, outcome and count, so failed commits are marked with `error.type`. Enabling Dekaf's source retains native child spans. Internal polling emits no Relayer receive span.

Producer delivery and protocol retries use Dekaf's native retry machinery. Consumer handler retries and dead-letter copies use Relayer's core failure pipeline. Dekaf's hosted retry-topic and dead-letter helpers own dispatch, retry headers and offset settlement themselves, which would bypass the portable classifier and durable-copy policy. Relayer therefore reuses Dekaf's partitioned runtime, completion tracking, producer acknowledgments and native commits, while leaving those hosted helpers disabled. The cross-transport outbox remains separate. Kafka durable redelivery is not implemented: nonempty `RedeliveryDelays` fails at startup even with a registered `IFailureScheduler`, because the copy representation cannot yet persist native tombstones and null-valued headers. Use `RetryDelays` for in-process retries followed by durable dead-letter copies. Manual `Retry` rejects redelivery before changing the settlement request and directs you to `RetryDelays` and the dead-letter policy.

Testcontainers tests run the reusable send/settle/restart, key ordering, trace propagation, stored creation-context and broker reconnect contracts. Separate broker tests cover core retries, durable dead-letter copies, native feature cleanup, and non-commit after failed copies. Native scheduled-delivery, native redelivery and native dead-letter conformance cases do not apply to Kafka.

## Health checks

Reference `Relayer.Kafka.HealthChecks` for the optional native adapters:

```csharp
using Relayer.Kafka;
using Relayer.HealthChecks;
using Microsoft.Extensions.DependencyInjection;
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

Liveness comes from each exact consumer declaration's Dekaf `PartitionedProcessingMonitor`. A receiver
with no buffered or in-flight record is idle and healthy regardless of elapsed time. Otherwise its age is the
arrival time of the oldest record that is buffered or being processed and not yet marked processed, including
retries and durable failure copies. Unlike a last-progress time, that value does not advance while one
partition is stuck and others keep processing. The oldest receiver determines liveness, so idle receivers
and healthy declarations on the same source cannot mask a stall. Before receiver startup and after receiver
shutdown or failure, state is unavailable and health is unhealthy. If consumer composition fails during
startup, a successful retry on the same host reports liveness for the newly composed receivers.
Connectivity and liveness are separate checks: an idle receiver does not prove broker connectivity.

Without this optional registration, Kafka consumers attach no native monitor and record no health state.
The Kafka transport itself has no dependency on the health-check packages. See [health integration](../guides/health-checks.md)
for endpoint registration, thresholds and the adapter contracts.

## Performance validation

`KafkaSendBenchmarks` compares acknowledged single sends and 32-message batches with raw Dekaf, with tracing disabled and `MemoryDiagnoser`. The `Tombstone` parameter covers native null values alongside ordinary records. `SendPipelineBenchmarks.TombstoneSend` and `ReceivePipelineBenchmarks.NativeTombstone` isolate typed pipeline cost. Set `RELAYER_KAFKA_BOOTSTRAP_SERVERS` to a test broker, then run:

```shell
dotnet run -c Release --project benchmarks/Relayer.Benchmarks -- --filter "*KafkaSendBenchmarks*"
```

Each benchmark creates and deletes its own topic. `Single` and `Batch` measure the serialized adapter seam. `DefaultJson` compares `ISender.SendAsync` with raw Dekaf: both serialize the same model using the registered default JSON serializer and generate fresh CloudEvents metadata. Tombstone cases skip serialization on both paths. Both use the same ordering key and producer defaults; the raw JSON producer uses Dekaf's byte-memory serializer without an extra body copy. Benchmark coverage alone does not establish the 5% throughput budget; that requires measurements in the same broker environment.

`KafkaReceiveBenchmarks` replays a fixed 4096-record topic through Dekaf's `RunPartitionedAsync` twice: once with a raw record handler and once with the production Relayer processor. Both runs use the same key ordering, handler limit and periodic commit policy, deserialize the same source-generated JSON contract, resolve the same scoped handler in a fresh DI scope, and include the stop commit. `HealthEnabled` attaches a native monitor to both runs. Relayer composes the executable `FailurePipeline`, native dead-letter copy factory, tracing, telemetry and commit observer as the Kafka hosted service does. No tracing or metrics listener is attached (D25); `MemoryDiagnoser` reports total allocations, including application DI lifetime costs (D52). With the same broker environment variable set, run `--filter "*KafkaReceiveBenchmarks*"`. Results are local diagnostics, not a claim that the 5% receive budget has passed.

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

Register `Customer` with `AddMessageTypesFromThisAssembly()` or `MessageTypes.Add<Customer>(...)`; do not register a separate identity or route for `Tombstone<Customer>`. Conflicting payload and tombstone identities throw `InvalidOperationException` during registration in either order, leaving existing registry mappings unchanged. A consumer can register both its regular handler and its tombstone handler. Tombstones never enter the JSON deserializer or upcaster chain. Native null records require a non-null key for typed dispatch; unkeyed null records follow the failure policy. The Kafka feature remains available and offsets commit only after durable handling.

This API requires a transport whose `TransportCapabilities.Tombstones` is true (currently Kafka). It does not enable broker log compaction; configure the topic's native cleanup policy separately. Tombstones can be buffered by a handler's send pipeline and retained in dead-letter copies. Database outbox persistence and durable redelivery of native null values remain unsupported: sends through an active outbox fail before enlistment. Use `SendWithoutOutboxAsync` for a direct native send when an outbox transaction is active.


## Inspect and redrive dead letters

Resolve the management service for a named Kafka transport from the built application's service provider.
It lists configured consumer dead-letter topics, including the default `<source>.dead-letter`.

```csharp
using Microsoft.Extensions.DependencyInjection;
using Relayer;

var manager = host.Services.GetRequiredKeyedService<IDeadLetterManager>("events");
var sources = manager.List();
var page = await manager.PeekAsync("orders.dead-letter", maxMessages: 20,
    cancellationToken: cancellationToken);
if (page.NextCursor is { } cursor)
{
    var next = await manager.PeekAsync("orders.dead-letter", maxMessages: 20,
        cursor: cursor, cancellationToken: cancellationToken);
}

if (page.Messages.FirstOrDefault()?.MessageId is { } deadLetterId)
{
    var redriven = await manager.RedriveAsync("orders.dead-letter",
        filter: message => message.MessageId == deadLetterId,
        maxMessages: 20, cancellationToken: cancellationToken);
}
```

`DeadLetterMessage.MessageId` identifies the Kafka record by topic, partition and offset, for example
`orders.dead-letter:0:42`. The original CloudEvents ID is available in the native record's `ce_id` header
with the default header prefix. To select by that original ID, inspect the header value:

```csharp
using Relayer.Kafka;

var redrivenByOriginalId = await manager.RedriveAsync("orders.dead-letter",
    filter: message => message.Features.GetRequired<KafkaDeadLetterFeature>()
        .Record.Headers.Any(header => header.Key == "ce_id" && !header.IsValueNull
            && header.Value.Span.SequenceEqual("order-event-123"u8)),
    maxMessages: 20, cancellationToken: cancellationToken);
```

Peek, redrive and purge read with Dekaf's `IsolationLevel.ReadCommitted` by default. Aborted transactional records are skipped, and records in an open transaction are unavailable until it commits. Set `native.WithIsolationLevel(...)` in `KafkaTransportOptions.ConfigureConsumer` to override the management reader's isolation.

All operations accept cancellation and a limit from 1 through 100. Peek and purge wait for native fetches until their record limit or partition EOF; a slow fetch is not treated as an empty partition. Pass a cancellation token to bound these waits. Peek does not commit or delete records. Each page reads one partition; a continuation can lead to an empty page before advancing to the next partition. Continue while `NextCursor` is non-null, and keep cursors scoped to their source. Broker retention, compaction and concurrent purge can change what a continuation returns. Snapshots own their storage: `KafkaDeadLetterFeature.Record` exposes the native key, offset, timestamp and ordered headers after the call returns. Its null `Value` denotes a tombstone, distinct from empty bytes; null keys and null header values are retained too.

Redrive sends to the configured original source topic using Dekaf and waits for a broker acknowledgment before committing the next offset in a dedicated native consumer group. The group is `relayer.deadletters.<name length>.<transport name>.<dead-letter topic>`; keep transport names stable across restarts to resume progress. Concurrent redrive calls use Kafka's native group partition ownership. Do not run unrelated consumers in that group. Resetting or deleting the group replays retained dead letters. Native group-offset expiration can also cause replay. Redrive preserves serialized bodies, binary keys, tombstones, duplicate/null headers and the original message identity; Kafka chooses the destination partition using the native producer configuration. Both non-null values (including empty bodies) and tombstones retain these headers. It does not preserve the original offset or append timestamp.

Redrive waits for native group assignment and reads until the scan limit or until every assigned partition reaches EOF or is paused by the filter. A slow group join or fetch is not treated as an empty topic. Pass a cancellation token to bound the operation, including when another group member owns all partitions.

Redrive is at-least-once: a crash, rebalance, cancellation or commit failure after an acknowledged send can produce another copy. It does **not** delete originals from the topic. Other groups can still read them until native retention, compaction or purge removes them. A filter scans at most the supplied limit. An unmatched record pauses its partition for that call, because Kafka cannot commit past it without losing it from future redrive. Later records in that partition remain behind the unmatched record; change the filter or redrive that prefix to reach them. Filter exceptions and failed sends do not advance progress for the failing record. Redriving to an actively consumed source can send a persistently failing record back to the dead-letter topic; fix the failure first.

`PurgeAsync(topic, maxMessages, cancellationToken)` scans bounded earliest partition prefixes and uses Dekaf's native `DeleteRecordsAsync` to advance their log-start offsets. This is permanent for **all** consumer groups, including ordinary inspectors and redrive. Kafka deletes every offset below each requested boundary, not selected records; the limit counts delivered records rather than offsets. Offset gaps, compacted records and records hidden by the configured isolation level do not count as scanned messages, but deletion still affects the entire prefix. Concurrent purge/retention may remove data while redrive runs. Purge can partially succeed across partitions before an error. No per-record deletion is emulated, and redrive never uses purge for settlement.

Management reuses `ConfigureConsumer` and `ConfigureProducer` for native authentication and connection configuration, while enforcing raw byte reads, manual commits, earliest reset and acknowledged redrive sends. Redrive replaces any subscription set by the callback with the selected dead-letter topic. Configure the admin client separately for metadata and purge:

```csharp
kafka.Options.Configure(options =>
{
    options.ConfigureAdmin = native => native.WithClientId("dead-letter-admin");
});
```

Use the same native TLS/SASL settings on all three builders when the cluster requires them. Provision topics externally and grant the native read, group-offset, produce, metadata and DeleteRecords permissions needed by the operations you expose. A dead-letter topic shared by different source topics has an ambiguous redrive destination, so resolving its manager fails with an actionable error; configure separate dead-letter topics for those consumers.

### Optional HTTP management

`Relayer.AspNetCore` exposes this named Kafka manager through the [shared HTTP management endpoints](service-bus.md#optional-http-management). Pass the Kafka transport's registered name to `MapRelayerDeadLetters`, and use a configured dead-letter topic as `source`. Apply the application's authentication and authorization policy as shown there; mapping alone does not secure inspection or destructive operations.

The HTTP API returns message IDs and base64-encoded body bytes, without native feature objects. A tombstone and an empty body both appear as empty bytes; use `IDeadLetterManager` directly to inspect `KafkaDeadLetterFeature.Record.Value`. HTTP redrive preserves tombstones and follows the same group-offset and filter rules above, and HTTP purge permanently deletes partition prefixes for all groups.

## Native per-send records and headers

Use `new SendOptions().Kafka(headers => headers.Add("source", "billing"))` to customize
native `Dekaf.Serialization.Headers` before sending. This wraps the existing native
header callback seam and works for single and batch sends. Native callbacks cannot be
persisted in an outbox.

Use an immutable native record transform for an explicit partition or event timestamp:

```csharp
await sender.SendAsync(order, options => options
    .Kafka(headers => headers.Add("source", "billing"))
    .KafkaRecord(record => record with
    {
        Partition = 2,
        Timestamp = eventTime,
    }));
```

`KafkaRecord` receives Dekaf's `ProducerMessage<WireString?, (ReadOnlyMemory<byte> Body, Headers Headers)?>`.
Return the record to send, normally with a `with` expression. Unset partition and timestamp
keep Dekaf's native defaults; broker timestamp settings still apply. The topic must remain
the declared route destination. Header callbacks and record transforms run in registration
order. The original tuple's headers preserve the resolved envelope during serialization;
use `Kafka(...)` for changes to those headers.

Transforms work for individual sends, same-topic and mixed-topic batches, and tombstones.
Batches containing a transform use native individual sends and drain every admitted operation
before returning borrowed storage, including on failure. Dekaf still owns network batching,
partitioning, ordering and acknowledgments. The record and all referenced memory are borrowed
until the send completes: do not retain them, and keep application-provided replacement memory
alive and unchanged until completion. Return a non-null record. Transforms cannot be persisted
in an outbox; use a direct send outside the active outbox transaction.

Only this opt-in path creates immutable native records (including copies made by `with`).
Ordinary sends retain the allocation-free component overloads. `KafkaSendBenchmarks.NativeRecord`
compares the opt-in path alongside ordinary sends and tombstones; its raw baseline reuses
preconfigured native records, so record construction cost appears on the Relayer side.

### Send pool retention

Kafka send-state pools retain at most 256 idle operation objects per pool. Concurrent sends above
the bound remain supported and allocate temporary overflow state on each burst. Native completion
still controls the lifetime of borrowed message memory; state is cleared before retention or collection.
The bound limits object count, not bytes: retained state can keep storage sized for its largest batch.
There is no retention option.

`TransportPoolRetentionBenchmarks` measures warmed pool reuse at concurrency 1 and 256 and repeated
512-operation bursts with `MemoryDiagnoser`. Measurements isolate pooling from broker I/O and do not
establish whole-path throughput parity.
