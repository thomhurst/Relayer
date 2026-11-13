# Relayer — Design

Status: draft v0.1 (2026-10-31)

Relayer is a modern .NET messaging library. It wraps existing messaging technologies (Kafka, Azure Service Bus, Amazon SQS/SNS, RabbitMQ, and more) and takes away the ceremony around them: resiliency, outbox/inbox, telemetry, trace propagation, serialization, topology, and testing.

It is **not** a new broker abstraction. It does not change how the underlying technology sends, receives, orders, or acknowledges messages. It runs a thin, fast pipeline around the native client and gives you full access to that client whenever you need it.

---

## 1. Principles

1. **Wrap, don't replace.** Every transport uses a native .NET client for its technology: Dekaf for Kafka, and the official SDKs for Azure Service Bus, SQS/SNS, and RabbitMQ. Native settlement, ordering, partitioning, sessions, and dead-lettering are used as-is. Relayer never emulates a feature the broker already has, and it calls a feature the native client already ships rather than re-implementing it (D42).
2. **No lowest common denominator.** Portable features (retry, outbox, tracing) are uniform. Transport-specific features are exposed through typed, discoverable extensions rather than hidden behind a generic API.
3. **Explicit over magic.** No assembly scanning. No runtime code generation. Every handler, route, and piece of topology is declared in code or produced by a source generator you can read.
4. **Correct by default.** The safe behaviour is the default behaviour: at-least-once delivery, idempotent consumers via the inbox, poison messages go to a dead-letter queue (they never crash or stall a consumer), and settlement happens only after the work is durable.
5. **Native AOT and trimming from day one.** No reflection on the hot path. Serialization uses System.Text.Json source generation by default. Optional native integrations declare upstream restrictions explicitly: Avro (D39) and SQL Server runtime operations (D46) currently report build diagnostics for unsupported trimming/AOT use.
6. **Performance is a feature.** The pipeline is composed once at startup, the hot path allocates as little as possible, and CI gates on benchmarks measured against the raw client.
7. **Observable by default.** OpenTelemetry tracing and metrics, plus structured logging, are built into the core rather than shipped as an add-on.
8. **Few dependencies.** The core depends only on `Microsoft.Extensions.*` abstractions and the BCL. Each transport package adds only its native client.
9. **Permissive licence.** MIT, permanently.

The PostgreSQL durable local transport is a scoped exception to broker wrapping: D66 requires it to reuse existing outbox rows, native database transactions, lane leases and poison handling. [Usage and transaction boundaries](../website/docs/transports/postgresql-local.md) describe its explicit registration.

### Non-goals

- Replacing or reimplementing broker features (Relayer does not emulate topics on SQS or partitions on RabbitMQ).
- Exactly-once delivery across arbitrary systems. Relayer provides at-least-once delivery plus idempotency. The one exception is Kafka-to-Kafka exactly-once, available via native Kafka transactions.
- Being an in-process mediator first. An in-memory transport exists for modular monoliths and tests, but it is just another transport.

### Target platform

- `net10.0` (current LTS) and `net11.0`. .NET 8 is out of support and is not targeted.
- `net11.0` builds compile with runtime async; `net10.0` builds use compiler-generated state machines.
- The latest C# version of the pinned SDK. Source generators are incremental generators.

---

## 2. Core model

```
          ┌──────────── send side ────────────┐            ┌────────── receive side ──────────┐
 your code → ISender → send middleware → outbox? → transport → native client → broker → native client → transport
                                                                                                │
                                         handler ← consume middleware (retry, inbox, telemetry…) ┘
```

### 2.1 Concepts

| Concept | Meaning |
|---|---|
| **Transport** | An adapter over one native client (Kafka, ASB, SQS, RabbitMQ, …). Registered with a name, so you can have several of the same kind. |
| **Endpoint** | A native destination or source: a Kafka topic, an ASB queue or topic/subscription, an SQS queue, a RabbitMQ exchange or queue. |
| **Route** | A mapping from a message type to an endpoint, plus how to derive native properties (key, session ID, partition key, message group ID). |
| **Consumer** | A subscription to an endpoint that dispatches to handlers. |
| **MessageContext** | The per-message context. Holds the message ID, headers, the trace context, the attempt count, and a feature collection that exposes the native message. |
| **Capabilities** | What an endpoint can do natively (DLQ, delayed delivery and redelivery, ordering, batching, transactions, max message size). Declared per endpoint, because an SQS FIFO queue and a standard queue differ. Relayer validates configuration against capabilities at startup. |

### 2.2 Feature collection: the native escape hatch

`MessageContext` follows the same pattern as ASP.NET Core's `HttpContext.Features`. Each transport publishes typed features, and extension methods make them discoverable:

```csharp
public async ValueTask HandleAsync(OrderPlaced message, MessageContext context, CancellationToken ct)
{
    var kafka = context.Kafka();               // IKafkaConsumeFeature, throws if not Kafka
    _logger.LogInformation("partition {P} offset {O}", kafka.Partition, kafka.Offset);

    ConsumeResult<byte[], byte[]> raw = kafka.Result;   // the untouched Dekaf record
}
```

On the send side, a typed callback receives the native outgoing object just before it is handed to the client:

```csharp
await sender.SendAsync(new ChargeCard(orderId, amount),
    o => o.ServiceBus(m => m.SessionId = customerId.ToString()), ct);
```

Native configuration is passed straight through and never re-modelled: Dekaf's `ProducerBuilder` and `ConsumerBuilder`, `ServiceBusClientOptions`, `ConnectionFactory`, and `AmazonSQSConfig`.

### 2.3 Capabilities

Each transport declares the capabilities of each endpoint (decision D24). A feature that needs one either uses the native mechanism or a documented fallback, and when there is neither, startup fails with a clear error (no silent degradation). The core runs every fallback, so transports contain native code only (§2.4).

`TransportCapabilities` is a sealed record with init-only values. A new record declares queue consumption, no optional features or native delays, and a maximum batch count of one. `MaxRedeliveryDelay = TimeSpan.Zero` declares immediate redelivery; `null` declares none. `MaxMessageBytes = null` means the endpoint limit has not been declared, not that messages are unlimited. Native envelope and batch byte limits still apply.

Transport stubs declare only conservative known values; native adapters must resolve configuration-dependent limits and features at startup. Adapters with asynchronous native evidence implement `ITransportCapabilityInitializer`; hosted startup awaits it before caching endpoint capabilities and composing send routes. None and Validate remain read-only. RabbitMQ verifies optional delayed exchanges through explicit native provisioning; otherwise it retains the outbox scheduling fallback. In particular, a Service Bus endpoint name does not establish session support, and a RabbitMQ queue name does not establish delayed-message plugin support. SQS declarations distinguish standard and FIFO queues: [FIFO queues do not support per-message delivery timers](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-message-timers.html).

Send routes declare known scheduling needs with `SendRouteOptions.RequiredDeliveryDelay` and select transaction guarantees with `TransactionRequirement` (`None`, `Native`, `Outbox`). Composition validates native limits or the actual registered outbox executor and due-row workers once. Native transaction availability never implies database durability or automatic native transaction ownership. Runtime checks reject unavailable schedules and missing active outbox transactions before serialization. See the [send guide](https://relayer.tomlonghurst.dev/docs/guides/sending) for configuration and executor contracts.

The native mechanisms and core fallbacks are:

| Feature | Native when available | Fallback |
|---|---|---|
| Delayed delivery / scheduling | ASB `ScheduledEnqueueTime`, SQS per-message `DelaySeconds` on standard queues (≤ 15 min; unavailable on FIFO queues), RabbitMQ delayed-message plugin | Outbox `deliver_at` |
| Delayed redelivery of a received message | SQS visibility timeout (≤ 12 h); immediate only: ASB abandon, RabbitMQ requeue | Scheduled re-send of a copy, else outbox `deliver_at` |
| Dead-letter with error metadata | ASB DLQ (`DeadLetterMessageAsync` with properties) | Send a copy to the dead-letter endpoint (Kafka DLT topic, SQS DLQ queue), then complete |
| Ordering | Kafka partition key, ASB sessions, SQS FIFO message group | None — must be declared unordered |
| Batching | Kafka, SQS (10), ASB batches | Client-side batching |
| Transactions | Kafka transactions, ASB cross-entity transactions | Outbox |

### 2.4 Transport SPI

The transport SPI is the seam between the core and a transport (decision D23). The core owns everything that is the same on every broker: serialization, the CloudEvents envelope, routing, the failure pipeline and its fallbacks, the inbox, the outbox, and telemetry. A transport owns only what is native: the client, the receive loop, the mapping to native messages, and settlement. Generics carry the native types through the core, so the core is written once and still runs specialized, allocation-free code for each transport.

A transport author writes five small pieces.

**1. A message binding.** A struct that reads one native message type. Its members are static abstract, so the pipeline is specialized per transport and every call is direct and inlinable:

```csharp
public interface IMessageBinding<TIncoming>
{
    static abstract ReadOnlySequence<byte> GetBody(TIncoming message);
    static abstract bool TryGetHeader(TIncoming message, string name, out WireString value);
    static abstract int? GetDeliveryCount(TIncoming message);   // null when the broker does not count deliveries
}

// Azure Service Bus: TIncoming is whatever the native receive callback hands over.
public readonly struct ServiceBusBinding : IMessageBinding<ProcessMessageEventArgs> { … }
```

`WireString` is a struct that holds a header value as the native client stores it: a UTF-16 `string` (ASB, SQS) or UTF-8 bytes (Kafka, RabbitMQ). The core compares and parses it in place, and converts it to a `string` only when user code asks. Header names come from the transport's `HeaderNames`, which encodes its CloudEvents protocol binding (`ce_` for Kafka, `cloudEvents:` for AMQP). Relayer's own attributes, such as the attempt count and error metadata, are CloudEvents extensions, so they follow the same binding.

**2. The receive loop.** The transport keeps its native receive loop and calls the core once per message. The core returns a `Disposition`, a small struct, and the transport settles natively:

```csharp
var pipeline = pipelines.Create<ProcessMessageEventArgs, ServiceBusBinding>(consumer);   // once, at startup

processor.ProcessMessageAsync += async args =>
{
    var disposition = await pipeline.ProcessAsync(args, args.CancellationToken).ConfigureAwait(false);
    switch (disposition.Kind)
    {
        case DispositionKind.Complete:         await args.CompleteMessageAsync(args.Message).ConfigureAwait(false); break;
        case DispositionKind.Redeliver:        await args.AbandonMessageAsync(args.Message).ConfigureAwait(false); break;
        case DispositionKind.DeadLetter:       await args.DeadLetterMessageAsync(args.Message, disposition.DeadLetter.Headers,
                                                   disposition.DeadLetter.Reason, disposition.DeadLetter.Description).ConfigureAwait(false); break;
        case DispositionKind.SettledByHandler: break;   // the handler settled natively, e.g. Defer
    }
};
```

The core never returns a disposition the endpoint's capabilities do not declare. If ASB declares immediate redelivery only, a 10-minute retry arrives as a scheduled re-send done by the core, followed by `Complete`. If Kafka declares no native dead-letter, a dead-lettered record arrives as a send to the DLT topic, followed by `Complete`. Transports therefore contain no fallback code. Batch receive works the same way: `ProcessBatchAsync(ReadOnlyMemory<TIncoming>, Memory<Disposition>, ct)` fills one disposition per message.

The pipeline puts the native message in the feature collection as `NativeMessage<TIncoming>`, which is pooled with the context. A transport's `context.ServiceBus()` extension wraps it in a struct view, so native access costs no allocation and no per-transport feature class.

The implemented receive seam adapts a composed `ConsumePipeline` with `new ReceivePipeline<TIncoming,TBinding>(consumer, headerNames, contextPoolCapacity)`. See [Receiving messages](https://relayer.tomlonghurst.dev/docs/transports/receive) for binding, settlement, pooling, and batch lifetime contracts. Failure-policy and capability fallback composition remain separate from this seam.

**3. Sending.** The core hands the transport a transport-neutral `OutgoingMessage`: destination, body bytes, named headers, ordering key, `DeliverAt`, and the native configurators from `SendOptions`. Its fields are exactly an outbox row (§6.3), so direct sends, outbox dispatch, scheduled re-sends and dead-letter copies all use the same two methods:

```csharp
ValueTask SendAsync(OutgoingMessage message, CancellationToken cancellationToken);
ValueTask SendBatchAsync(ReadOnlyMemory<OutgoingMessage> messages, CancellationToken cancellationToken);
```

The transport builds its native message, applies `options.ApplyNative<TNative>(native)`, and sends. Native configurators are delegates and cannot be stored in the outbox, so a send that has them and is enlisted in the outbox fails immediately with an explanation instead of dropping them. The resolved ordering key is a nullable `WireString`: string selectors keep their text, and scalar selectors format into pooled UTF-8 memory. The transport and outbox follow the same borrowed-memory lifetime for that key as for body and headers.

**4. Capabilities and topology.** `GetCapabilities(endpoint)` returns a `TransportCapabilities` record for one endpoint (D24). `ApplyTopologyAsync(mode, ct)` validates or provisions the endpoints the transport's builder declared and returns a report the core uses for the startup diff and for topology export.

Service Bus sending uses native queue/topic/subscription declarations. Least-privilege Validate checks send-link accessibility and peeks subscriptions without sending probe messages or using administration APIs. Entity descriptions require Manage rights, so Validate cannot compare native settings or establish session ordering; declared session requirements fail explicitly. Explicit Provision verifies native settings and creates missing entities without updating or deleting existing ones. See [Service Bus](https://relayer.tomlonghurst.dev/docs/transports/service-bus) for ownership and validation boundaries. Hosted queue and subscription receivers compose the durable core pipeline with native processors. Session processors retain the original native callbacks. Both reject auto-complete and ReceiveAndDelete; settlement runs after scope disposal. Immediate redelivery uses native abandon and error-property updates; delayed redelivery is rejected instead of copying subscription retries back to the topic.

**5. Builders.** The core ships generic builders, and the transport supplies its own option types:

```csharp
public sealed class RouteBuilder<TMessage, TRouteOptions>   { … OrderingKey(…) … }      // core
public sealed class ConsumerBuilder<TConsumerOptions>        { … Handler<T>(), Inbox(), Failures(…) … }  // core

public static ConsumerBuilder<KafkaConsumerOptions> Group(this ConsumerBuilder<KafkaConsumerOptions> consumer, string group);  // Kafka
```

Handlers, inbox, failure policy and ordering keys are implemented once in the core and work the same on every transport. A Kafka-only setting such as `Group` exists only on Kafka's builder, so it cannot be applied to a queue by mistake. `OrderingKey` accepts a `string` or any `IUtf8SpanFormattable` value such as `Guid` or `long`; the latter is formatted into a pooled buffer, so a Kafka key costs no allocation.

**Log-based transports.** Kafka, Event Hubs, Kinesis, Pulsar, RabbitMQ Streams and JetStream share ordering and durable-checkpoint requirements (§7), but their native clients may already implement scheduling, backpressure and ownership drain. Search the pinned client's source first and wrap those mechanisms (D1, D40, D42). Kafka's planned parallel receive path wraps Dekaf's `RunPartitionedAsync`; it does not use the core scheduler.

The core's `PartitionScheduler<TIncoming, TBinding, TPartition, TPosition>` is a fallback only for a log transport whose native client has no equivalent. A missing piece in an existing native runtime belongs upstream; it does not justify replacing that runtime. Record any exception and its native-feature gap in the decisions record before building a parallel implementation. When that fallback is justified, the transport feeds records in and implements three calls:

```csharp
public interface IPartitionSource<TPartition, TPosition>
{
    void Commit(TPartition partition, TPosition position);   // last completed position; the transport batches and flushes
    void Pause(TPartition partition);
    void Resume(TPartition partition);
}
```

`TPosition` is whatever the broker uses: a `long` offset for Kafka, a sequence-number string for Kinesis, a `MessageId` for Pulsar. The scheduler never compares positions; it tracks them in arrival order.

The implemented fallback [partition scheduler](../website/docs/transports/partition-scheduler.md) uses `IPartitionMessageBinding<TIncoming>` for native key equality and lease release, and the [completion-window primitive](../website/docs/transports/partition-completion.md) for bounded arrival-order checkpoints. One transport-owned pump serializes admission, dispatch, completion observation and native calls without per-delivery tasks or continuations. Assignment tokens fence stale completions; configurable global/per-assignment limits include retained work, and global limits include late handlers after ownership ends. Revoke stops dispatch and drains active work up to a monotonic deadline; loss fences immediately. No production transport in `src/` currently uses this scheduler. When designing the next log transport, check its native client before choosing the fallback; remove the public scheduler if no planned transport needs it (D23).

Every transport proves the SPI contract by passing the transport conformance suite (§15). The in-memory transport is the first implementation, in phase 0.

---

## 3. Developer experience

### 3.1 Configuration

The core configuration surface is available now. Transport packages supply the three option types and wrap `AddTransport` in typed extensions. The [Kafka adapter](../website/docs/transports/kafka.md) implements producers and native partition processing through `Kafka`. Transport pages describe each adapter's implemented features. The following call site uses the real core API:

```csharp
var services = new ServiceCollection();

services.AddRelayer(relayer =>
{
    relayer.AddMessageTypesFromThisAssembly();
    relayer.AddTransport<BrokerOptions, RouteOptions, ConsumerOptions>("events", broker =>
    {
        broker.Options.Bind(configuration.GetSection("Events"))
            .Validate(o => !string.IsNullOrWhiteSpace(o.Server), "Server is required.");

        broker.Send<OrderPlaced>("orders.placed", route => route.OrderingKey(m => m.OrderId));
        broker.Consume("orders.placed", consumer =>
        {
            consumer.Options.Group = "billing";
            consumer.Handler<OrderPlacedHandler>().Concurrency(8)
                .Batch(maxSize: 10, maxWait: TimeSpan.FromMilliseconds(20));
        });
    });
    relayer.TopologyMode = TopologyMode.Validate;
});

// Example option types; an adapter supplies its actual native option types.
public sealed class BrokerOptions { public string Server { get; set; } = ""; }
public sealed class RouteOptions { }
public sealed class ConsumerOptions { public string Group { get; set; } = ""; }
```

`Send<T>` declares a destination; publishing versus sending is native routing, not a parallel API (§3.4). An ordering selector derives the native partition key, session ID, or message group ID. `string` and `IUtf8SpanFormattable` values such as `Guid` remain typed. The pipeline resolves the selector once at construction and calls it directly; it must never use `DynamicInvoke` or format scalars through `object`.

Names are case-sensitive and unique across all transport kinds. Several transports can use the same option type with independent named configuration. Read them through `IOptionsMonitor<BrokerOptions>.Get("events")`; binding uses the normal Options change-token mechanism. For Native AOT, enable `EnableConfigurationBindingGenerator` in the application project so concrete `OptionsBuilder<T>.Bind` calls use generated binding.

`AddRelayer` captures `RelayerConfiguration`; declarations freeze when their callbacks finish. Its first hosted service checks all declarations and all named option validators before later transport hosted services start, and reports their errors together in `OptionsValidationException.Failures`. Handler types are registered explicitly as scoped services, preserving existing registrations. Message names must be registered explicitly or through the generated `AddMessageTypesFromThisAssembly` method.

This API declares configuration. Startup validates runtime adapters' endpoint capability values and rejects ordering keys on destinations without native ordering support. Receive dispatch, sender execution, native adapters, failure policies, inbox/outbox integration and topology validation consume these declarations. Feature-specific capability and fallback selection belongs to the corresponding execution path. See [Writing a transport](https://relayer.tomlonghurst.dev/docs/transports/authoring) and [Configuration](https://relayer.tomlonghurst.dev/docs/guides/configuration) for the transport-author contract and validation boundaries.

### 3.2 Handlers

Handlers are plain classes that are resolved from a fresh DI scope for every message (or batch). They can inject scoped services such as a `DbContext`. A failure-pipeline consumer whose handlers are all singletons opens that scope on demand (D53).

```csharp
public sealed class OrderPlacedHandler(AppDbContext db, ISender sender) : IHandler<OrderPlaced>
{
    public async ValueTask HandleAsync(OrderPlaced message, MessageContext context, CancellationToken ct)
    {
        db.Invoices.Add(Invoice.For(message));
        await sender.SendAsync(new ChargeCard(message.OrderId, message.Total), ct);
        await db.SaveChangesAsync(ct);  // business data + outbox row + inbox row, one transaction
    }
}
```

There are also batch handlers. Each message in a batch can succeed or fail independently, and the transport settles each message natively.

```csharp
public sealed class AuditHandler : IBatchHandler<AuditEvent>
{
    public async ValueTask<BatchResult> HandleAsync(Batch<AuditEvent> batch, CancellationToken ct)
    {
        var failures = await _store.WriteAsync(batch.Items, ct);
        return BatchResult.PartialFailure(failures);   // only the failed messages are retried
    }
}
```

### 3.3 Registration without scanning

- `.Handler<THandler>()` is generic. The constraint `THandler : IHandlerMarker` together with source-generated descriptors gives the message type with zero reflection.
- Register handlers on their consumer with `consumer.Handler<THandler>()`. This selects dispatch and registers generated descriptors and DI together; there is no separate builder or assembly-wide handler registration (D72).
- Analyzers report problems at compile time: a handler that is registered nowhere, a message type with no stable name, a consumer with no handler, or a feature the transport does not support (for example, ordering on a non-FIFO SQS queue).

The [diagnostics reference](https://relayer.tomlonghurst.dev/docs/reference/diagnostics) documents `RLY001`–`RLY004`, their limits, suppression and the `[Message]` code fix. Transport capability validation remains a startup check when configuration values are not statically known.

Register handlers inside their consumer configuration callbacks:

```csharp
services.AddRelayer(relayer => relayer.Kafka("events", kafka =>
    kafka.Consume("orders", consumer =>
    {
        consumer.Options.Group = "orders-handler";
        consumer.Handler<OrderPlacedHandler>();
    })));
```

Generation supports inherited contracts, explicit interface implementations, and handlers with multiple message or batch contracts. Open generic, abstract, private nested, and file-local classes are excluded. Message types must be accessible to the generated code; `RLYH001` reports inaccessible contracts. Explicit `Handler<ClosedHandler<OrderPlaced>>()` also supports closed generic handlers and handlers in referenced assemblies. Repeated registration is idempotent. Handlers default to scoped DI registration, preserving an existing registration.

The core's `HandlerRegistry` freezes after configuration. Pipeline composition selects a `HandlerDescriptor<TMessage>` or `BatchHandlerDescriptor<TMessage>` once. For each message scope, its generated `Resolve` method uses typed DI resolution; `DispatchAsync` then calls the handler through its exact interface and returns its `ValueTask` unchanged. Dispatch does no type lookup, delegate creation, or allocation. Registry metadata uses CLR types only during configuration, never as wire identities.

### 3.4 Sending from anywhere

`ISender` is scoped. When an outbox is configured and a transaction is active in the current scope, sends are enlisted automatically. Otherwise they go straight to the transport. To be explicit, use `sender.SendNowAsync` (bypass the outbox) or `sender.SendViaOutboxAsync` (require the outbox; throws when there is no transaction).

Publish versus send is a routing concern, not a separate API. Whether a destination fans out depends on what the endpoint is natively (a topic or exchange versus a queue).

The core route registration seam, middleware lifetime, timeout contract, and scoped outbox integration are documented in [Sending messages](https://relayer.tomlonghurst.dev/docs/guides/sending).

---

## 4. Message contracts and wire format

### 4.1 Stable message type names

Messages are identified by a stable string name, never by the CLR type name. That way, renaming or moving a class never breaks consumers, and services do not need to share a contracts assembly.

```csharp
[Message("billing.charge-card", Version = 2)]
public sealed record ChargeCard(Guid OrderId, decimal Amount);
```

The identity is declared with `[Message]` or explicitly through `MessageTypeRegistry.Add<T>`. RLY001 warns when neither is visible in the current compilation; there is no CLR-name fallback. See the [message guide](../website/docs/guides/messages.md) for explicit registration and its analyzer limits.

### 4.2 CloudEvents by default

Relayer writes CloudEvents attributes using each transport's binary-mode binding, as native headers or properties: `ce_id` (UUIDv7), `ce_type`, `ce_source`, `ce_time`, `ce_specversion`, `traceparent`, and `tracestate`. As a result:

- Non-Relayer applications in any language can produce and consume the same messages (Wolverine #1592, CloudEvents requests).
- Consumers route by `ce_type`, so **several message types can share one topic or queue** (confluent#1183).
- A raw mode (no envelope, just the native payload) exists for integrating with legacy producers. In raw mode you map the type yourself, for example from the topic name.

The core header and envelope primitives, their native binding contracts, and raw-mode examples are documented in [Envelopes and headers](https://relayer.tomlonghurst.dev/docs/transports/envelopes).

### 4.3 Serialization

- The default is System.Text.Json with a source-generated `JsonSerializerContext`. The generator builds it from the declared message types, so serialization is AOT-safe.
- Serialization is pluggable (`IMessageSerializer`): optional Protobuf and MessagePack packages use explicit generated contracts and native buffer APIs. The optional Avro package uses generated codecs and startup-prepared schema registry integration; its AOT/trimming support remains limited by upstream schema preparation. See [serialization](../website/docs/guides/messages.md#protobuf) for registration and wire format.
- Serialization writes to `IBufferWriter<byte>` and reads from `ReadOnlySequence<byte>` where the native client allows it.

`AddMessageTypesFromThisAssembly()` registers the declared messages and their generated JSON context together. Pass `JsonSerializerOptions` to that method to customize JSON, or add a context with `UseJsonSerializer(MyJsonContext.Default)`. When contexts overlap, the last registered context supplies the metadata for that type. A custom context can also cover explicitly registered message types from a contracts assembly. Missing metadata fails when the default serializer is first resolved, including at host startup through Relayer's hosted validator; there is no reflection fallback.

```csharp
services.AddRelayer(relayer =>
{
    relayer.AddMessageTypesFromThisAssembly(new JsonSerializerOptions
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase
    });
    // Optional: override metadata for the types covered by this context.
    relayer.UseJsonSerializer(MyJsonContext.Default);
});
```

Register a custom `IMessageSerializer` in the service collection before, inside or after `AddRelayer` to replace JSON. The default uses a lazy DI factory, so the final serializer registration determines whether JSON metadata is needed. Call `AddRelayer` once per service collection; a second call throws before running its configuration callback. Registrations on separate service collections keep their generated contexts, options and writer pools isolated. Typed metadata is resolved at startup. Serialization reuses a bounded writer pool per options instance; deserialization reads the sequence directly and rejects null bodies and trailing JSON values.

Named transports can override the application serializer with `transport.UseSerializer(factory)`. The host resolves each keyed singleton once, awaits optional `IMessageSerializerInitializer` preparation before receivers start, and captures it in send and consume pipelines. See [named Kafka schema registries](../website/docs/transports/kafka.md#independent-schema-registries) for the optional startup-prepared JSON integration and its explicit contract limits.

The optional [Avro/schema registry adapter](../website/docs/guides/messages.md#avro) reuses Dekaf's generated codecs and startup preparation under D39. Its native schema parser currently prevents Native AOT/trimming; registration exposes that limitation through build diagnostics rather than a reflection fallback.

### 4.4 Versioning

- `Version` lives in the type name (`billing.charge-card;v=2`).
- Upcasters (`IUpcaster<V1, V2>`) are registered explicitly and run before the handler.
- Unknown types or versions are treated as poison and dead-lettered with a reason. They are never ignored silently and never crash the consumer.

---

## 5. Failure handling

There is one pipeline for every transport. Every step is visible in headers, logs, and metrics.

```
exception
   │
   ├─ classify ──► permanent (configured types, deserialization, unknown type) ──► dead-letter
   │
   ├─ immediate retry (in-process, jittered backoff, N attempts)
   │
   ├─ delayed redelivery (native delay if supported, else outbox) with jitter
   │
   └─ dead-letter (native DLQ, or DLT endpoint) ──► redrive API
```

- **Classification** is a predicate per exception type or a custom `IFailureClassifier`. Deserialization and contract failures are permanent by default, so they never block a partition or crash a consumer (Silverback #249, SlimMessageBus #438).

Compose exception rules once and apply them with `consumer.Classify(classifier)`; the low-level composer also accepts the classifier directly:

```csharp
var classifier = new FailureClassifierBuilder()
    .On<ArgumentException>(FailureAction.DeadLetter)
    .On<TimeoutException>(FailureAction.DeadLetter,
        static (_, context) => context.Attempt > 5)
    .Build();
```

Rules match the declared exception type and its derived types, in registration order. The first matching rule with a true predicate wins. Unmatched failures use `DefaultFailureClassifier`, or the custom `IFailureClassifier` passed to `Build`. Explicit rules can override permanent defaults. Builders freeze after `Build`; the resulting classifiers can be shared across consumers. Predicates and custom fallbacks must support concurrent calls. Predicate failures propagate without producing a settlement decision. Classification does not retry, send, or settle a delivery; the consumer's failure handler owns durable failure handling.

Declare portable policy with `consumer.Failures(policy)` on the core builder for broker and in-memory consumers. The declaration stores `FailureOptions` and an optional per-consumer classifier, which overrides a global registration. Bindable delay arrays use null `RetryDelays` for the default sequence so configuration binding replaces rather than appends defaults. Adapter-owned native options contain no retry/redelivery/dead-letter policy. PostgreSQL local consumers retain their fenced outbox row recovery policy and reject a separate consumer policy.

Transport authors compose executable failure handling before starting native receive services:

```csharp
var failures = new FailurePipeline(services, "broker", "orders", "billing",
    new FailureOptions
    {
        RetryDelays = new[] { TimeSpan.FromMilliseconds(100), TimeSpan.FromMilliseconds(500) },
        RedeliveryDelays = new[] { TimeSpan.FromSeconds(30), TimeSpan.FromMinutes(5) },
        DeadLetterEndpoint = "orders.errors"
    }, copy: CopyOriginalDelivery, scheduler: durableScheduler);

var consumer = new ConsumePipelineBuilder(scopeFactory, messageTypes, serializer, failures, classifier)
    .Handler(orderHandlerDescriptor).Build();
var receive = new ReceivePipeline<NativeDelivery, NativeBinding>(consumer, headerNames);
```

`services` is the application service provider. `CopyOriginalDelivery` reads the native feature and returns
an `OutgoingMessage` with the original body, **all** headers, and ordering key. It must preserve malformed
envelopes as-is; it must not access `context.MessageType` to rebuild them. A copy factory is required only
when a configured path needs a copy. `IFailureScheduler` is an available durable executor, not an Options
flag: it must commit the scheduled message independently of the failed handler transaction before returning.
Concrete outbox integrations supply that executor. No outbox integration is implied by configuring delays.

Composition reuses the hosted validator's endpoint capability cache. It validates every configured delay
(including the longest) and the dead-letter destination, and requires each fallback's executor. Native
redelivery wins within `MaxRedeliveryDelay`, then native scheduled copy within `MaxDeliveryDelay`, then
a durable scheduler. Native dead-lettering is used only when `DeadLetter` guarantees error metadata.
Each delay entry permits one further attempt. The default policy uses three in-process retries at 100 ms, 500 ms and 1 s before jitter, with no durable redelivery (D69). `FailureOptions.None` or explicitly empty retry/redelivery lists select immediate dead-lettering for retryable failures. Permanent failures still bypass retries.
Jitter shortens delays by up to 20% by default, keeping configured capability boundaries valid.

Each immediate retry uses a fresh async DI scope. Direct `ISender` sends, including `SendNowAsync`, are
serialized into reusable buffers and sent only after the handler, middleware and async scope disposal succeed. Failed attempts
clear those buffers. Await every send started by the handler; concurrent awaited sends use distinct buffer slots. Native send callbacks run during the flush after scope disposal. The scoped sender is pooled for failure-enabled consumers; ordinary sender scopes
retain their existing behavior. Active outbox sends still enlist in their transaction: the transaction owner
must roll back a failed attempt, as required by the outbox contract. Native client calls made directly by user
code remain the user's responsibility. A failure while flushing successful sends propagates without an
in-process retry or successful settlement, because an earlier send may already have succeeded.

Copies preserve the original identity and creation context. Native redelivery dispositions carry
`FailureHeaders` that the adapter **must persist before releasing the original**. Native dead-letter dispositions
carry the same history in `DeadLetterInfo.Headers`. Their owned metadata survives pooled-context disposal.
Adapters must not advertise native redelivery if they cannot preserve that metadata. History records the
original ID (null for a missing ID), total failed attempts, first/last failure times, and retained attempts' consumer,
host, exception type, message and stack. D75 bounds diagnostics while retaining total counts. Error metadata uses generated JSON, including under Native AOT.
Malformed failure counters are permanent envelope errors; corrupt history is replaced so poison messages can
still reach the dead-letter destination. History construction and exception type diagnostics run only on failure.

The receive adapter returns Complete for a copy fallback only after the send or durable schedule succeeds.
Fallback exceptions, timeouts, cancellation, scope-disposal errors and flush failures propagate without a
settlement decision. The native adapter must retain or release the delivery using its own failure semantics.
Copies remain at-least-once: a crash between the copy and completion can duplicate it (D24).

Manual settlement requests follow D11: the pooled `ISettlementFeature` queues an outcome for a successful
attempt, while exceptions override it. The same durable fallback executors resolve manual retry and
dead-letter requests. See [Failure handling](https://relayer.tomlonghurst.dev/docs/guides/failure-handling#manual-settlement)
for the call site, precedence and buffered-send contract.


- **Error metadata** goes on the dead-lettered message: the original message ID, exception type, message and stack trace for retained attempts (D75) (Rebus #1154), attempt count, first and last failure time, consumer name, and host. SQS visibility retries preserve immutable native attributes: their final copy retains previously stored history and the current delivery’s in-process failures; earlier visibility-delivery exceptions are unavailable from SQS (D67).
- **Redrive API:** `IDeadLetterManager` provides `List`, `Peek`, `Redrive(filter)`, and `Purge`, backed by native broker mechanisms. Queue redrive settles an original after an acknowledged send; Kafka redrive commits a dedicated group offset after that send and leaves the original subject to native retention. Kafka purge deletes scanned partition prefixes with native `DeleteRecords`, never arbitrary records. See the [site roadmap](../website/docs/roadmap.md) for implemented transports and optional endpoint/CLI status.
- **Circuit breaker:** when the failure rate of a consumer crosses a threshold, Relayer **pauses the consumer natively** (Kafka `Pause`, ASB processor stop, RabbitMQ consumer cancel) instead of burning messages into the DLQ (SlimMessageBus #454). It then half-opens with a probe message.
- **Retries never duplicate side effects.** Outbox rows written by a failed attempt are rolled back with the transaction. Without a transaction, sends made during a failed attempt are buffered and discarded (Rebus #1198, Rebus.PostgreSql #53).
- Relayer implements this pipeline itself, because it has to integrate with native settlement and delayed redelivery, which Polly cannot model. Inside a handler, you remain free to use Polly or `Microsoft.Extensions.Resilience` for downstream calls.

---

## 6. Outbox and inbox

The outbox is the area where every existing library has had correctness bugs, so it gets the most design attention.

### 6.1 Guarantees

- A message is written in the **same database transaction** as your business data. It is dispatched if and only if that transaction commits.
- Delivery is at-least-once. The message ID (UUIDv7) is stable across retries and dispatches, so the consumer-side inbox deduplicates.
- Ordering is preserved per ordering key when the route declares it (Kafka key, ASB session, SQS FIFO message group). Lanes follow a database-assigned insert sequence (D50), preserving sequential transactions' commit order independently of host clocks. Genuinely concurrent transactions have no defined order.
- A poison outbox row never blocks unrelated messages.
- The design is safe with any number of instances. There is no double dispatch under normal operation, no cleanup race between instances, and no per-replica background-job storms.

### 6.2 Enlisting: hard to misuse

- **EF Core:** Relayer registers a `SaveChangesInterceptor` and a `DbTransactionInterceptor`. Before an implicit save, `context.EnlistRelayerOutbox()` explicitly selects the context that owns buffered sends. An explicit EF transaction activates enlistment automatically when unambiguous. `SaveChangesAsync`, or committing the EF transaction directly, writes business data and outgoing rows atomically. You do not need to override `SaveChanges` (Silverback #164), and it works with **multiple DbContexts** (Silverback #163). Commit through EF's `IDbContextTransaction`, including its `UseTransaction` wrapper: calling the underlying `DbTransaction.Commit` bypasses EF interception. D31 records ownership and failure boundaries; [the site](https://relayer.tomlonghurst.dev/docs/reference/ef-core-outbox) documents implemented PostgreSQL enlistment and remaining dispatch work.
- **ADO.NET / Dapper:** `await using var tx = await outbox.BeginTransactionAsync(connection)` returns a `DbTransaction` subclass. Commit writes the shared outbox batch before the native commit. Create business commands through `tx.Connection`, including Dapper calls: Npgsql commands from the original connection cannot accept a wrapper transaction (D32). Rollback and disposal discard buffered messages. See [PostgreSQL transactions](../website/docs/reference/postgres-outbox.md) for ownership and recovery boundaries. SQL Server follows the same command-connection lifecycle (D47), flushing reusable rows through native `SqlBulkCopy` on the business transaction. Its opt-in unordered commit dispatcher uses native fenced batches (D49); see [SQL Server transactions and dispatch](../website/docs/reference/sqlserver-store.md). SQL Server offers an opt-in ADO.NET consumer inbox boundary using native key-range locking and the same outbox transaction (D59). SQL Server ordered dispatch uses conditional lane leases and native sequence ranges (D64); EF Core remains planned; unordered recovery and maintenance are implemented (D62). MySQL ADO.NET/Dapper enqueue (D48) also supports opt-in commit-triggered unordered dispatch using native MySqlBatch and fenced row counts (D55); its opt-in ADO.NET/Dapper consumer boundary commits inbox identity, business writes and outgoing rows atomically before settlement; ordered workers use database-assigned insertion sequences and fenced InnoDB lane leases (D50, D63); leased unordered recovery, poison isolation and bounded dead/inbox retention reuse the direct dispatcher (D65).

- **MongoDB / DynamoDB:** use their session and transaction APIs in the same style.

### 6.3 Schema

```
relayer_outbox
  id             uuid (v7)       primary key
  seq            bigint          database-assigned insert identity (outbox only)
  lane           integer         hash(ordering_key) % lanes, or null when unordered
  ordering_key   text            nullable
  transport      text
  destination    text
  message_type   text
  headers        bytes           (includes the creation context captured at enqueue, §8.2)
  body           bytes
  created_at     timestamptz
  deliver_at     timestamptz     nullable (scheduling fallback)
  attempts       int
  last_error     text
  tenant_id      text            nullable
  status         text            pending or parked
  claim_token    uuid            nullable, unique per dispatch attempt
  claim_expires_at timestamptz   nullable

index (lane, seq) where status is pending and lane is not null        -- partial/filtered where the database supports it
index (id) where status is pending and lane is null
index (lane, ordering_key, seq) where status is parked
index (deliver_at) where deliver_at is not null

relayer_leases
  name           text primary key  -- 'lane:7', 'cleanup', 'scheduler'
  owner          text
  expires_at     timestamptz
```

PostgreSQL ships one embedded SQL creation script, exposed by the database CLI and an application-owned
EF Core migration operation (D30). D50 adds an explicit insert-sequence SQL upgrade for existing PostgreSQL and SQL Server tables; creation does not upgrade them. Existing rows keep their former UUID order because historical insertion order cannot be recovered. Other database stores' ordered workers adopt native identity/sequence ordering when implemented. Indexes are created by default
(CAP #1599). Tables can be prefixed and given a schema (NSB.Persistence.Sql #2093). See
[schema deployment and migration usage](../website/docs/reference/postgres-schema.md).

The implemented [durable store contract](../website/docs/reference/outbox-stores.md) separates transaction
enlistment from dispatch reads and mutations. Reads fill caller-owned bounded buffers. Eligible rows are pending,
not parked or dead, due at the query instant and without a live claim. Health uses the same predicate across
all lanes. Claims use a unique token and an expiry; renew, completion and failure updates require the matching
live token, so a stale dispatcher cannot mutate a reclaimed row. PostgreSQL schema creation and the
[PostgreSQL read/claim store](../website/docs/reference/postgres-store.md),
[EF Core transactional enqueue](../website/docs/reference/ef-core-outbox.md),
[ADO.NET/Dapper transactions](../website/docs/reference/postgres-outbox.md),
[unordered direct dispatch](../website/docs/reference/direct-dispatch.md),
[ordered lane dispatch](../website/docs/reference/ordered-dispatch.md) and
[leased recovery and cleanup](../website/docs/reference/postgres-maintenance.md) are implemented; registering the read/claim store alone does not start dispatch.

### 6.4 Dispatching

There are two paths, each chosen per message based on whether its route is ordered:

1. **Unordered messages — direct dispatch.** After commit, the instance that committed dispatches immediately, in the background, without waiting for a poll. It claims an eligible row with a conditional update that sets a fresh claim token and expiry only when no live claim exists. Completion, renewal and failure updates require that token and an unexpired claim. This prevents concurrent claims and stale database mutations; a crash or a broker send that outlives its claim can still duplicate delivery, as allowed by the at-least-once guarantee. Latency equals the broker round trip (fixes the latency of polling designs, SlimMessageBus #275).
2. **Ordered messages — lane leases.** Rows are hashed into N lanes (default 32). Instances lease lanes through `relayer_leases` (SQL Server uses `relayer_ordered_leases`, separate from maintenance leases) with heartbeats and fair-share rebalancing as instances join and leave. A lane owner reads its lane in database-assigned `seq` order with a simple indexed range scan. There are no `SKIP LOCKED` scans across the whole table, and connected owners do not overlap. D34 specifies transaction locks, deadlines and the at-least-once/native-send failure boundary; D50 preserves sequential commit order using insertion sequence, while genuinely concurrent transactions have no defined order. Each ordered worker retains at most 256 successful endpoint capability lookups with FIFO eviction. An evicted endpoint is resolved and validated again before its next native send; failed lookups never displace valid entries.

The implemented PostgreSQL direct path transfers reusable buffers after EF commit to an explicitly named
hosted worker (D33). It claims each native group of due unordered rows with one conditional statement,
excludes competing claims, respects native batch-count limits and deletes successful groups with one
fenced statement. Completion errors after a successful send leave claims held without incrementing
send attempts; incomplete deletions warn about duplicate delivery. It does not poll, heartbeat
during native sends or retry failed commits. Queue overflow and shutdown leave durable work pending with
an error log; explicitly registered leased recovery polls those rows; ordered lane dispatch is implemented separately.
Each worker bounds successful endpoint capability retention to 256 entries using FIFO eviction (D35).
Cache hits remain read-only; an evicted endpoint is resolved again before claiming its next group.

A **sweeper** (also leased) picks up anything that was committed but not dispatched within a threshold, for example because the instance crashed. Postgres `LISTEN/NOTIFY` and SQL Server Service Broker can optionally replace the sweeper's poll.

After dispatch, rows are deleted in batches. Opt-in PostgreSQL sent history atomically copies only ID, destination and completion time into `relayer_outbox_sent` through the fenced delete statement. One leased **cleanup** job handles retention for that metadata, the dead table and inbox; pending and parked outbox rows never expire by age (CAP #1811, Rebus.SqlServer #101). D38 records bounded PostgreSQL recovery, leadership and retention semantics.

**Poison rows:** after a configurable number of attempts, the row is moved to `relayer_outbox_dead` and an alert metric is raised. For ordered routes, only later rows **with the same ordering key** are parked; the rest of the lane continues. Parked keys can be inspected and released through `IDeadLetterManager`.

**Change data capture:** the optional [PostgreSQL dispatcher](../website/docs/reference/postgres-cdc.md) uses native Npgsql logical replication to wake bounded unordered recovery on committed inserts, instead of waiting for a poll. A persistent slot retains WAL, startup catch-up covers pre-slot rows, and feedback follows durable dispatch transactions. Maintenance recovers failures, expired claims and future schedules; ordered dispatch retains its lane leases. SQL Server CDC remains planned. The unchanged table layout supports Debezium's event router with explicit column mappings and connector-specific header/payload handling, rather than its default schema. D56 records these boundaries.

### 6.5 Inbox

- `relayer_inbox(consumer, message_id, processed_at)`, written in the same transaction as the handler's business changes and outbox rows.
- A duplicate delivery is acknowledged without invoking the handler, and a metric is recorded.
- The inbox is opt-in per consumer. It is enabled automatically when the outbox is used on the same DbContext.

The implemented PostgreSQL EF Core consumer boundary explicitly selects its scoped DbContext at
composition time, validates its provider and owns the transaction around the complete handler attempt.
It inserts the inbox key before dispatch with `ON CONFLICT DO NOTHING`; the database unique constraint
coordinates overlapping instances and permits a waiting delivery to run if the first rolls back.
Outbox configuration on the selected context enables inbox deduplication automatically (D36).
The transaction uses the provider's default isolation level unless an explicit native isolation level is supplied.
Explicit isolation enables bounded PostgreSQL conflict replay through EF's native execution strategy
when no application retrying strategy is configured; configured strategies remain authoritative (D51).
Before commit the boundary requests completion if no settlement outcome is pending. The existing
one-request rule rejects settlement changes during scope disposal after the work is durable.
See [consumer configuration and failure boundaries](https://relayer.tomlonghurst.dev/docs/reference/ef-core-inbox).

SQL Server ADO.NET/Dapper consumers opt into `.SqlServer(consumer, connectionString, inbox: true)`
as their outer transaction boundary (D59). Native key-range locks and primary-key uniqueness protect
the inbox identity; business writes and outgoing rows share the existing outbox transaction.
See [SQL Server consumer boundaries](../website/docs/reference/sqlserver-store.md#atomic-consumer-inbox).

### 6.6 Multi-tenancy

- Application-owned scoped factories select native connections or DbContexts, supporting a database per tenant or a schema per tenant (CAP #699, Rebus #1218, Wolverine #556). Reuse native `AddDbContext`/Npgsql configuration instead of introducing a library-owned `IOutboxStoreResolver` facade (D45). Manual units, explicit tenant endpoints and incoming single-message EF consumers use native scoped factories. With tenancy registered, the EF boundary validates the selected context per delivery/retry scope without resolving a startup context or requiring a fallback tenant. See the [native tenant store contract](../website/docs/reference/tenant-stores.md).
- PostgreSQL `ITenantSource` enumerates immutable application-owned native store snapshots for one bounded scheduler. It reuses direct queues, unordered recovery, ordered lanes, native leases and cleanup without per-tenant hosted workers. All catalog activity, including lease release, shares `MaxConnections`; native unpooled dispatcher sources prevent idle per-source pool growth while business sources may remain pooled (D60). Store additions/removals take effect between finite turns. See [tenant catalog dispatch](../website/docs/reference/tenant-stores.md#dispatch-a-tenant-catalog).
- The tenant ID travels in a header and is restored into the consumer's scope. [Scoped propagation](../website/docs/guides/multi-tenancy.md) is implemented for single-message consumers and scoped sends, requests and replies. Application-authorized `OutboxTenantBinding` in native scoped store options binds durable tenant metadata to the current tenant and wire header (D45). Bounded PostgreSQL catalog dispatch is implemented; batch consumers cannot share a tenant scope.

---

## 7. Kafka done properly

Kafka gets its own consumption model instead of being forced into a queue shape (Wolverine #3134).

The transport wraps **[Dekaf](https://github.com/thomhurst/Dekaf)**, a pure C# Kafka client (decision D22). Dekaf has no native dependencies, supports Native AOT, and is built for zero-allocation hot paths, which matches Relayer's own goals. Dekaf already ships features such as partitioned processing, retry and dead-letter topics, an outbox, and OpenTelemetry instrumentation. Following principle 1, search the pinned client's source and reuse its mechanisms. A feature that cannot be used needs a recorded reason and an upstream issue; a parallel implementation needs an explicit decision. Relayer's outbox remains the cross-transport outbox.

- **Key-ordered parallelism (planned):** wrap Dekaf's `RunPartitionedAsync` with `Ordering = PartitionedProcessingOrder.Key` and `MaxConcurrentHandlersPerPartition`. Dekaf compares byte-array keys by content and owns the ordered lanes; Relayer invokes its receive/failure pipeline within the native processor. Kafka does not use the fallback core scheduler (§2.4).
- **Offset commits by watermark (planned for parallel receive):** Dekaf tracks the highest *contiguous* completed offset per partition and batches asynchronous commits through `PartitionCommitPolicy.CommitCompletedPeriodically`, including flush on revoke and shutdown (Brighter #2269). Relayer allows native processing completion only after `Complete` or `SettledByHandler` means the work is durable. Relayer supplies its commit span without a second checkpoint tracker.
- **Rebalance safety (planned for parallel receive):** use Dekaf's native cooperative assignment, `PartitionStopPolicy.Drain`, `StopTimeout` and `PartitionBackpressureMode.PauseResume`. The native runtime owns revoked lanes, drain and pause tracking, including rebalances while paused (KafkaFlow #456, #681). Preserve the configured native rebalance listeners.
- **Consume-transform-produce:**
  - *Kafka → database → anything:* the inbox, business data, and outbox are written in one database transaction. The offset is committed only after that transaction commits. Redelivery after a crash is deduplicated by the inbox.
  - *Kafka → Kafka:* optional native Kafka transactions (Dekaf's `SendOffsetsToTransactionAsync`) give exactly-once processing.
- **Tombstones:** `sender.SendAsync(Tombstone.For<Customer>(key))` sends a record with a null value (KafkaFlow #516). A consumer receives it as `Tombstone<T>`.
- **Ordered batch produce:** batch produce preserves order per key (KafkaFlow #594).
- **Replay and seek:** `IKafkaConsumerControl.SeekAsync(topic, partition | timestamp)` for operational replay (Wolverine #3479).
- **Schema registry:** works with multiple clusters, each with its own registry (KafkaFlow #558).
- **Logging:** Dekaf logs through `ILoggerFactory` (`WithLoggerFactory`), so Kafka client logs land in the app's logging pipeline with no native log bridge.

The shipped adapter still uses serial native fetch batches; see the [Kafka guide](../website/docs/transports/kafka.md) for the current API. [#22](https://github.com/thomhurst/Relayer/issues/22) owns the parallel adapter implementation (consolidating #182). Dekaf 1.26.0 supplies consumer-wide handler/buffer limits through `PartitionedProcessingOptions.MaxConcurrentHandlers` and `MaxBufferedRecords`, oldest outstanding record time through `PartitionedProcessingMonitor.OldestOutstandingRecordAt`, and runtime-aware offset/timestamp replay through `PartitionedProcessingControl.SeekAsync`. These close the native gaps tracked in Dekaf#4054, #4055 and #4056. The planned adapter must reuse those APIs and preserve their contracts; upgrading Dekaf alone does not enable parallel receive in Relayer.

---

## 8. Observability

Relayer's traces and metrics follow the [OpenTelemetry messaging semantic conventions](https://opentelemetry.io/docs/specs/semconv/messaging/) (decisions D15 and D25). Backends, dashboards and samplers that understand those conventions work with Relayer without custom mapping.

### 8.1 Sources and conventions version

- One `ActivitySource` named `Relayer` and one `Meter` named `Relayer`. Both carry the telemetry schema URL of the semantic conventions version that Relayer implements. That version is pinned in one place in the code, and a release that changes it says so in its notes.
- The messaging conventions are still in Development status upstream. Relayer has no older telemetry shape to preserve, so it emits only the current conventions. When the messaging conventions become stable, Relayer migrates through the standard `OTEL_SEMCONV_STABILITY_OPT_IN` values (`messaging` and `messaging/dup`).
- A failed operation sets `error.type` and the span status `Error`. The exception itself is logged (§8.6) with the trace and span IDs, not recorded as a span event.

### 8.2 Spans

Span names are `{messaging.operation.name} {destination}`, for example `send orders.placed`, `process orders.placed` or `complete billing-charge-card`. `{destination}` is the endpoint's declared topic, queue or exchange, which is low-cardinality because topology is declared (§9). Temporary or anonymous destinations, such as reply queues, are left out of the name unless a template is available. Span names and the endpoint's constant tags are computed once per endpoint at startup.

| Operation | Span name | Kind | `messaging.operation.type` | Parent | Links |
|---|---|---|---|---|---|
| Direct send of one message | `send {d}` | `PRODUCER` | `send` | Ambient | None |
| Direct batch send | `create {d}` per message, then `send {d}` | `PRODUCER`, then `CLIENT` | `create`, then `send` | Ambient | The send span links to each create span |
| Outbox enqueue | `create {d}` | `PRODUCER` | `create` | Ambient (for example the HTTP request) | None |
| Outbox dispatch | `send {d}` | `CLIENT` | `send` | The dispatcher's ambient span, usually none | Each dispatched message's creation context |
| Handler for one message | `process {d}` | `CONSUMER` | `process` | See below | The message's creation context |
| Batch handler | `process {d}` | `CONSUMER` | `process` | Ambient | One link per message's creation context |
| Settlement | The native operation name, for example `complete {d}`, `ack {d}` or `commit {d}` | `CLIENT` | `settle` | The process span; ambient for an amortized Kafka commit | None |

**Creation context.** Every message carries exactly one creation context, written as W3C `traceparent` and `tracestate` in native headers (§4.2).

- The creation context is the `create` span's context when there is one, and otherwise the producer `send` span's context.
- With the outbox, the `create` span runs at enqueue, inside the caller's transaction and trace. Its context is stored in the row's headers (§6.3). Dispatch, possibly minutes later on another instance, does not change it: the dispatch `send` span only links to it. A consumer therefore correlates with the request that enqueued the message, not with the dispatcher.
- Sends that the core makes on a message's behalf (a scheduled re-send for delayed redelivery, or a dead-letter copy, D24) keep the original creation context. Their `send` span is a `CLIENT` span under the process span, with a link to that context.
- A message that already has a creation context, for example from a non-Relayer producer or in raw mode, is not modified.
- A transport adapter makes sure that the native client's own instrumentation does not overwrite the creation context that Relayer wrote. The conformance suite checks this.

**Process span parent.** A process span always links to the creation context of each message it handles. For a single message, its parent is chosen as follows:

- If a span is already current when the transport hands the message to the pipeline, for example the native client's own consumer span, the process span is its child. This is the conventions' default.
- Otherwise, the creation context is the parent, so the producer and the consumer appear in one trace. The conventions allow this for single messages. `relayer.Telemetry(t => t.CreationContextAsParent(false))` makes each process span a new root instead.
- A batch process span never uses a creation context as its parent, because a span can have only one parent.

**No receive spans.** To application code, Relayer is push-based: messages reach handlers, not a pull call that the user makes. Relayer therefore does not create `receive` spans for its internal polling, and it creates no span for a prefetched message until the pipeline starts processing it. When the user enables the native client's instrumentation (Dekaf, the Azure SDK or the AWS SDK), that client's receive and transport spans appear in the trace. Relayer does not duplicate them, and it does not depend on them.

**Deliveries and retries.** There is one process span per delivery. A native redelivery is a new delivery and gets a new process span, with the delivery count where the system defines an attribute for it. Immediate in-process retries from the failure pipeline stay inside the same process span and are counted by `relayer.retries`.

**Settlement.** Each native settlement operation gets one settle span, whether the core or the handler (D11) triggers it. The settle span is a child of the process span, so the process span and `messaging.process.duration` include settlement (NSB #3565). Kafka commits are amortized over many messages (D13): there is one `commit {topic}` span per commit call, outside any process span, with `messaging.batch.message_count` and no per-message links. Dead-lettering is a native settle span (`dead_letter {d}`) where the endpoint supports it, and otherwise a `send` span to the dead-letter endpoint followed by the settle span that completes the original.

**Native client spans.** When enabled, spans from the native client nest under Relayer's spans as transport-level detail. Relayer's spans are the messaging spans of record. Each transport's documentation names the native sources to enable.

### 8.3 Attributes

The attributes that samplers need are set when the span starts: `messaging.system`, `messaging.operation.name`, `messaging.operation.type`, `messaging.destination.name`, `messaging.destination.template`, `messaging.destination.partition.id`, `server.address` and `server.port`.

| Attribute | Set on | Value |
|---|---|---|
| `messaging.system` | Messaging spans and endpoint metrics | `kafka`, `servicebus`, `aws_sqs`, `aws.sns` or `rabbitmq`; `relayer.inmemory` for the in-memory transport |
| `messaging.operation.name` | Messaging spans and endpoint metrics | `send`, `create`, `process`, or the system's settlement name |
| `messaging.operation.type` | All spans | `create`, `send`, `process` or `settle` |
| `messaging.destination.name` | All spans | The endpoint's destination, in the system's format |
| `messaging.destination.template` | All spans | When the destination comes from a template |
| `messaging.destination.temporary`, `messaging.destination.anonymous` | All spans | Only when true |
| `messaging.destination.partition.id` | Single-message spans | Partition, when the system has one |
| `messaging.consumer.group.name` | Consumer spans | Kafka consumer group |
| `messaging.destination.subscription.name` | Consumer spans | Azure Service Bus subscription |
| `messaging.message.id` | Single-message spans | `ce_id` |
| `messaging.message.conversation_id` | Single-message spans | The correlation ID |
| `messaging.batch.message_count` | Batch spans only | Number of messages; never set on single-message spans |
| `messaging.client.id` | All spans | The native client's ID, when it has one |
| `messaging.message.body.size` | Single-message spans, opt-in | Serialized body size |
| `server.address`, `server.port` | All spans and metrics | Broker endpoint, when known |
| `error.type` | Failed spans and metrics only | The exception's full type name, a native error code, or `_OTHER` |
| `cloudevents.event_type` | Single-message spans | The stable message name (`ce_type`) |
| `cloudevents.event_source` | Single-message spans | `ce_source` |

Each transport adapter adds the attributes and destination format that its system's semantic conventions page defines:

- **Kafka:** `messaging.kafka.message.key` (not set when the key is null), `messaging.kafka.offset`, `messaging.kafka.message.tombstone` (only when true) and `messaging.kafka.cluster.id`. Settlement is `commit`.
- **Azure Service Bus:** `messaging.servicebus.message.delivery_count` (when greater than 0), `messaging.servicebus.message.enqueued_time`, and `messaging.servicebus.disposition_status` on settle spans. Operation names are `send` or `schedule`, `process`, and `complete`, `abandon`, `defer` or `dead_letter`.
- **RabbitMQ:** the destination is `{exchange}:{routing key}` on send spans and `{exchange}:{routing key}:{queue}` on process spans. Also `messaging.rabbitmq.destination.routing_key` (when not empty) and `messaging.rabbitmq.message.delivery_tag`. Settlement is `ack` or `nack`.
- **Amazon SQS and SNS:** `aws.sqs.queue.url` for SQS, `aws.sns.topic.arn` for SNS, and `aws.request_id`. SQS settlement is `delete`.

Attributes that only Relayer defines use the `relayer.` prefix and never the `messaging.` prefix. Enrichers (`IActivityEnricher`) can add tags or rename spans without forking (MassTransit #4383). They run only when the span is recorded.

### 8.4 Metrics

See [metrics and logging integration](https://relayer.tomlonghurst.dev/docs/guides/telemetry) for the implemented transport-author call site and recording contracts.

Instruments from the semantic conventions, with the conventions' recommended histogram buckets given as instrument advice:

| Instrument | Type | Unit | Measures |
|---|---|---|---|
| `messaging.client.operation.duration` | Histogram | `s` | Each native send, create and settle operation, through native completion. A logical batch span can cover several native sends. |
| `messaging.process.duration` | Histogram | `s` | Each process operation, including settlement. |
| `messaging.client.sent.messages` | Counter | `{message}` | Messages that Relayer attempted to send to the broker. An outbox enqueue is not counted; its dispatch is. |
| `messaging.client.consumed.messages` | Counter | `{message}` | Messages delivered to the pipeline, counted once per delivery. |

These instruments carry `messaging.system` and `messaging.operation.name`, plus `messaging.destination.name`, `messaging.consumer.group.name`, `messaging.destination.subscription.name`, `server.address`, `server.port` and `error.type` where they apply. Metrics never carry high-cardinality values such as message IDs or keys.

Relayer-specific instruments, for which the conventions define nothing:

- `relayer.critical_time` (histogram, `s`): processing end minus `ce_time`.
- `relayer.retries` (counter, `{retry}`)
- `relayer.dead_lettered` (counter, `{message}`)
- `relayer.outbox.pending` (up-down counter, `{message}`): instance-observed durable row-count deltas, aggregated across stores and transports without endpoint dimensions. Commit adds rows; successful fenced deletion subtracts rows. This is not a database snapshot and can be negative when recovering another instance's rows.
- `relayer.outbox.dispatch_lag` (histogram, `s`): native dispatch attempt start minus original enqueue time, aggregated without endpoint dimensions. Includes failed native attempts.
- `relayer.inbox.duplicates` (counter, `{message}`)
- `relayer.consumer.lag` (gauge, `{message}`, Kafka): taken from Dekaf where Dekaf already exposes it.
- `relayer.circuit.state` (gauge)

### 8.5 Cost

- With no listener on the `Relayer` source, starting a span returns `null` after one `HasListeners` check, and no tags are computed. With no listener on an instrument, recording is a no-op. The zero-allocation budget (§12) is measured in this state.
- When tracing is on, `System.Diagnostics` allocates the activity and its tags. Constant tags come from the endpoint's precomputed set. Per-message values that need formatting, such as a non-string Kafka key, are formatted only when the span is recorded. The benchmark suite measures this cost separately.
- Samplers receive the constant attributes, partition ID and creation links at span start. Per-operation scratch storage is reused for the partition and links, then cleared after `StartActivity` consumes them; borrowed batch buffers are never retained. Other message attributes are added only when all activity data is requested. A sampler that drops the span pays no steady-state Relayer preparation allocation. `System.Diagnostics` can still allocate a root trace ID, and its `ActivityContext` stores trace and span IDs as strings; UTF-8 traceparent decoding does not create an additional whole-header string.
- Metric tags use `TagList` with the endpoint's precomputed values, so recording does not allocate.
- Batch sends create one `create` span per message. `relayer.Telemetry(t => t.CreateSpansForBatches(false))` turns them off, as the conventions recommend offering; the batch `send` span then becomes the `PRODUCER` span and its context is the creation context of every message in the batch.

### 8.6 Logging and health

- Logging uses `LoggerMessage` source generation and scopes that carry the message ID, type, and correlation ID around generated failure logs. Successful deliveries open no logging scope, preserving zero incremental allocations with Error logging enabled. Failure scopes hold read-only identity snapshots that providers can retain after processing.
- Health checks: transport connectivity, consumer liveness (each consumer has made progress or is idle), and outbox backlog thresholds.
  The optional `Relayer.HealthChecks` package registers all three with `services.AddRelayerHealthChecks()`.
  [Health integration](https://relayer.tomlonghurst.dev/docs/guides/health-checks) documents explicit native adapters, unavailable state and validated thresholds.

---

## 9. Topology

- Topology is **declared, not inferred.** Routes and consumers form a topology model you can inspect at runtime (`ITopology`). See [Topology](https://relayer.tomlonghurst.dev/docs/guides/topology) for startup modes, the adapter contract, JSON export and AsyncAPI 3.0.0 export of registered JSON contracts. Protocol bindings and custom wire-format schemas are not exported.
- Mode is chosen per transport:
  - `Provision` creates missing entities (development).
  - `Validate` checks that entities exist and fails fast with a diff (**default**). It needs only least-privilege permissions (MassTransit #5613, EasyNetQ #1000).
  - `None` skips checks entirely.
- Export: JSON, Service Bus Bicep/Terraform and SQS/SNS Terraform are implemented through their existing APIs and the managed `dotnet relayer topology export` command. Infrastructure export reads native declarations without resolving adapters or contacting brokers. D68 defines the explicit application factory contract; see [topology export](https://relayer.tomlonghurst.dev/docs/guides/topology#command-line-export). Service Bus targets an existing namespace using ARM resource schemas (Terraform uses AzAPI); AWS emits Terraform JSON. Unsupported native declarations fail before output.
- Entity names come from a naming convention you can override per endpoint. Nothing is auto-created with surprising names.

---

## 10. Resilience of the connection itself

- Transports must recover after a broker restart: they reconnect, redeclare topology when in `Provision` mode, and resume consumers. This is covered by integration tests that kill the broker container mid-stream (MassTransit #6253, Wolverine #937, EasyNetQ #560).
- Every send has a timeout, and so does every request/response (MassTransit #3502, #6229).
- Credential refresh is delegated to the native client's token providers (Azure `TokenCredential`, the AWS credential chain, RabbitMQ `ICredentialsProvider`) (MassTransit #2940).
- No consumer blocks a thread-pool thread. Everything is async, with bounded channels and backpressure (CAP #1821).

---

## 11. Testing

`Relayer.Testing` provides:

- **In-memory transport** with configurable capabilities. It can pretend to be Kafka-like (partitions) or queue-like, so tests exercise the same semantics.
- **`RelayerHarness`:**
  - Records sent (including sends routed to publish/subscribe destinations), dispatched, consumed, failed, and dead-lettered messages through the single send API.
  - Observes messages at both the logical send and the outbox dispatch, so outbox-enabled code is fully visible (MassTransit #4874).
  - `await harness.WaitForIdleAsync()` waits deterministically until in-flight work reaches zero. There are no sleeps and no race conditions (MassTransit #4835).
  - `harness.Reset()` clears state between tests (MassTransit #4908).
  - Multiple buses and transports are supported (MassTransit #4718).
- **Unit testing handlers:** handlers are POCOs. `TestMessageContext` builds a context with arbitrary features, including fake native features.
- The implemented harness, its idle boundary and standalone contexts are documented in [Testing](https://relayer.tomlonghurst.dev/docs/guides/testing).
- `ISender` and the batch APIs are interfaces with no static state, so they are easy to mock (MassTransit #3119).
- No process-wide statics: the full test suite runs in parallel within one process (Wolverine #3521).
- Testcontainers-based fixtures for every transport, published in `Relayer.Testing.Containers`.

---

## 12. Performance

Goals, measured against the raw native client in the same benchmark:

- **≤ 5% throughput overhead** on the receive path and the direct send path, with no outbox, using the default JSON serializer.
- **Zero allocations** from Relayer's own pipeline per message in steady state. Allocations by the serializer and the native client are excluded. The budget is measured with no telemetry listener attached; the cost with tracing on is measured separately (§8.5).
- **Startup:** < 50 ms to build the pipeline for 100 handlers. Startup works under Native AOT.

Techniques:

- The middleware pipeline is compiled into a delegate chain once per consumer at startup. Nothing is resolved per message except the DI scope, which singleton-only consumers open on demand (D53).
- `ValueTask` everywhere. Context objects are pooled. Headers are read lazily from the native message and only materialized when accessed.
- Single-message dispatch with an explicit failure delegate keeps continuation storage with the rented context and native receive state. Cross-worker completion does not depend on runtime async state-machine cache hits. Fresh scopes, async disposal and settlement ordering remain unchanged; batch and executable-retry paths retain their existing implementations.
- `IBufferWriter<byte>` / `ReadOnlySequence<byte>` serialization with pooled buffers.
- Source-generated dispatch per message type: no `Type` lookups, no `MethodInfo.Invoke`, no `dynamic`.
- Outbox writes are batched per transaction (one insert of N rows). Dispatch uses native batch send APIs.
- Benchmarks (BenchmarkDotNet) run in CI on every push to `main` and on pull requests that opt in; a regression beyond a threshold fails that advisory run.

The [performance regression gate](https://relayer.tomlonghurst.dev/docs/contributing/performance)
documents the pinned-main comparison, thresholds, allocation attribution and
raw-client benchmark pattern. The in-memory gate does not establish real-broker
overhead compliance. Fresh receive DI scope costs remain in the reported totals;
zero incremental composition allocations do not establish zero total pipeline allocations.
CI brackets the candidate with two fixed baseline executions to expose runner
drift. A throughput regression must exceed 5% against both. An allocation
regression on the per-message path must exceed both baselines by more than
0.5 B/op; an increase against only the lower baseline is inconclusive because
unchanged asynchronous executions can allocate different totals. Inconclusive
results remain visible and do not prove unchanged performance. CI hashes actual
comparison inputs after the common harness/SDK overlay. Byte-identical inventories
make throughput and allocation differences inconclusive execution variation, even
above both baselines; changed or unverified inputs retain the strict thresholds.
Separate processes can produce separated timing confidence intervals for unchanged
code; those intervals do not identify a source-change regression. The site documents
the inventory and excluded tooling/documentation. Input validation remains active.
Candidate startup stays under 50 ms. Startup allocation totals
are informational: randomized
hashes can change frozen dispatch-table layouts across unchanged processes.

---

## 13. Package layout

| Package | Contents | Dependencies |
|---|---|---|
| `Relayer` | Core abstractions, pipeline, routing, failure handling, telemetry, in-memory transport, analyzers and generators | `Microsoft.Extensions.*` abstractions only |
| `Relayer.Kafka` | Kafka transport | `Dekaf` |
| `Relayer.AzureServiceBus` | ASB transport | `Azure.Messaging.ServiceBus` |
| `Relayer.AmazonSqs` | SQS and SNS transport | `AWSSDK.SQS`, `AWSSDK.SimpleNotificationService` (v4) |
| `Relayer.RabbitMQ` | RabbitMQ transport | `RabbitMQ.Client` (7.x) |
| `Relayer.RabbitMQ.Streams` | RabbitMQ Streams connection/topology foundation; message execution planned | `RabbitMQ.Stream.Client` (1.x) |
| `Relayer.Nats` | NATS JetStream execution implemented; hosted performance comparison accepted (#468) | `NATS.Client.JetStream` (3.x) |
| `Relayer.GooglePubSub` | Native publishing, streaming pull, durable failure copies and topology | `Google.Cloud.PubSub.V1` (3.x), generated Protobuf messages |
| `Relayer.Sagas` / `.EntityFrameworkCore` | Saga state transitions and optional EF Core persistence | `Relayer`; EF package adds `Microsoft.EntityFrameworkCore.Relational` |
| `Relayer.Outbox.EntityFrameworkCore` | EF Core outbox and inbox | `Microsoft.EntityFrameworkCore.Relational` |
| `Relayer.Outbox.PostgreSql` / `.SqlServer` / `.MySql` | ADO.NET outbox and inbox (`NpgsqlDataSource` supported) | The provider's driver |
| `Relayer.Outbox.MongoDB` | MongoDB outbox and inbox | `MongoDB.Driver` |
| `Relayer.Serialization.*` | Protobuf, Avro, MessagePack | The respective library |
| `Relayer.Testing` / `.Testing.Containers` | Harness and fixtures | `Testcontainers` |
| `Relayer.Aspire` | OpenTelemetry subscription (D61); connections use extensions in the existing transport packages, native AppHost resources and existing health adapters | Relayer core, `OpenTelemetry.Extensions.Hosting` |
| `Relayer.AzureServiceBus.HealthChecks` | Optional Service Bus native probes and receiver-owned liveness | `Relayer.AzureServiceBus`, `Relayer.HealthChecks` |
| `Relayer.HealthChecks` | Connectivity, consumer liveness and outbox backlog health integration | `Microsoft.Extensions.Diagnostics.HealthChecks` |
| `Relayer.Kafka.HealthChecks` | Optional native Kafka connectivity and receiver liveness adapters | `Relayer.Kafka`, `Relayer.HealthChecks` |
| Later | `Relayer.Pulsar` | — |

Transports target the latest major version of their native client. Updating clients is part of the release cadence (Rebus #1204, Rebus.RabbitMq #137, CAP #1573).

---

## 14. Beyond v1

- **Sagas / process managers** (`Relayer.Sagas`): explicit state classes, optimistic concurrency (no hand-written lock providers), built on the inbox and outbox for idempotency, timeouts through the scheduler, and diagram export (Rebus #341, #1136, #1205; SlimMessageBus #384). [Diagram declarations and export](https://relayer.tomlonghurst.dev/docs/guides/saga-diagrams) and [transactional state transitions](https://relayer.tomlonghurst.dev/docs/guides/sagas) are implemented. The optional `Relayer.Sagas.EntityFrameworkCore` integration uses the existing PostgreSQL EF Core consumer boundary, native `xmin` concurrency and generated handler dispatch. These are post-v1 extensions under D20. [Durable process timeouts](https://relayer.tomlonghurst.dev/docs/guides/saga-timeouts) compose typed handlers, explicit wait correlation and existing transactional scheduled sends with the PostgreSQL due-row dispatcher.
- **Request/response** is implemented for explicit endpoints with mandatory timeouts, bounded process-local correlation, and `replyto`/`correlationid` CloudEvents extensions (D41). It reuses the send and consume pipelines; arbitrary reply destinations and temporary queues remain unsupported. See the [request/response guide](https://relayer.tomlonghurst.dev/docs/guides/request-response).
- **Google Pub/Sub:** [Native publishing, streaming pull and topology](../website/docs/transports/google-pubsub.md) use prepared native API clients (D57), native flow control and core durable dead-letter copies. The generated JSON path has a Native AOT emulator round trip.
- **Additional transports:** NATS JetStream message execution and connection/topology management are implemented; the hosted performance comparison was accepted in #468 within its documented measurement limits, and byte-backed headers are an upstream optimization under D52 ([status](../website/docs/transports/nats.md)). RabbitMQ Streams connection/topology management is implemented, while execution awaits a released client with the merged native `SubscriptionListener` recovery API ([status](../website/docs/transports/rabbitmq-streams.md)). Pulsar remains planned pending a verified Native AOT byte-message path ([status](../website/docs/transports/pulsar.md)). The Azure Service Bus emulator profile remains planned.
- **Outbox dispatch from CDC** (§6.4): PostgreSQL unordered capture is implemented; SQL Server remains planned.

---

## 15. Roadmap

| Phase | Scope | Exit criteria |
|---|---|---|
| 0 | Core abstractions, pipeline, source generator and analyzers, in-memory transport, harness, telemetry | Handler round trip under Native AOT; benchmarks in CI |
| 1 | Kafka and ASB transports; failure pipeline; DLQ and redrive | Broker-restart and rebalance chaos tests pass |
| 2 | EF Core and Postgres outbox and inbox (lanes, direct dispatch, cleanup) | Crash and duplicate tests with multiple instances pass; ≤ 5% overhead |
| 3 | SQS/SNS and RabbitMQ; SQL Server and MySQL outbox; topology export; multi-tenancy | Every transport passes the conformance suite |
| 4 | Sagas, request/response, CDC dispatch, more transports | — |

A **transport conformance test suite** (send, receive, settle, retry, DLQ, delay, ordering, trace propagation, reconnect) is part of the core. Every transport, including third-party ones, must pass it.

---

## 16. Traceability to known pain points

| Pain point in existing libraries | Design answer |
|---|---|
| No AOT, reflection, runtime code generation, slow cold start (MassTransit #4772, Wolverine #1577, Rebus #1095) | §1.5, §3.3, §4.3, §12 |
| Assembly scanning surprises (NSB #5595, #6967; Wolverine #1444, #3521) | §3.3 |
| Outbox duplicates, loss, misuse (CAP #1429, #1675; Wolverine #565; MassTransit #6238; Rebus.PostgreSql #55) | §6.1–6.4 |
| Outbox poison loops, no DLQ (MassTransit #6231, SlimMessageBus #284) | §6.4 poison rows |
| Expensive polling, missing indexes, per-replica cleanup (CAP #1599, #1811) | §6.3, §6.4 |
| Rigid outbox: one DbContext, overriding SaveChanges, no tenants (Silverback #163/#164, CAP #699) | §6.2, §6.6 |
| OTel missing or bolted on (EasyNetQ #1609, SlimMessageBus #149, Rebus #1163, KafkaFlow #465) | §8 |
| Kafka treated as a queue; rebalance, offset, and ordering bugs (Wolverine #3134, Brighter #2269, KafkaFlow #594/#681/#516) | §7 |
| Unclear retry and DLQ behaviour; no redrive (Brighter #3667/#1519, Rebus #1154, Wolverine #469) | §5 |
| Poison deserialization stalls consumers (Silverback #249, SlimMessageBus #438) | §5 classification |
| Consumers dead after a broker restart (MassTransit #6253, Wolverine #937, EasyNetQ #560) | §10 |
| Topology opacity and over-privilege (MassTransit #2799/#5613, NSB #7370) | §9 |
| Weak test harness (MassTransit #4874/#4908/#4718/#4835) | §11 |
| Scoped DI in handlers (Brighter #1560, Wolverine #1610) | §3.2 |
| Shared contract assemblies, type renames (Rebus #852/#905, NSB #6490) | §4.1, §4.4 |
| No CloudEvents interop (Wolverine #1592) | §4.2 |
| Several types per topic (confluent #1183) | §4.2 |
| Batch consumption declined (CAP #1553, SlimMessageBus #186) | §3.2 batch handlers |
| Thread-blocking consumers (CAP #1821) | §10 |
| Licence cost (MassTransit v9, NServiceBus) | §1.9 |

---

## 17. Open questions

1. **Handler signature:** should Relayer also accept method-based handlers (`Handle(OrderPlaced m)` on any class) via the source generator, or keep `IHandler<T>` only? Recommendation: `IHandler<T>` only in v1, because it is more explicit.
2. **Settlement override:** should handlers be able to settle manually (`context.DeadLetter(reason)`, `context.Defer()`)? Recommendation: yes, through a transport-neutral `ISettlementFeature`, with native extras exposed through the transport features.
3. **Sagas in v1?** The current recommendation is no; ship a sound core first.
4. **Minimum native client versions:** latest major only, or latest and previous? Recommendation: latest only.
