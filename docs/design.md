# Relayer — Design

Status: draft v0.1 (2026-10-31)

Relayer is a modern .NET messaging library. It wraps existing messaging technologies (Kafka, Azure Service Bus, Amazon SQS/SNS, RabbitMQ, and more) and takes away the ceremony around them: resiliency, outbox/inbox, telemetry, trace propagation, serialization, topology, and testing.

It is **not** a new broker abstraction. It does not change how the underlying technology sends, receives, orders, or acknowledges messages. It runs a thin, fast pipeline around the native client and gives you full access to that client whenever you need it.

---

## 1. Principles

1. **Wrap, don't replace.** Every transport uses a native .NET client for its technology: Dekaf for Kafka, and the official SDKs for Azure Service Bus, SQS/SNS, and RabbitMQ. Native settlement, ordering, partitioning, sessions, and dead-lettering are used as-is. Relayer never emulates a feature the broker already has.
2. **No lowest common denominator.** Portable features (retry, outbox, tracing) are uniform. Transport-specific features are exposed through typed, discoverable extensions rather than hidden behind a generic API.
3. **Explicit over magic.** No assembly scanning. No runtime code generation. Every handler, route, and piece of topology is declared in code or produced by a source generator you can read.
4. **Correct by default.** The safe behaviour is the default behaviour: at-least-once delivery, idempotent consumers via the inbox, poison messages go to a dead-letter queue (they never crash or stall a consumer), and settlement happens only after the work is durable.
5. **Native AOT and trimming from day one.** No reflection on the hot path. Serialization uses System.Text.Json source generation by default.
6. **Performance is a feature.** The pipeline is composed once at startup, the hot path allocates as little as possible, and CI gates on benchmarks measured against the raw client.
7. **Observable by default.** OpenTelemetry tracing and metrics, plus structured logging, are built into the core rather than shipped as an add-on.
8. **Few dependencies.** The core depends only on `Microsoft.Extensions.*` abstractions and the BCL. Each transport package adds only its native client.
9. **Permissive licence.** MIT, permanently.

### Non-goals

- Replacing or reimplementing broker features (Relayer does not emulate topics on SQS or partitions on RabbitMQ).
- Exactly-once delivery across arbitrary systems. Relayer provides at-least-once delivery plus idempotency. The one exception is Kafka-to-Kafka exactly-once, available via native Kafka transactions.
- Being an in-process mediator first. An in-memory transport exists for modular monoliths and tests, but it is just another transport.

### Target platform

- `net10.0` (current LTS) and later. .NET 8 support ends in November 2026, so it is not targeted.
- C# 14. Source generators are incremental generators.

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

Each transport declares the capabilities of each endpoint (decision D24). A feature that needs one either uses the native mechanism or a documented fallback, and when there is neither, startup fails with a clear error (no silent degradation). The core runs every fallback, so transports contain native code only (§2.4):

| Feature | Native when available | Fallback |
|---|---|---|
| Delayed delivery / scheduling | ASB `ScheduledEnqueueTime`, SQS `DelaySeconds` (≤ 15 min), RabbitMQ delayed-message plugin | Outbox `deliver_at` |
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

**3. Sending.** The core hands the transport a transport-neutral `OutgoingMessage`: destination, body bytes, named headers, ordering key, `DeliverAt`, and the native configurators from `SendOptions`. Its fields are exactly an outbox row (§6.3), so direct sends, outbox dispatch, scheduled re-sends and dead-letter copies all use the same two methods:

```csharp
ValueTask SendAsync(OutgoingMessage message, CancellationToken cancellationToken);
ValueTask SendBatchAsync(ReadOnlyMemory<OutgoingMessage> messages, CancellationToken cancellationToken);
```

The transport builds its native message, applies `options.ApplyNative<TNative>(native)`, and sends. Native configurators are delegates and cannot be stored in the outbox, so a send that has them and is enlisted in the outbox fails immediately with an explanation instead of dropping them.

**4. Capabilities and topology.** `GetCapabilities(endpoint)` returns a `TransportCapabilities` record for one endpoint (D24). `ApplyTopologyAsync(mode, ct)` validates or provisions the endpoints the transport's builder declared and returns a report the core uses for the startup diff and for topology export.

**5. Builders.** The core ships generic builders, and the transport supplies its own option types:

```csharp
public sealed class RouteBuilder<TMessage, TRouteOptions>   { … OrderingKey(…) … }      // core
public sealed class ConsumerBuilder<TConsumerOptions>        { … Handler<T>(), Inbox(), Failures(…) … }  // core

public static ConsumerBuilder<KafkaConsumerOptions> Group(this ConsumerBuilder<KafkaConsumerOptions> consumer, string group);  // Kafka
```

Handlers, inbox, failure policy and ordering keys are implemented once in the core and work the same on every transport. A Kafka-only setting such as `Group` exists only on Kafka's builder, so it cannot be applied to a queue by mistake. `OrderingKey` accepts a `string` or any `IUtf8SpanFormattable` value such as `Guid` or `long`; the latter is formatted into a pooled buffer, so a Kafka key costs no allocation.

**Log-based transports.** Kafka, Event Hubs, Kinesis, Pulsar, RabbitMQ Streams and JetStream share one consumption model. The core ships it once as `PartitionScheduler<TIncoming, TBinding, TPartition, TPosition>`: key-ordered parallelism within a partition, commits at the contiguous-completed watermark, backpressure, drain on revoke, and pause tracking per partition (§7). The transport feeds records in and implements three calls:

```csharp
public interface IPartitionSource<TPartition, TPosition>
{
    void Commit(TPartition partition, TPosition position);   // last completed position; the transport batches and flushes
    void Pause(TPartition partition);
    void Resume(TPartition partition);
}
```

`TPosition` is whatever the broker uses: a `long` offset for Kafka, a sequence-number string for Kinesis, a `MessageId` for Pulsar. The scheduler never compares positions; it tracks them in arrival order.

Every transport proves the SPI contract by passing the transport conformance suite (§15). The in-memory transport is the first implementation, in phase 0.

---

## 3. Developer experience

### 3.1 Configuration

```csharp
var builder = WebApplication.CreateBuilder(args);

builder.Services.AddDbContext<AppDbContext>(...);

builder.Services.AddRelayer(relayer =>
{
    relayer.AddKafka("events", kafka =>
    {
        kafka.BootstrapServers(builder.Configuration["Kafka:Servers"]);
        kafka.ConfigureProducer(p => p.WithAcks(Acks.All));                     // Dekaf's own builder

        kafka.Produce<OrderPlaced>("orders.placed", p => p.OrderingKey(m => m.OrderId));

        kafka.Consume("orders.placed", c => c
            .Group("billing")
            .Handler<OrderPlacedHandler>()
            .Concurrency(perPartition: 8, orderBy: OrderBy.Key));     // key-ordered parallelism
    });

    relayer.AddAzureServiceBus("commands", asb =>
    {
        asb.Client(sp => sp.GetRequiredService<ServiceBusClient>()); // reuse your own client
        asb.Send<ChargeCard>("billing-charge-card");
        asb.Consume("billing-charge-card", c => c.Handler<ChargeCardHandler>());
    });

    relayer.Outbox(o => o.UseEntityFrameworkCore<AppDbContext>());
    relayer.Inbox(i => i.UseEntityFrameworkCore<AppDbContext>());

    relayer.Failures(f => f
        .Retry(immediate: 2)
        .Redeliver(TimeSpan.FromSeconds(10), TimeSpan.FromMinutes(1), TimeSpan.FromMinutes(10))
        .WithJitter()
        .On<ValidationException>(FailureAction.DeadLetter));

    relayer.Topology(TopologyMode.Validate);
});
```

Configuration also binds from `IConfiguration` and uses the Options pattern (`IOptionsMonitor<KafkaTransportOptions>` and so on), so the same settings can live in `appsettings.json`.

### 3.2 Handlers

Handlers are plain classes that are resolved from a fresh DI scope for every message (or batch). They can inject scoped services such as a `DbContext`.

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
- For people who want convenience, the source generator emits an explicit, readable `AddHandlersFromThisAssembly()` that lists every handler it found at compile time. This is "scanning" done by the compiler — it is visible, debuggable, and AOT-safe.
- Analyzers report problems at compile time: a handler that is registered nowhere, a message type with no stable name, a consumer with no handler, or a feature the transport does not support (for example, ordering on a non-FIFO SQS queue).

Register handlers inside `AddRelayer`. Both forms use the same generated descriptors; explicit registration adds only the requested handler:

```csharp
services.AddRelayer(relayer => relayer.Handler<OrderPlacedHandler>());
// Or register every concrete, assembly-accessible handler declared in this assembly:
services.AddRelayer(relayer => relayer.AddHandlersFromThisAssembly());
```

Discovery includes inherited contracts, explicit interface implementations, and handlers with multiple message or batch contracts. Open generic, abstract, private nested, and file-local classes are excluded. Message types must be accessible to the generated code; `RLYH001` reports inaccessible contracts. Explicit `Handler<ClosedHandler<OrderPlaced>>()` also supports closed generic handlers and handlers in referenced assemblies. Repeated registration is idempotent. Handlers default to scoped DI registration, preserving an existing registration.

The core's `HandlerRegistry` freezes after configuration. Pipeline composition selects a `HandlerDescriptor<TMessage>` or `BatchHandlerDescriptor<TMessage>` once. For each message scope, its generated `Resolve` method uses typed DI resolution; `DispatchAsync` then calls the handler through its exact interface and returns its `ValueTask` unchanged. Dispatch does no type lookup, delegate creation, or allocation. Registry metadata uses CLR types only during configuration, never as wire identities.

### 3.4 Sending from anywhere

`ISender` is scoped. When an outbox is configured and a transaction is active in the current scope, sends are enlisted automatically. Otherwise they go straight to the transport. To be explicit, use `sender.SendNowAsync` (bypass the outbox) or `sender.SendViaOutboxAsync` (require the outbox; throws when there is no transaction).

Publish versus send is a routing concern, not a separate API. Whether a destination fans out depends on what the endpoint is natively (a topic or exchange versus a queue).

---

## 4. Message contracts and wire format

### 4.1 Stable message type names

Messages are identified by a stable string name, never by the CLR type name. That way, renaming or moving a class never breaks consumers, and services do not need to share a contracts assembly.

```csharp
[Message("billing.charge-card", Version = 2)]
public sealed record ChargeCard(Guid OrderId, decimal Amount);
```

If the attribute is missing, an analyzer warns and suggests one. A fallback convention exists but is discouraged.

### 4.2 CloudEvents by default

Relayer writes CloudEvents attributes using each transport's binary-mode binding, as native headers or properties: `ce_id` (UUIDv7), `ce_type`, `ce_source`, `ce_time`, `ce_specversion`, `traceparent`, and `tracestate`. As a result:

- Non-Relayer applications in any language can produce and consume the same messages (Wolverine #1592, CloudEvents requests).
- Consumers route by `ce_type`, so **several message types can share one topic or queue** (confluent#1183).
- A raw mode (no envelope, just the native payload) exists for integrating with legacy producers. In raw mode you map the type yourself, for example from the topic name.

### 4.3 Serialization

- The default is System.Text.Json with a source-generated `JsonSerializerContext`. The generator builds it from the declared message types, so serialization is AOT-safe.
- Serialization is pluggable (`IMessageSerializer`): Protobuf, Avro (with schema registry), and MessagePack ship as optional packages.
- Serialization writes to `IBufferWriter<byte>` and reads from `ReadOnlySequence<byte>` where the native client allows it.

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
- **Error metadata** goes on the dead-lettered message: the original message ID, exception type, message and stack trace for **every** attempt (Rebus #1154), attempt count, first and last failure time, consumer name, and host.
- **Redrive API:** `IDeadLetterManager` provides `List`, `Peek`, `Redrive(filter)`, and `Purge`. It is backed natively by the ASB DLQ, SQS redrive, RabbitMQ DLX queue, or Kafka DLT topic. It is also exposed as an optional minimal-API endpoint group and as CLI commands.
- **Circuit breaker:** when the failure rate of a consumer crosses a threshold, Relayer **pauses the consumer natively** (Kafka `Pause`, ASB processor stop, RabbitMQ consumer cancel) instead of burning messages into the DLQ (SlimMessageBus #454). It then half-opens with a probe message.
- **Retries never duplicate side effects.** Outbox rows written by a failed attempt are rolled back with the transaction. Without a transaction, sends made during a failed attempt are buffered and discarded (Rebus #1198, Rebus.PostgreSql #53).
- Relayer implements this pipeline itself, because it has to integrate with native settlement and delayed redelivery, which Polly cannot model. Inside a handler, you remain free to use Polly or `Microsoft.Extensions.Resilience` for downstream calls.

---

## 6. Outbox and inbox

The outbox is the area where every existing library has had correctness bugs, so it gets the most design attention.

### 6.1 Guarantees

- A message is written in the **same database transaction** as your business data. It is dispatched if and only if that transaction commits.
- Delivery is at-least-once. The message ID (UUIDv7) is stable across retries and dispatches, so the consumer-side inbox deduplicates.
- Ordering is preserved per ordering key when the route declares it (Kafka key, ASB session, SQS FIFO message group).
- A poison outbox row never blocks unrelated messages.
- The design is safe with any number of instances. There is no double dispatch under normal operation, no cleanup race between instances, and no per-replica background-job storms.

### 6.2 Enlisting: hard to misuse

- **EF Core:** Relayer registers a `SaveChangesInterceptor` and a `DbTransactionInterceptor`. Messages sent in the scope are added to the change set, so `SaveChangesAsync`, or a transaction you started yourself, writes them atomically. You do not need to override `SaveChanges` (Silverback #164), and it works with **multiple DbContexts** (Silverback #163). Committing the raw transaction is fine, because Relayer observes the commit through the interceptor (CAP #1675).
- **ADO.NET / Dapper:** `await using var tx = await outbox.BeginTransactionAsync(connection)` returns a real `DbTransaction` subclass. Using it as normal is the correct usage, so there is no second "Relayer transaction" to forget.
- **MongoDB / DynamoDB:** use their session and transaction APIs in the same style.

### 6.3 Schema

```
relayer_outbox
  id             uuid (v7)       primary key
  lane           smallint        hash(ordering_key) % lanes, or null when unordered
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

index (lane, id) where status is pending        -- partial/filtered where the database supports it
index (deliver_at) where deliver_at is not null

relayer_leases
  name           text primary key  -- 'lane:7', 'cleanup', 'scheduler'
  owner          text
  expires_at     timestamptz
```

Migrations ship as EF Core migrations and as plain SQL scripts for each database (`dotnet relayer db script --provider postgres`). Indexes are created by default (CAP #1599). Tables can be prefixed and given a schema (NSB.Persistence.Sql #2093).

### 6.4 Dispatching

There are two paths, each chosen per message based on whether its route is ordered:

1. **Unordered messages — direct dispatch.** After commit, the instance that committed dispatches immediately, in the background, without waiting for a poll. It claims the row with a conditional update (`UPDATE … SET claimed_by = @me WHERE id = @id AND claimed_by IS NULL`), so another instance never sends it twice. Latency equals the broker round trip (fixes the latency of polling designs, SlimMessageBus #275).
2. **Ordered messages — lane leases.** Rows are hashed into N lanes (default 32). Instances lease lanes through `relayer_leases` with heartbeats and fair-share rebalancing as instances join and leave. A lane owner reads its lane in `id` order with a simple indexed range scan. There are no `SKIP LOCKED` scans across the whole table, and ordering per key is guaranteed.

A **sweeper** (also leased) picks up anything that was committed but not dispatched within a threshold, for example because the instance crashed. Postgres `LISTEN/NOTIFY` and SQL Server Service Broker can optionally replace the sweeper's poll.

After dispatch, rows are deleted in batches. One leased **cleanup** job handles retention for both the outbox and the inbox (CAP #1811, Rebus.SqlServer #101).

**Poison rows:** after a configurable number of attempts, the row is moved to `relayer_outbox_dead` and an alert metric is raised. For ordered routes, only later rows **with the same ordering key** are parked; the rest of the lane continues. Parked keys can be inspected and released through `IDeadLetterManager`.

**Change data capture** (phase 3): an optional dispatcher reads Postgres logical replication or SQL Server CDC instead of polling. The table layout is compatible with Debezium's outbox event router, so teams can use Debezium instead.

### 6.5 Inbox

- `relayer_inbox(consumer, message_id, processed_at)`, written in the same transaction as the handler's business changes and outbox rows.
- A duplicate delivery is acknowledged without invoking the handler, and a metric is recorded.
- The inbox is opt-in per consumer. It is enabled automatically when the outbox is used on the same DbContext.

### 6.6 Multi-tenancy

- `IOutboxStoreResolver` maps the current tenant to a connection or DbContext, supporting a database per tenant or a schema per tenant (CAP #699, Rebus #1218, Wolverine #556).
- `ITenantSource` enumerates tenants for the dispatcher. Each tenant store gets its own lanes and leases. Dispatchers share a bounded connection budget, which avoids per-tenant connection sprawl (Wolverine #3376).
- The tenant ID travels in a header and is restored into the consumer's scope.

---

## 7. Kafka done properly

Kafka gets its own consumption model instead of being forced into a queue shape (Wolverine #3134).

The transport wraps **[Dekaf](https://github.com/thomhurst/Dekaf)**, a pure C# Kafka client (decision D22). Dekaf has no native dependencies, supports Native AOT, and is built for zero-allocation hot paths, which matches Relayer's own goals. Dekaf already ships features such as retry and dead-letter topics, an outbox, and OpenTelemetry instrumentation. Following principle 1, the Kafka transport reuses Dekaf's mechanisms where they fit Relayer's uniform pipeline, rather than re-implementing them. Relayer's outbox remains the cross-transport outbox.

- **Key-ordered parallelism:** within a partition, messages with different keys are processed concurrently (`Concurrency(perPartition: n, orderBy: OrderBy.Key)`), while messages with the same key stay ordered. This, the watermark commits and the rebalance handling below live in the core's partition scheduler (§2.4), so other log-based transports reuse them.
- **Offset commits by watermark:** Relayer commits the highest *contiguous* completed offset per partition, asynchronously and batched, and flushes on rebalance and shutdown (Brighter #2269).
- **Rebalance safety:** `cooperative-sticky` is supported. On revoke, Relayer stops dispatching for the revoked partitions, drains in-flight work up to a timeout, then commits. Pause and resume are tracked per partition, so a rebalance while paused cannot deadlock (KafkaFlow #456, #681).
- **Consume-transform-produce:**
  - *Kafka → database → anything:* the inbox, business data, and outbox are written in one database transaction. The offset is committed only after that transaction commits. Redelivery after a crash is deduplicated by the inbox.
  - *Kafka → Kafka:* optional native Kafka transactions (Dekaf's `SendOffsetsToTransactionAsync`) give exactly-once processing.
- **Tombstones:** `sender.SendAsync(Tombstone.For<Customer>(key))` sends a record with a null value (KafkaFlow #516). A consumer receives it as `Tombstone<T>`.
- **Ordered batch produce:** batch produce preserves order per key (KafkaFlow #594).
- **Replay and seek:** `IKafkaConsumerControl.SeekAsync(topic, partition | timestamp)` for operational replay (Wolverine #3479).
- **Schema registry:** works with multiple clusters, each with its own registry (KafkaFlow #558).
- **Logging:** Dekaf logs through `ILoggerFactory` (`WithLoggerFactory`), so Kafka client logs land in the app's logging pipeline with no native log bridge.

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
| `messaging.system` | All spans and metrics | `kafka`, `servicebus`, `aws_sqs`, `aws.sns` or `rabbitmq`; `relayer.inmemory` for the in-memory transport |
| `messaging.operation.name` | All spans and metrics | `send`, `create`, `process`, or the system's settlement name |
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
- **Amazon SQS and SNS:** `aws.sqs.queue.url` and `aws.request_id`. SQS settlement is `delete`.

Attributes that only Relayer defines use the `relayer.` prefix and never the `messaging.` prefix. Enrichers (`IActivityEnricher`) can add tags or rename spans without forking (MassTransit #4383). They run only when the span is recorded.

### 8.4 Metrics

Instruments from the semantic conventions, with the conventions' recommended histogram buckets given as instrument advice:

| Instrument | Type | Unit | Measures |
|---|---|---|---|
| `messaging.client.operation.duration` | Histogram | `s` | Each send, create and settle operation. Equals the span's duration. |
| `messaging.process.duration` | Histogram | `s` | Each process operation, including settlement. |
| `messaging.client.sent.messages` | Counter | `{message}` | Messages that Relayer attempted to send to the broker. An outbox enqueue is not counted; its dispatch is. |
| `messaging.client.consumed.messages` | Counter | `{message}` | Messages delivered to the pipeline, counted once per delivery. |

These instruments carry `messaging.system` and `messaging.operation.name`, plus `messaging.destination.name`, `messaging.consumer.group.name`, `messaging.destination.subscription.name`, `server.address`, `server.port` and `error.type` where they apply. Metrics never carry high-cardinality values such as message IDs or keys.

Relayer-specific instruments, for which the conventions define nothing:

- `relayer.critical_time` (histogram, `s`): processing end minus `ce_time`.
- `relayer.retries` (counter, `{retry}`)
- `relayer.dead_lettered` (counter, `{message}`)
- `relayer.outbox.pending` (up-down counter, `{message}`)
- `relayer.outbox.dispatch_lag` (histogram, `s`): dispatch time minus enqueue time.
- `relayer.inbox.duplicates` (counter, `{message}`)
- `relayer.consumer.lag` (gauge, `{message}`, Kafka): taken from Dekaf where Dekaf already exposes it.
- `relayer.circuit.state` (gauge)

### 8.5 Cost

- With no listener on the `Relayer` source, starting a span returns `null` after one `HasListeners` check, and no tags are computed. With no listener on an instrument, recording is a no-op. The zero-allocation budget (§12) is measured in this state.
- When tracing is on, `System.Diagnostics` allocates the activity and its tags. Constant tags come from the endpoint's precomputed set. Per-message values that need formatting, such as a non-string Kafka key, are formatted only when the span is recorded. The benchmark suite measures this cost separately.
- Metric tags use `TagList` with the endpoint's precomputed values, so recording does not allocate.
- Batch sends create one `create` span per message. `relayer.Telemetry(t => t.CreateSpansForBatches(false))` turns them off, as the conventions recommend offering; the batch `send` span then becomes the `PRODUCER` span and its context is the creation context of every message in the batch.

### 8.6 Logging and health

- Logging uses `LoggerMessage` source generation (zero allocations when disabled) and scopes that carry the message ID, type, and correlation ID.
- Health checks: transport connectivity, consumer liveness (each consumer has made progress or is idle), and outbox backlog thresholds.

---

## 9. Topology

- Topology is **declared, not inferred.** Routes and consumers form a topology model you can inspect at runtime (`ITopology`).
- Mode is chosen per transport:
  - `Provision` creates missing entities (development).
  - `Validate` checks that entities exist and fails fast with a diff (**default**). It needs only least-privilege permissions (MassTransit #5613, EasyNetQ #1000).
  - `None` skips checks entirely.
- Export: `dotnet relayer topology export --format json|bicep|terraform` emits the topology for infrastructure-as-code (NSB #7370, MassTransit #2799).
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
  - Records sent, published, consumed, failed, and dead-lettered messages.
  - Observes messages at both the logical send and the outbox dispatch, so outbox-enabled code is fully visible (MassTransit #4874).
  - `await harness.WaitForIdleAsync()` waits deterministically until in-flight work reaches zero. There are no sleeps and no race conditions (MassTransit #4835).
  - `harness.Reset()` clears state between tests (MassTransit #4908).
  - Multiple buses and transports are supported (MassTransit #4718).
- **Unit testing handlers:** handlers are POCOs. `TestMessageContext` builds a context with arbitrary features, including fake native features.
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

- The middleware pipeline is compiled into a delegate chain once per consumer at startup. Nothing is resolved per message except the DI scope.
- `ValueTask` everywhere. Context objects are pooled. Headers are read lazily from the native message and only materialized when accessed.
- `IBufferWriter<byte>` / `ReadOnlySequence<byte>` serialization with pooled buffers.
- Source-generated dispatch per message type: no `Type` lookups, no `MethodInfo.Invoke`, no `dynamic`.
- Outbox writes are batched per transaction (one insert of N rows). Dispatch uses native batch send APIs.
- Benchmarks (BenchmarkDotNet) run in CI, and a regression beyond a threshold fails the build.

---

## 13. Package layout

| Package | Contents | Dependencies |
|---|---|---|
| `Relayer` | Core abstractions, pipeline, routing, failure handling, telemetry, in-memory transport, analyzers and generators | `Microsoft.Extensions.*` abstractions only |
| `Relayer.Kafka` | Kafka transport | `Dekaf` |
| `Relayer.AzureServiceBus` | ASB transport | `Azure.Messaging.ServiceBus` |
| `Relayer.AmazonSqs` | SQS and SNS transport | `AWSSDK.SQS`, `AWSSDK.SimpleNotificationService` (v4) |
| `Relayer.RabbitMQ` | RabbitMQ transport | `RabbitMQ.Client` (7.x) |
| `Relayer.Outbox.EntityFrameworkCore` | EF Core outbox and inbox | `Microsoft.EntityFrameworkCore.Relational` |
| `Relayer.Outbox.PostgreSql` / `.SqlServer` / `.MySql` | ADO.NET outbox and inbox (`NpgsqlDataSource` supported) | The provider's driver |
| `Relayer.Outbox.MongoDB` | MongoDB outbox and inbox | `MongoDB.Driver` |
| `Relayer.Serialization.*` | Protobuf, Avro, MessagePack | The respective library |
| `Relayer.Testing` / `.Testing.Containers` | Harness and fixtures | `Testcontainers` |
| `Relayer.Aspire` | Aspire integration (resources and health) | `Aspire.Hosting` |
| Later | `Relayer.Nats`, `Relayer.GooglePubSub`, `Relayer.Pulsar`, `Relayer.RabbitMQ.Streams`, `Relayer.Sagas` | — |

Transports target the latest major version of their native client. Updating clients is part of the release cadence (Rebus #1204, Rebus.RabbitMq #137, CAP #1573).

---

## 14. Beyond v1

- **Sagas / process managers** (`Relayer.Sagas`): explicit state classes, optimistic concurrency (no hand-written lock providers), built on the inbox and outbox for idempotency, timeouts through the scheduler, and diagram export (Rebus #341, #1136, #1205; SlimMessageBus #384).
- **Request/response** with a mandatory timeout and the reply address carried in CloudEvents extensions.
- **Additional transports:** NATS JetStream, Google Pub/Sub, Pulsar, RabbitMQ Streams, and the Azure Service Bus emulator profile.
- **AsyncAPI document generation** from the topology model.
- **Outbox dispatch from CDC** (§6.4).

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
