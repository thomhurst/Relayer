---
title: RabbitMQ
description: "Native RabbitMQ consumers, confirmed publishing, optional health and explicit topology validation."
---

`Relayer.RabbitMQ` wraps RabbitMQ.Client 7. Named connection lifecycle and exchange, queue and queue-binding declarations are implemented. Confirmed outgoing sends, binary CloudEvents headers, send telemetry and verified delayed-exchange scheduling are implemented. Native consumers, durable failure handling, acknowledgement telemetry and optional connectivity/liveness adapters are implemented. The real-broker conformance suite covers restart, requeue, tracing, broker recovery, outage sends and graceful drain; absent native capabilities are explicitly skipped.

## Provision native topology

```csharp
using RabbitMQ.Client;
using Relayer;
using Relayer.RabbitMQ;

services.AddRelayer(relayer => relayer.RabbitMQ("orders", rabbit =>
{
    rabbit.TopologyMode = TopologyMode.Provision; // Explicit development/bootstrap operation.
    rabbit.Options.Configure(options =>
    {
        options.ConnectionFactory = new ConnectionFactory
        {
            HostName = "localhost",
            UserName = "application",
            Password = "application",
        };
        options.Exchanges.Add(new("orders") { Type = ExchangeType.Topic });
        options.Exchanges.Add(new("orders.dead"));
        options.Queues.Add(new("orders")
        {
            Arguments = new Dictionary<string, object?>
            {
                ["x-dead-letter-exchange"] = "orders.dead",
                ["x-dead-letter-routing-key"] = "failed",
            },
        });
        options.Queues.Add(new("orders.dead"));
        options.Bindings.Add(new("orders", "orders", "placed.*"));
        options.Bindings.Add(new("orders.dead", "orders.dead", "failed"));
    });
}));
```

Run the application host to apply topology during startup. Relayer declares exchanges first, queues second and bindings last. RabbitMQ checks declaration equivalence: an existing entity with incompatible properties produces an actionable startup difference. Relayer never deletes or changes existing entities. Provisioning can create some entities before another declaration fails; it is not transactional.

Unspecified exchange properties provision a durable, non-auto-delete direct exchange. Unspecified queue properties provision a durable, non-exclusive, non-auto-delete queue. Specify native arguments directly, including DLX and queue-type arguments. RabbitMQ owns routing and dead-lettering. Native DLX settlement cannot retain Relayer error metadata; the default policy uses a confirmed dead-letter copy before acknowledging the source.

Queues need explicit names. Server-generated queue names and binding the default exchange are not supported by these declarations. Routing keys may be empty. Exchange and queue names occupy separate namespaces.

## Validate existing entities

The default [topology mode](../guides/topology.md) is `Validate`. RabbitMQ.Client's passive AMQP declarations can verify **existence**, but cannot read exchange type, durability, exclusivity, auto-delete, arguments or bindings. For existence-only validation, declare just names:

```csharp
services.AddRelayer(relayer => relayer.RabbitMQ("orders", rabbit =>
{
    rabbit.Options.Configure(options =>
    {
        options.ConnectionFactory = nativeConnectionFactory;
        options.Exchanges.Add(new("orders"));
        options.Queues.Add(new("orders"));
    });
}));
```

Null declaration properties mean no property expectation in `Validate`; they select the defaults above in `Provision`. Explicit properties or arguments in `Validate` produce unresolved startup differences even when the entity exists. Every binding declaration also produces an unresolved difference: AMQP has no read-only binding inspection operation. Use infrastructure tooling to verify those settings and bindings, or explicitly run `Provision` with configure permissions for native equivalence checks and binding declarations. Provisioning uses RabbitMQ's own equivalence rules, including any broker-defined exceptions.

Validation does not publish probe messages, consume messages, bind queues or create missing entities. Missing entities close their individual native channels; checks continue on other channels so one missing entity does not hide another. `None` skips topology operations; hosted startup still opens the native connection.

Queue names can use up to 255 UTF-8 bytes. In `Validate` and `Provision`, the startup report names any oversized queue declaration and shows its byte count. Relayer skips that declaration and continues checking the remaining topology.

A passive declaration can renew an unused classic queue's expiration timer (`x-expires`). When RabbitMQ reads the queue's statistics for the declaration response, it renews the queue's lease. Use `None` if startup must leave queue expiration timers untouched and you manage topology outside Relayer. [RabbitMQ's passive declaration handler](https://github.com/rabbitmq/rabbitmq-server/blob/v4.1.4/deps/rabbit/src/rabbit_amqqueue_process.erl#L1425-L1428)

## Connection ownership and native configuration

Each named registration owns one connection, created lazily during topology application or hosted startup. Resolving options, the adapter or `ITopology` does not connect. Host shutdown closes the connection. Other connections remain independent. Each topology operation disposes its channel; RabbitMQ.Client retains its own connection-level topology recovery records.

`ConnectionFactory` is passed through unchanged. Configure TLS, endpoints, heartbeats, native automatic recovery, topology recovery and `ICredentialsProvider` on that factory. Relayer does not implement another credential-refresh or recovery loop. Configure options before startup; live connection reconfiguration is not implemented.

Consumers use their native queue name as the source. The consumer source and failure destination are registered internally for confirmed failure copies. Declare or provision those queues explicitly.

## Confirmed sends

Declare a route to a queue by name, or use `exchange:routing-key` to publish through an exchange. The first colon separates the exchange from the routing key; use an empty exchange (`:queue`) for the default exchange. An exchange name in this notation cannot contain a colon. Empty routing keys are supported.

For one message type on a named RabbitMQ transport, declare each exchange and routing key pair once. For example, `orders` and `:orders` both address the same default-exchange destination. Startup rejects a configuration that declares both, as does resolving a sender without a host.

```csharp
services.AddRelayer(relayer =>
{
    relayer.AddMessageTypesFromThisAssembly();
    relayer.RabbitMQ("orders", rabbit =>
    {
        rabbit.Options.Configure(options => options.ConnectionFactory = nativeConnectionFactory);
        rabbit.Send<OrderPlaced>("orders:placed.eu");
    });
});

await sender.SendAsync(new OrderPlaced(orderId), options => options.RabbitMQ(properties =>
{
    properties.Priority = 3;
    properties.AppId = "billing";
}));
```

Register the message with a stable `[Message]` identity and a source-generated JSON serializer as in the [sending guide](../guides/sending.md). Topology declarations remain independent: sending does not implicitly create queues or bindings.

A single send leases one native channel exclusively and holds it until RabbitMQ.Client completes confirmation tracking. `PublisherCount` defaults to 16. A concurrent sender waits for an available channel with its cancellation token. Relayer does not share a publishing channel between concurrent sends. A batch leases one channel, admits its publishes in input order without waiting for each confirmation, then awaits every admitted native operation before releasing the channel or the caller’s borrowed storage. Each batch slot directly retains its completion operation, so batch items do not scan the shared completion pool. Each in-flight message has separate mutable properties and header buffers, retained per channel at the batch size high-water mark. Batches can contain different declared destinations and immediate or native delayed messages. They can partially succeed and are not atomic: a return, nack, preparation failure or cancellation fails the batch after its admitted operations drain; cancellation does not retract messages already sent. Each native publish has its own send span and operation duration. Concurrent callers have RabbitMQ's native ordering semantics, with no ordering-by-key guarantee.

Immediate publishes use `mandatory: true` and persistent basic properties. RabbitMQ.Client propagates nacks, unroutable `basic.return` responses, protocol errors and connection failures. Confirmation is the broker's durability boundary, not proof of consumer processing. Queue/exchange durability and queue type still determine the native durability guarantees. Cancellation or a connection failure can leave delivery uncertain; Relayer does not retry a publish automatically. Native automatic recovery remains owned by RabbitMQ.Client. A channel closed by a protocol error is replaced before the next send.

Set native channel settings through `PublisherChannelOptions` (`CreateChannelOptions`), passed unchanged. Both confirmation flags must be enabled; startup rejects disabling either. `SendOptions.RabbitMQ` exposes borrowed native `BasicProperties`. Do not retain that pooled object after the callback/send. The portable headers, serializer content type, delivery time and persistent delivery mode take precedence over conflicting native callbacks. Other native properties pass through.

CloudEvents attributes use AMQP `cloudEvents:` headers. The serializer's `content-type` becomes native `BasicProperties.ContentType`. Existing creation `traceparent` and `tracestate` are preserved even when the native publisher instrumentation is enabled. The pinned client only accepts strings or exact byte arrays for textual field-table values; Relayer reuses exact-length buffers for borrowed UTF-8 slices. Each publisher (including each retained batch slot) keeps at most 256 header position/length buffers and 256 decoded content types in FIFO caches. Cache storage grows with the shapes actually encountered; empty slots do not reserve 256 entries. Slots and awaiters remain proportional to the largest batch on that channel, so split very large inputs into smaller batches to limit retained storage. Independent slot buffers remain necessary because native flow control can pause before serialization. Cache hits allocate 0 B. A UTF-8 slice miss allocates one exact-length byte array; a content-type miss allocates a decoded string and a UTF-8 key array. Evicted shapes allocate again when reused. These conversions are native-client boundary costs under D52: the pinned client requires arrays or strings. Retention is bounded by entry count, not bytes; large values still retain large buffers. The temporary header cache will be removed when native memory-backed headers ship (rabbitmq/rabbitmq-dotnet-client#2144). Send metrics remain available when message tracing is absent. The sent-message counter records each native publish attempt, including failed publishes, regardless of tracing; validation and preparation failures do not count. Single-send duration starts after acquiring a publisher channel, so it includes channel recovery and confirmation but excludes waiting for a free publisher. Batch item durations start when each item is admitted after channel recovery and include its confirmation wait. Send completion sources retain at most 256 idle operations, including when continuations complete on another thread; overflow operations become collectible after their returned `ValueTask` is consumed.

Host shutdown cancels consumers and drains their durable work while publishing channels remain available. RabbitMQ starts draining its publishers after all hosted services finish their stop phase, so an outbox worker can publish during its own stop regardless of registration order. A cancellation during shutdown closes the connection to release native operations. Always start the application host before sending.

## Native delays and the outbox fallback

Native delays require the delayed-message plugin **and a successfully verified delayed exchange**. Configure an alternate delayed exchange for each ordinary exchange that needs scheduling. An empty key selects routes through the default exchange:

```csharp
rabbit.TopologyMode = TopologyMode.Provision;
rabbit.Options.Configure(options =>
{
    options.DelayedExchanges.Add("", new("orders.delayed")
    {
        Type = "x-delayed-message",
        Arguments = new Dictionary<string, object?> { ["x-delayed-type"] = "direct" },
    });
});
rabbit.Send<OrderPlaced>("orders");
```

Bind `orders.delayed` to `orders` with routing key `orders` through infrastructure tooling or native APIs before scheduling. Delayed exchange declarations pass their native durability, auto-delete and argument settings through unchanged. They do not create bindings or change the immediate destination. Do not also declare an optional delayed exchange in `Exchanges`: regular topology declarations are required and a missing plugin makes those declarations fail.

During hosted startup, explicit `Provision` verifies the alternate exchange with native `ExchangeDeclareAsync`. Only the broker's specific unknown-exchange-type response selects fallback. Permission failures, incompatible declarations and other errors fail startup. A successful declaration enables delays less than `2^32` milliseconds, rounded up to the next millisecond; immediate sends continue using the ordinary exchange. Relayer recomputes the remaining delay after waiting for a publisher and replacing a closed channel, so those waits do not extend `DeliverAt`. Expired delivery times publish immediately through the ordinary exchange with mandatory routing.

AMQP has no read-only API to inspect installed plugins or exchange types. `None` and `Validate` therefore do not probe, create or advertise delayed exchanges, even when a plugin is installed. These modes use conservative capabilities and the existing [outbox scheduling](../guides/sending.md) fallback. When the plugin is absent in `Provision`, the alternate exchange is not created and scheduling also uses that fallback. The core requires an active outbox transaction and a due-row dispatch worker; direct future sends fail clearly when no verified native delay is available. Dispatch of a due outbox row publishes immediately through the ordinary destination.

The plugin stores delayed messages using its own native durability and routing guarantees. It cannot report eventual routability at enqueue time: delayed publishes use native confirms with `mandatory: false`. A confirm does not guarantee that a binding will exist when the delay expires, or replication of the plugin's local message store. Choose the outbox fallback when those native plugin guarantees do not meet the application's requirements.

## Send telemetry

Enable source `Relayer` and meter `Relayer`. Send destinations use `{exchange}:{routing key}`, including `:orders` for a default-exchange queue. Routing key, system, destination and configured server address/port are available to samplers when spans start. Single sends preserve message creation links and use the common CloudEvents attributes. Sends record attempt counts and client operation duration, including native confirmation time; failures carry the bounded `_OTHER` error type.

RabbitMQ.Client 7.2.2 already owns transport-level publisher instrumentation. Enable `RabbitMQ.Client.Publisher` for those additional child spans; Relayer neither replaces it nor changes `RabbitMQActivitySource`'s process-wide configuration. `RabbitMQ.Client.Subscriber` supplies native delivery spans. Relayer adds durable process and actual ack/nack operations through the shared `Relayer` source without changing native tracing configuration.


## Receive and settle

```csharp
services.AddRelayer(relayer =>
{
    relayer.AddMessageTypesFromThisAssembly();
    relayer.RabbitMQ("orders", rabbit =>
    {
        rabbit.Options.Configure(options =>
        {
            options.ConnectionFactory = nativeConnectionFactory;
            options.Queues.Add(new("orders"));
            options.Queues.Add(new("orders.dead-letter"));
        });
        rabbit.Consume("orders", consumer =>
        {
            consumer.Concurrency(4);
            consumer.Options.ChannelOptions = new CreateChannelOptions(
                publisherConfirmationsEnabled: false,
                publisherConfirmationTrackingEnabled: false,
                consumerDispatchConcurrency: 4);
            consumer.Options.PrefetchCount = 32;
            consumer.Handler<OrderPlacedHandler>();
        });
    });
});
```

Use single-message handlers. `ChannelOptions` passes through unchanged; its native `ConsumerDispatchConcurrency`
must match `Concurrency`. Startup rejects mismatches and batch handlers. Defaults are one callback and a
prefetch count of one. RabbitMQ.Client owns dispatch concurrency, prefetch backpressure, automatic connection
and topology recovery, and native cancellation. `Exclusive` and `Arguments` pass to `BasicConsumeAsync`.
Queue routing stays native.

`RabbitMqMessageBinding` borrows the body and reads headers lazily. `context.RabbitMQ()` exposes the original
`BasicDeliverEventArgs` through `Message` and its native `IChannel` through `Channel`. Retain neither after
the callback. Direct native settlement must set
`context.Features.GetRequired<NativeMessage<RabbitMqDelivery>>().Disposition = Disposition.SettledByHandler`
after success to prevent a second settlement. Prefer the common settlement API when native access is unnecessary.

Relayer acknowledges after handling, scoped disposal and durable failure handling finish. Default poison
handling sends to `source + ".dead-letter"`, waits for its publisher confirm, then acknowledges the original.
Set `DeadLetterEndpoint` in `consumer.Failures(new FailureOptions { DeadLetterEndpoint = "orders.errors" })` to use another declared destination.
For a consumer on `orders`, startup rejects both `orders` and `:orders` because the default exchange routes
both forms back to that queue. When using a named exchange, keep its dead-letter route clear of the source queue.
Copies retain CloudEvents,
creation context, native headers and native correlation/reply destinations, and add common error metadata. A failed or unroutable copy is logged and leaves the
source unacknowledged for a one-second backoff, then uses native `basic.nack` with `requeue: true`. The host
continues running. A delivery already settled successfully by native handler code is never settled again if later processing throws. Optional consumer health remains unhealthy while any delivery is in failure backoff, and reports the last failure until subsequent processing succeeds. Successful processing clears the prior failure even when the requeue callback is still finishing; health remains unhealthy until all failure backoffs have ended. Provision
a durable failure queue or exchange/binding before starting consumers.

Use `Failure.RetryDelays` for in-process backoff. Native nack can requeue immediately or use the configured DLX
with `requeue: false`; it cannot rewrite headers or retain Relayer error metadata. The `DeadLetter` capability
therefore stays false and configured durable `RedeliveryDelays` fail startup. Explicit immediate requeue retains
native semantics, including potentially repeated deliveries without an attempt count. The native `Redelivered`
flag is exposed unchanged and is never treated as a delivery counter. Native delayed sends do not add delayed nack.

Graceful stop starts native cancellation on every consumer in the named transport. Each consumer waits for
native cancel-ok and active callbacks before closing its settlement channel. The host's stop deadline cancels
processing. Unexpected callback failure reaches the hosted service and leaves uncertain
work unacknowledged. A connection outage that prevents acknowledgement leaves the delivery for native
redelivery without terminating the host when both `ConnectionFactory.AutomaticRecoveryEnabled` and
`ConnectionFactory.TopologyRecoveryEnabled` are true. RabbitMQ.Client restores subscriptions as part of
topology recovery. With either setting disabled, an unexpected connection shutdown faults the receiver
and reaches the hosted service even when no handler is active. Enable both settings for consumers that
must resume after an outage. Channel protocol errors and other unrecoverable native callback errors still
fault the receiver. Failed poison copies on an open consumer channel use the backoff and requeue policy above.
Normal host shutdown does not fault completion. When cancellation forces the consumer channel to close before a delivery can be acknowledged, RabbitMQ requeues the unacknowledged delivery. Cancellation of that callback or an acknowledgement interrupted by the channel’s normal close does not fault consumer completion. Automatic recovery remains owned by RabbitMQ.Client.

Process and settle destinations are the consumed queue, with names `process {queue}`, `ack {queue}` and
`nack {queue}`. Each consumer composes these operations once at startup; distinct exchanges or routing keys
create no tracing cache entries. Samplers receive the queue destination, system, operation and configured
server address/port at span creation. The delivered routing key is added only when the span requests data,
along with the delivery tag; it is not a sampler input. This replaces the previous
`{exchange}:{routing key}:{queue}` destination and routing-key sampler attributes, keeping span names bounded
for wildcard bindings such as `order.#`. Send telemetry retains its exchange/routing-key destination.
Relayer composes each consumer's handler and failure pipeline at startup. If any pipeline fails to compose,
Relayer discards the partial set. A successful retry of host startup on the same host starts one subscription
per configured consumer.

A suspended consumer callback must return `Task`. D52 treats that task as native-client boundary cost, measured
against the equivalent raw callback. The upstream [8.0 consumer API proposal](https://github.com/rabbitmq/rabbitmq-dotnet-client/issues/964) discusses a `ValueTask` boundary. `RabbitMqReceiveBenchmarks` compares native JSON receive/ack with the
registered pipeline using `MemoryDiagnoser`. Both fixed and high-cardinality routing keys use equivalent native JSON/ack work. Warm pipeline, body binding and optional health
tracking add no per-message objects of their own.

### Start consumers later

Consumers subscribe with the host by default. Set `AutoStartConsumers = false` to bring the host up first and start them yourself through the transport's keyed `IConsumerControl`:

```csharp
using Microsoft.Extensions.DependencyInjection;
using Relayer.Transports;

rabbit.Options.Configure(options => options.AutoStartConsumers = false);

// After the host starts.
var consumers = app.Services.GetRequiredKeyedService<IConsumerControl>("orders");
await consumers.StartAsync(cancellationToken);
```

The host still applies topology, validates and composes every consumer, and opens the connection and publisher channels at startup, so sending works while the consumers are stopped and messages wait on their queues. `StartAsync` issues the native `basic.consume` and fails until the host signals `ApplicationStarted` and after the host stops. `StopAsync` sends `basic.cancel` and drains in-flight callbacks like host shutdown, so the same control pauses and resumes consumption; both calls do nothing when the consumers are already in that state. Host shutdown stops consumers that the control started, even when its token has expired. If startup is cancelled while rolling back a partially started group, Relayer closes the affected consumer channels. RabbitMQ requeues any unacknowledged deliveries, and acknowledgements interrupted by that channel close do not fault the host. While stopped, consumer [health](#health) reports them as unavailable.

## Health

Reference `Relayer.RabbitMQ.HealthChecks` alongside `Relayer.HealthChecks` and register before host startup:

```csharp
using Relayer.HealthChecks;

services.AddRelayerRabbitMqHealthProviders("orders"); // Exact transport name.
services.AddRelayerHealthChecks();
```

Connectivity reads the existing native connection's `IsOpen` state. It never connects independently or publishes,
consumes, settles or declares probe messages. Closed/recovering connections report unhealthy. Liveness is
unavailable before startup, during native recovery and after shutdown; idle receivers explicitly report healthy.
Active receivers report the oldest callback's activation time across concurrent workers, so newer completions
cannot hide a stalled older callback. `ConsumerStallTimeout` determines when that snapshot becomes unhealthy.
Health tracking is enabled only by this optional registration. The same registration works in test hosts
that call `AddRelayerHarness()`.


## Performance validation

`RabbitMqSendBenchmarks` compares native confirmed publishing with Relayer on the same broker. `Single` measures the pre-serialized transport boundary. `Batch` compares 100 or 10,000 pre-serialized messages on one channel, admitting the native publishes before awaiting all confirmations in both paths; results are per batch (divide time and allocations by `BatchSize` for per-message values). `DefaultJson` compares `ISender.SendAsync` with raw publishing: both serialize the same model using the registered default JSON serializer and generate fresh CloudEvents metadata. Both use persistent messages, mandatory publishing and tracked publisher confirmations. A shared consumer drains both paths during measurement.

Set `RELAYER_RABBITMQ_URI` to a test broker, then run:

```shell
dotnet run -c Release --project benchmarks/Relayer.Benchmarks -- --filter '*RabbitMqSendBenchmarks*' --anyCategories DefaultJson
```

`MemoryDiagnoser` includes native-client and serializer allocations. Local results are diagnostic; adding this comparison does not establish the 5% throughput budget.

### Send pool retention

RabbitMQ retains at most 256 idle send completion objects per transport. Concurrent sends above
the bound remain supported and allocate temporary overflow state on each burst. Native completion
still controls the lifetime of borrowed message memory. Completion state is cleared and released
only after the caller consumes the returned `ValueTask`.
The bound limits object count, not bytes: retained state can keep storage sized for its largest batch.
There is no retention option.

`TransportPoolRetentionBenchmarks` measures warmed pool reuse at concurrency 1 and 256 and repeated
512-operation bursts with `MemoryDiagnoser`. Measurements isolate pooling from broker I/O and do not
establish whole-path throughput parity.
