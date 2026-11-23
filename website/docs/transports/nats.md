---
title: NATS JetStream
description: "Confirmed native JetStream sends, durable pull consumers and explicit topology."
---

`Relayer.Nats` wraps NATS.Net 3.4.1 with owned named connections, confirmed JetStream publications, durable pull consumers and confirmed acknowledgements. Enable JetStream on the server (`nats-server -js`). The [hosted Linux comparison](https://github.com/thomhurst/Relayer/actions/runs/42847435555) accepted in [#468](https://github.com/thomhurst/Relayer/pull/468) measured complete-path mean overhead of 1.05% for synchronously driven confirmed sends, 2.62% for naturally awaited sends and 1.49% for confirmed receive. Reported native/Relayer allocations were 2856/2856 B for both send comparisons and 5510/5509 B for receive. Those means support the 5% budget on that runner; allocation pins and sampled stacks supplement the comparison without proving the absence of every occasional allocation. No performance exception was accepted. Earlier measurements and the pending attribution status are superseded by that run. An unexplained historical Native AOT initial-connect failure remains documented in [#313](https://github.com/thomhurst/Relayer/issues/313#issuecomment-6765026417); later successful executions do not establish its cause.

## Register sends and a durable consumer

```csharp
using Microsoft.Extensions.Options;
using NATS.Client.Core;
using NATS.Client.JetStream.Models;
using Relayer;
using Relayer.Nats;

services.AddRelayer(relayer =>
{
    relayer.AddMessageTypesFromThisAssembly();
    relayer.Nats("events", nats =>
    {
        nats.TopologyMode = TopologyMode.Provision;
        nats.Options.Configure(options =>
        {
            options.ConnectionOptions = new NatsOpts { Url = "nats://localhost:4222" };
            options.Streams.Add(new("orders")
            {
                Configuration = new StreamConfig("orders", ["orders.placed", "orders.dead"]),
            });
            options.Consumers.Add(new("orders", "billing")
            {
                Configuration = new ConsumerConfig("billing") { FilterSubject = "orders.placed" },
            });
        });
        nats.Send<OrderPlaced>("orders.placed");
        nats.Consume("billing", consumer =>
        {
            consumer.Options.Stream = "orders";
            consumer.Failures(new()
            {
                RetryDelays = new[] { TimeSpan.FromSeconds(1) },
                DeadLetterEndpoint = "orders.dead",
            });
            consumer.Handler<BillingHandler>();
        });
    });
});
```

Register source-generated JSON serialization as described in [serialization](../guides/messages.md). Send routes use concrete publish subjects; consumers select a declared durable consumer and its stream. A topology consumer declaration alone does not start a receiver.

`ConnectionOptions` passes `NatsOpts` unchanged, including authentication, TLS, logger factory, header case sensitivity and native recovery. Optional `JetStreamOptions` passes `NatsJSOpts` unchanged, including domains, API prefixes and timeouts. `PublishOptions` passes native `NatsJSPubOpts` unchanged to every publication on that named transport, including its retry policy. DI resolution does not connect. Relayer closes only its own named connection.

NATS.Net 3.4.0 always uses TLS for `tls://` and `wss://` URLs. In the default `TlsMode.Auto`, either scheme requires TLS for all servers in the pool. Use `TlsMode.Prefer` if a mixed pool intentionally includes plaintext URLs. For TLS upgrades after INFO against servers older than 2.12, list all cluster servers explicitly to keep failover available during initial discovery. See the [NATS.Net 3.4.0 release notes](https://github.com/nats-io/nats.net/releases/tag/v3.4.0) for the full set of changes.

## Delivery and failure boundaries

Sends await native JetStream publication acknowledgements. Borrowed payload/header storage remains owned by the caller until completion. `SendBatchAsync` performs individual confirmed publications in input order; it is not an atomic broker batch.

When tracing is enabled, a batch for one subject emits a logical send span with the batch count and links to each message's creation context. A batch across subjects emits per-message send spans. See [send tracing](../guides/sending.md#send-tracing).

Receivers require durable pull consumers with explicit per-message acknowledgement, full payloads and unlimited `MaxDeliver`. Unsupported modes fail before consumption: auto/no acknowledgement can settle before durable work, and finite `MaxDeliver` can strand a poison message before its durable failure copy succeeds. Native flow control, delivery attempts, reconnect and acknowledgement timing remain NATS behavior. Configure `AckWait` for the processing duration, or call native `AckProgressAsync` through the delivery feature for long work; Relayer adds no renewal loop.

`consumer.Options.Settings` passes native `NatsJSConsumeOpts` unchanged. `DrainOnCancel = true` is required and enabled by default. `Concurrency(n)` starts `n` native pull enumerations against the durable consumer, each processing one delivery at a time. Native buffer limits apply per enumeration. Shutdown requests pull cancellation for every durable consumer before waiting for workers to drain. JetStream drains through its native protocol. A stop deadline cancels processing. Incomplete work remains available for native redelivery.

Hosted NATS keeps its connection open until all hosted services finish `StopAsync`, including for send-only registrations. Handlers in other hosted consumers can flush buffered JetStream sends within the shutdown deadline, even when services stop concurrently.
If shutdown starts with an expired token, NATS can still begin shutdown when no other transport lifecycle operation is running. It requests pull cancellation and closes the connection in `StoppedAsync` when all consumers have drained, including for send-only registrations.

Starting an already running transport returns without probing the connection; native reconnect remains responsible for an outage. If a bounded stop is canceled while native drain is still running, the next start waits for that shutdown to finish before opening a fresh connection and restarting the durable pull consumers. Cancellation of the new start still propagates. Relayer logs a non-cancellation confirmed ACK failure and marks the settlement and process spans as failed. The process duration records `error.type = "_OTHER"`. Native NATS acknowledgement telemetry also records the error. Consumption continues, and native `AckWait` controls redelivery if the server did not receive the ACK. Relayer does not retry ACKs or undo durable work. Handlers must be idempotent: if the server did not receive an ACK, redelivery can execute the handler again after its side effects are already durable. An exception escaping message processing, failure-copy publication or settlement is logged and leaves the delivery unacknowledged. Consumption continues; native `AckWait` controls redelivery without a Relayer retry loop. An unavailable dead-letter destination therefore produces repeated errors until it is restored; monitor these errors and pending acknowledgements. Processing cancellation still aborts the worker, and faults from the native pull enumerator fault the hosted consumer and cancel sibling pull enumerations.

Poison messages use the existing core failure pipeline: in-process `RetryDelays`, then a copy to the required `DeadLetterEndpoint` subject, confirmed before ACK of the original. Ensure that subject is captured by a JetStream stream. Copy/ACK failure can duplicate the copy at the at-least-once boundary. Original bodies, CloudEvents identity and creation context survive, including malformed envelopes. Copies omit the original native `Nats-Msg-Id` so stream-wide deduplication cannot suppress a dead-letter copy to another subject in the same stream.

At startup, Relayer asks JetStream which streams capture the dead-letter subject. If the consumer's own stream captures it, Relayer reads that stream's input subject transform and checks the stored subject against the durable consumer's filters, including wildcards and multiple filters. Startup fails if the consumer can receive the dead-letter copy. Narrow the consumer filter or choose a different dead-letter subject. An unfiltered consumer needs a dead-letter subject outside its stream.

Relayer checks literal transform destinations. If the transform applies to the dead-letter subject and uses a destination template, such as a wildcard substitution or a mapping function, startup fails because Relayer cannot validate the stored subject. Choose a dead-letter subject outside the transform or configure a literal destination that the consumer excludes.

This check requires permission to request `$JS.API.STREAM.NAMES` when a dead-letter subject is configured and `$JS.API.STREAM.INFO.<stream>` when the consumer's own stream captures it. With a custom JetStream API prefix, grant the equivalent permissions under that prefix.

JetStream NAK cannot persist changed failure headers. Portable `RedeliveryDelays` and portable manual retry therefore fail explicitly; use in-process retries and durable dead-letter copies. Native NAK remains accessible through `context.Nats().Message`. Relayer does not advertise native dead-lettering with error metadata, portable keyed ordering, native transactions, scheduled sends or tombstones. Use the durable outbox for scheduling. Subject/stream order remains native, without a portable per-key guarantee.

## Native access and tracing

```csharp
var delivery = context.Nats();
var nativeMessage = delivery.Message;
var nativeConsumer = delivery.Consumer;

await sender.SendWithoutOutboxAsync(order,
    options => options.Nats(headers => headers["application-header"] = "value"), ct);
```

The original native message exposes its connection. Delivery objects are borrowed until processing returns. Send callbacks receive reusable native `NatsHeaders` immediately before publication; do not retain them. CloudEvents attributes use `ce-` headers. Dead-letter copies preserve native header value lists and omit the original stream-wide deduplication ID (`Nats-Msg-Id`), native publication preconditions (`Nats-Expected-*`) and destructive rollup commands (`Nats-Rollup`), including values injected by transport-wide publication options. Copies also omit `Nats-TTL`, so the dead-letter stream applies its own retention policy and can accept the copy with per-message TTL disabled. Relayer strips JetStream schedule and purge headers (`Nats-Schedule`, `Nats-Schedule-*` and `Nats-Scheduler`) so the copy is an ordinary publication, even when the destination stream has scheduling disabled. Ordinary sends retain their configured native options and headers. Relayer telemetry uses `messaging.system = nats`, the publish subject for sends, and the native stream/durable subscription for processing and settlement. Its public native context-aware serializer hook preserves stored `traceparent`/`tracestate` even with NATS instrumentation enabled, so dispatch and failure copies keep their creation context.

## Topology modes

`None` makes no topology requests. `Validate` reads stream/consumer information only. Name-only declarations check existence. With `Configuration`, validation compares native generated JSON settings, normalizes JetStream scalar defaults and unordered subject sets, and ignores server-owned `_nats.*` metadata. For a stream without a mirror or sources, omitting subjects is equivalent to declaring the stream name as its only subject. A mirror or sourced stream with no subjects keeps its empty subject set. Validation applies the two-minute duplicate window default, capped by a shorter maximum age, only to streams without a mirror or sources. Validation treats an omitted persistence mode as `StreamConfigPersistMode.Default`. An `Async` declaration requires a stream configured with `Async`. Differences identify the entity, setting and expected/actual values. Validation never publishes probes or changes settings.

`Provision` calls native create-only APIs with the supplied `StreamConfig`/`ConsumerConfig`. The server checks existing entities for equivalence. Relayer never updates, deletes or purges entities; reconcile conflicts externally. Names and optional `DurableName` must match the declaration.

## Performance and validation

[D52](https://github.com/thomhurst/Relayer/blob/main/docs/decisions.md#d52-allocations-a-native-api-requires-count-as-native-client-cost) classifies strings required by `NatsHeaders` for changing CloudEvents attributes as native-client cost. The native receive API also boxes JetStream deliveries and materializes raw payload memory. Relayer retains at most 64 reusable send operations per subject, each owning its native header dictionary and publication continuation. Completion state returns to that endpoint after the caller consumes the send, including when initiation and completion run on different threads. Overflow allocates additional operations and header dictionaries; retention stays bounded. Settlement state uses the runtime async pool. These mechanisms do not establish whole-path allocation parity or waive the performance budget.

`NatsSendBenchmarks` compares native publication and the full Relayer JSON/CloudEvents send path with matching native header conversions (`NATS_URL` selects the real server). `NatsReceiveBenchmarks` compares already-delivered JSON dispatch with the core receive path. `NatsWireReceiveBenchmarks` additionally compares real native pull, JSON dispatch and confirmed ACK paths, preparing publications outside each measured iteration. All use `MemoryDiagnoser`; local results are diagnostic. Allocation tests pin header conversion against equivalent native string conversion. [nats.net#1285](https://github.com/nats-io/nats.net/issues/1285) requests byte-backed headers as an optimization, not an execution prerequisite.

Real-server tests cover topology, read-only configuration checks, recovery, hosted failure copies and every applicable reusable conformance case; unsupported capabilities skip explicitly. Outage recovery failures report the outstanding message values. The NATS fixture captures native connection logs and snapshots worker completion, connection state and consumer pending/acknowledgement counts after scenario shutdown. Set `NATS_URL` when running the Native AOT smoke to exercise real JSON send/receive and confirmed settlement.
