---
title: NATS JetStream
description: "Confirmed native JetStream sends, durable pull consumers and explicit topology."
---

`Relayer.Nats` wraps NATS.Net 3.3.2 with owned named connections, confirmed JetStream publications, durable pull consumers and confirmed acknowledgements. Enable JetStream on the server (`nats-server -js`). The [hosted Linux comparison](https://github.com/thomhurst/Relayer/actions/runs/42847435555) accepted in [#468](https://github.com/thomhurst/Relayer/pull/468) measured complete-path mean overhead of 1.05% for synchronously driven confirmed sends, 2.62% for naturally awaited sends and 1.49% for confirmed receive. Reported native/Relayer allocations were 2856/2856 B for both send comparisons and 5510/5509 B for receive. Those means support the 5% budget on that runner; allocation pins and sampled stacks supplement the comparison without proving the absence of every occasional allocation. No performance exception was accepted. Earlier measurements and the pending attribution status are superseded by that run. An unexplained historical Native AOT initial-connect failure remains documented in [#313](https://github.com/thomhurst/Relayer/issues/313#issuecomment-6765026417); later successful executions do not establish its cause.

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
            consumer.Options.Failure = new()
            {
                RetryDelays = new[] { TimeSpan.FromSeconds(1) },
                DeadLetterEndpoint = "orders.dead",
            };
            consumer.Handler<BillingHandler>();
        });
    });
});
```

Register source-generated JSON serialization as described in [serialization](../guides/messages.md). Send routes use concrete publish subjects; consumers select a declared durable consumer and its stream. A topology consumer declaration alone does not start a receiver.

`ConnectionOptions` passes `NatsOpts` unchanged, including authentication, TLS, logger factory, header case sensitivity and native recovery. Optional `JetStreamOptions` passes `NatsJSOpts` unchanged, including domains, API prefixes and timeouts. `PublishOptions` passes native `NatsJSPubOpts` unchanged to every publication on that named transport, including its retry policy. DI resolution does not connect. Relayer closes only its own named connection.

## Delivery and failure boundaries

Sends await native JetStream publication acknowledgements. Borrowed payload/header storage remains owned by the caller until completion. `SendBatchAsync` performs individual confirmed publications in input order; it is not an atomic broker batch.

Receivers require durable pull consumers with explicit per-message acknowledgement, full payloads and unlimited `MaxDeliver`. Unsupported modes fail before consumption: auto/no acknowledgement can settle before durable work, and finite `MaxDeliver` can strand a poison message before its durable failure copy succeeds. Native flow control, delivery attempts, reconnect and acknowledgement timing remain NATS behavior. Configure `AckWait` for the processing duration, or call native `AckProgressAsync` through the delivery feature for long work; Relayer adds no renewal loop.

`consumer.Options.Settings` passes native `NatsJSConsumeOpts` unchanged. `DrainOnCancel = true` is required and enabled by default. `Concurrency(n)` starts `n` native pull enumerations against the durable consumer, each processing one delivery at a time. Native buffer limits apply per enumeration. Shutdown cancels pulls, drains through the native protocol, finishes buffered processing and confirms ACKs before closing. A stop deadline cancels processing; incomplete work remains available for native redelivery.

Starting an already running transport returns without probing the connection; native reconnect remains responsible for an outage. If a bounded stop is canceled while native drain is still running, the next start waits for that shutdown to finish before opening a fresh connection and restarting the durable pull consumers. Cancellation of the new start still propagates. A non-cancellation confirmed-ACK failure is logged and marks its settlement span as failed; native NATS acknowledgement telemetry records the error. Consumption continues, and native `AckWait` controls redelivery if the server did not receive the ACK. Relayer does not retry ACKs or undo durable work. Handlers must be idempotent: if the server did not receive an ACK, redelivery can execute the handler again after its side effects are already durable. An exception escaping message processing, failure-copy publication or settlement is logged and leaves the delivery unacknowledged. Consumption continues; native `AckWait` controls redelivery without a Relayer retry loop. An unavailable dead-letter destination therefore produces repeated errors until it is restored; monitor these errors and pending acknowledgements. Processing cancellation still aborts the worker, and faults from the native pull enumerator fault the hosted consumer and cancel sibling pull enumerations.

Poison messages use the existing core failure pipeline: in-process `RetryDelays`, then a copy to the required `DeadLetterEndpoint` subject, confirmed before ACK of the original. Ensure that subject is captured by a JetStream stream. Copy/ACK failure can duplicate the copy at the at-least-once boundary. Original bodies, CloudEvents identity and creation context survive, including malformed envelopes. Copies omit the original native `Nats-Msg-Id` so stream-wide deduplication cannot suppress a dead-letter copy to another subject in the same stream.

JetStream NAK cannot persist changed failure headers. Portable `RedeliveryDelays` and portable manual retry therefore fail explicitly; use in-process retries and durable dead-letter copies. Native NAK remains accessible through `context.Nats().Message`. Relayer does not advertise native dead-lettering with error metadata, portable keyed ordering, native transactions, scheduled sends or tombstones. Use the durable outbox for scheduling. Subject/stream order remains native, without a portable per-key guarantee.

## Native access and tracing

```csharp
var delivery = context.Nats();
var nativeMessage = delivery.Message;
var nativeConsumer = delivery.Consumer;

await sender.SendNowAsync(order,
    options => options.Nats(headers => headers["application-header"] = "value"), ct);
```

The original native message exposes its connection. Delivery objects are borrowed until processing returns. Send callbacks receive reusable native `NatsHeaders` immediately before publication; do not retain them. CloudEvents attributes use `ce-` headers. Dead-letter copies preserve native header value lists and omit the original stream-wide deduplication ID, native publication preconditions (`Nats-Expected-*`) and destructive rollup commands (`Nats-Rollup`), including values injected by transport-wide publication options. Those commands apply to the original publication; retaining them could reject its durable failure copy or purge earlier dead-letter messages. Ordinary sends retain the configured native options. Relayer telemetry uses `messaging.system = nats`, the publish subject for sends, and the native stream/durable subscription for processing and settlement. Its public native context-aware serializer hook preserves stored `traceparent`/`tracestate` even with NATS instrumentation enabled, so dispatch and failure copies keep their creation context.

## Topology modes

`None` makes no topology requests. `Validate` reads stream/consumer information only. Name-only declarations check existence. With `Configuration`, validation compares native generated JSON settings, normalizes JetStream scalar defaults and unordered subject sets, and ignores server-owned `_nats.*` metadata. Differences identify the entity, setting and expected/actual values. Validation never publishes probes or changes settings.

`Provision` calls native create-only APIs with the supplied `StreamConfig`/`ConsumerConfig`. The server checks existing entities for equivalence. Relayer never updates, deletes or purges entities; reconcile conflicts externally. Names and optional `DurableName` must match the declaration.

## Performance and validation

[D52](https://github.com/thomhurst/Relayer/blob/main/docs/decisions.md#d52-allocations-a-native-api-requires-count-as-native-client-cost) classifies strings required by `NatsHeaders` for changing CloudEvents attributes as native-client cost. The native receive API also boxes JetStream deliveries and materializes raw payload memory. Relayer retains at most 64 reusable send operations per subject, each owning its native header dictionary and publication continuation. Completion state returns to that endpoint after the caller consumes the send, including when initiation and completion run on different threads. Overflow allocates additional operations and header dictionaries; retention stays bounded. Settlement state uses the runtime async pool. These mechanisms do not establish whole-path allocation parity or waive the performance budget.

`NatsSendBenchmarks` compares native publication and the full Relayer JSON/CloudEvents send path with matching native header conversions (`NATS_URL` selects the real server). `NatsReceiveBenchmarks` compares already-delivered JSON dispatch with the core receive path. `NatsWireReceiveBenchmarks` additionally compares real native pull, JSON dispatch and confirmed ACK paths, preparing publications outside each measured iteration. All use `MemoryDiagnoser`; local results are diagnostic. Allocation tests pin header conversion against equivalent native string conversion. [nats.net#1285](https://github.com/nats-io/nats.net/issues/1285) requests byte-backed headers as an optimization, not an execution prerequisite.

Real-server tests cover topology, read-only configuration checks, recovery, hosted failure copies and every applicable reusable conformance case; unsupported capabilities skip explicitly. Outage recovery failures report the outstanding message values. The NATS fixture captures native connection logs and snapshots worker completion, connection state and consumer pending/acknowledgement counts after scenario shutdown. Set `NATS_URL` when running the Native AOT smoke to exercise real JSON send/receive and confirmed settlement.
