---
title: Google Cloud Pub/Sub
description: Native Pub/Sub publishing, streaming pull, acknowledgement and topology with explicit capability limits.
---

`Relayer.GooglePubSub` wraps `Google.Cloud.PubSub.V1` 3.x. Native publishing, streaming pull, ordering, lease extension, flow control, recovery and acknowledgement remain owned by Google's client. Relayer composes its generated dispatch, serialization, failure handling and tracing around those operations. Packages are not released yet; use project references while developing.

## Register topics and subscriptions

Supply prepared `PublisherServiceApiClient` and `SubscriberServiceApiClient` instances. They and their channels remain caller-owned. For a regular JIT application, Google's native builders can prepare them with application default credentials:

```csharp
using Google.Cloud.PubSub.V1;
using Relayer.GooglePubSub;

var publisher = await new PublisherServiceApiClientBuilder().BuildAsync();
var subscriber = await new SubscriberServiceApiClientBuilder().BuildAsync();
var topic = TopicName.FromProjectTopic("my-project", "orders");
var deadLetter = TopicName.FromProjectTopic("my-project", "orders-dead");
var subscription = SubscriptionName.FromProjectSubscription("my-project", "billing");

services.AddRelayer(relayer =>
{
    relayer.AddMessageTypesFromThisAssembly();
    relayer.UseJsonSerializer(AppJson.Default);
    relayer.GooglePubSub("events", pubsub =>
    {
        pubsub.TopologyMode = TopologyMode.Provision;
        pubsub.Options.Configure(options =>
        {
            options.Publisher = publisher;
            options.Subscriber = subscriber;
            options.Topics.Add(topic);
            options.Topics.Add(deadLetter);
            options.Subscriptions.Add(new Subscription
            {
                SubscriptionName = subscription,
                TopicAsTopicName = topic,
                AckDeadlineSeconds = 60,
            });
        });
        pubsub.Send<OrderPlaced>(topic.ToString());
        pubsub.Consume(subscription.ToString(), consumer =>
        {
            consumer.Options.Failure = new()
            {
                DeadLetterEndpoint = deadLetter.ToString(),
                RetryDelays = new[] { TimeSpan.FromSeconds(1) },
            };
            consumer.Handler<OrderPlacedHandler>();
        });
    });
});
```

Use fully qualified native endpoint names. Topics must be declared in `Topics`; consumers refer to entries in `Subscriptions`. These are ordinary named Options registrations. Use the generated handler and JSON registration described in [messages](../guides/messages.md).

`PublisherClient.Settings` and `SubscriberClient.Settings` pass through as native settings. Relayer snapshots settings when composing the transport. It creates native `PublisherClientImpl` instances per topic and `SubscriberClientImpl` instances per running consumer, using your prepared API clients. Stopping drains handlers and native acknowledgements. Restarting a stopped transport creates fresh streaming subscribers over the same API clients. A fatal subscriber task fails the hosted background service; recreate the application host after that failure.

For concurrency greater than one, declare the same limit in both places:

```csharp
consumer.Concurrency(8);
consumer.Options.Settings = new SubscriberClient.Settings
{
    FlowControlSettings = new Google.Api.Gax.FlowControlSettings(8, 16_000_000),
};
```

The default maximum outstanding element count is one. Native flow control supplies backpressure and the concurrency bound; Relayer does not add a scheduler. A mismatch fails startup. Streaming callbacks deliver individual messages; batch handlers fail startup.

## Ordering and native access

For keyed ordering, set `PublisherSettings.EnableMessageOrdering = true`, set `Subscription.EnableMessageOrdering = true` on every consuming subscription, and use the common route ordering selector. Google's client retains responsibility for ordering keys in error, including `PublisherClient.ResumePublish`. Resolve `IGooglePubSubClients` with the transport name as its DI key, or use `context.GooglePubSub().Clients`, and call `PublisherFor(topic)` for those native operations. Do not dispose the returned publisher. Relayer does not claim exactly-once application effects, even when a native subscription enables exactly-once delivery.

Inside a handler, `context.GooglePubSub().Message` exposes the original `PubsubMessage` and `.Subscriber` exposes the native streaming client. Both are borrowed until processing ends. Configure outgoing messages with `new SendOptions().GooglePubSub(message => ...)`; portable ordering keys and headers normally need no callback.

CloudEvents attributes use the Pub/Sub binary binding's `ce-` prefix, for example `ce-id` and `ce-type`. `content-type`, `traceparent` and `tracestate` remain unprefixed. Data is the serialized body. Incoming data and attribute values are read lazily without copying. Failure copies preserve original creation context and envelope attributes.

## Settlement and limits

| Capability | Behavior |
|---|---|
| Delivery | At least once; Ack follows durable handler work and failure-copy completion |
| Native recovery | Google's streaming client reconnects and extends acknowledgement leases |
| Batching | Native publisher batching; up to 1,000 messages per native request, with native byte limits |
| Message size | Native 10,000,000-byte limit; attributes and request overhead also consume native limits |
| Scheduled sends | Not available natively; use the durable outbox |
| Durable redelivery policy | Unavailable: Nack cannot persist changed failure attributes; `RedeliveryDelays` fails startup |
| Dead-lettering | Explicit durable copy to a declared topic, followed by Ack; configure `Failure.DeadLetterEndpoint` |
| Native `DeadLetterPolicy` | Can be passed through, but cannot supply Relayer's on-demand dead-letter operation with error metadata |
| Transactions, tombstones, pause/resume, generic replay | Not advertised |

In-process `RetryDelays` reuse the core failure pipeline. Exhausted retries and poison messages are published to the explicit dead-letter topic with error history before acknowledging the original. A failed copy leaves the original unacknowledged. Google's callback Nack remains available for native processing failures; it redelivers unchanged attributes and cannot implement Relayer's durable retry history. The binding uses the native `GetDeliveryAttempt()` extension: a broker delivery count is available when native dead-letter queues supply it, otherwise it is null.

Pub/Sub [attribute values are limited to 1,024 UTF-8 bytes](https://docs.cloud.google.com/pubsub/quotas#resource_limits). Larger Relayer error histories are framed losslessly into `relayer.failure.history.part.0`, `.part.1`, and subsequent attributes, with their count in `relayer.failure.history.parts`. The binding reconstructs the logical `relayer.failure.history` header only when requested. Replacing history removes old parts; the body, CloudEvents and creation context stay unchanged. Reserve attribute capacity for failure copies: the service permits 100 attributes in total, 256-byte keys and 1,024-byte values. If the full history and original attributes cannot fit, publishing fails explicitly and the original stays unacknowledged; history is never silently truncated. Native dead-letter policy can provide an additional broker-managed safety net for messages that cannot be copied within service limits. These limits are enforced on failure copies even where the emulator accepts oversized attributes.

`PublisherClient.PublishAsync` has no cancellation parameter. Cancellation before admission prevents publishing. Once admitted, Relayer waits for native completion before returning cancellation, so a serializer buffer cannot be reused while the publisher still borrows it. Cancellation can therefore outlive the portable send timeout; delivery may have occurred. Configure native API deadlines and retries on your prepared client. Batch failure may leave some messages delivered; every admitted publish is drained before borrowed storage is released.

Relayer emits `gcp_pubsub` process/send spans and an `ack` settlement span when committing the callback reply to the native client. This span does not promise that an acknowledgement RPC has reached the broker. Native acknowledgement batching and exactly-once acknowledgement responses retain their native semantics.

## Topology

`None` makes no administration calls. `Validate` only reads topic/subscription state and reports missing entities or differences. `Provision` creates missing topics/subscriptions, tolerates concurrent creation and never rewrites or deletes existing entities.

Topic declarations check existence. Subscription declarations support pull delivery, topic, acknowledgement deadline, ordering, exactly-once delivery, filter, native dead-letter/retry policies, retention, expiration and labels. Native subscription messages are passed unchanged to creation. Nondefault unsupported fields, including push/export delivery, fail startup. Unspecified acknowledgement deadline, retention and expiration accept service defaults; declared values are checked. Extra labels on existing subscriptions are preserved.

## Native AOT and emulator validation

The adapter and generated JSON/handler path are Native AOT tested. Google's convenience builders still retain reflection and authentication dependencies that fail strict AOT publication ([upstream #13328](https://github.com/googleapis/google-cloud-dotnet/issues/13328)). Prepare native API clients through Google's public constructors over an application-owned managed gRPC channel instead. For the emulator:

```csharp
using Grpc.Net.Client;

using var channel = GrpcChannel.ForAddress("http://localhost:8085");
var publisher = new PublisherServiceApiClientImpl(
    new Publisher.PublisherClient(channel), null, null);
var subscriber = new SubscriberServiceApiClientImpl(
    new Subscriber.SubscriberClient(channel), null, null);
```

Keep the channel alive until Relayer stops and disposes its workers. Production channels require TLS and application-prepared credentials; validate the chosen authentication path separately under trimming/AOT. Relayer does not supply another authentication stack or suppress native warnings.

The tests use an isolated official Pub/Sub emulator per scenario. The conformance suite runs send/receive/settle, ordering, trace propagation, stored creation-context copies and recovery after Toxiproxy drops live TCP connections while the emulator preserves endpoint state. Native delay, on-demand dead-letter and metadata-preserving durable retry cases return explicit capability skips. Separate emulator tests cover native Nack, unchanged creation attributes, handler drain and durable poison copies. IAM, authentication, regional ordering, exactly-once acknowledgement guarantees and production dead-letter-policy forwarding are not established by emulator tests.

`tests/Relayer.GooglePubSub.AotSmoke` publishes a warning-clean executable and exercises topology plus a generated JSON handler round trip. Set `PUBSUB_EMULATOR_HOST` to a ready emulator. `GooglePubSubSendBenchmarks` compares acknowledged native and Relayer JSON sends against that emulator. `GooglePubSubBrokerReceiveBenchmarks` compares native streaming pull and drained acknowledgements over fresh 512-message workloads, using the same JSON contract, native flow control and handler on both paths. `GooglePubSubReceiveBenchmarks` compares already-delivered JSON processing with the same scoped handler; it diagnoses pipeline cost independently of network acquisition and is not a broker-throughput result. `GooglePubSubPoolBenchmarks` measures reusable state bursts within and beyond the fixed retention capacity. All use `MemoryDiagnoser`; local results are diagnostic, not performance compliance claims.

Relayer retains at most 256 reusable send states per topic publisher and 256 callback replies per consumer. Larger concurrent bursts remain supported: a pool miss allocates and returned overflow is collected rather than retained for the transport lifetime. Native string-valued attributes and ordering keys require string conversions on send; the raw send benchmark performs the same conversions.
