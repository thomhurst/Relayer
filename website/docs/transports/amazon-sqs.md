---
title: Amazon SQS and SNS
description: "Native SQS receiving, settlement, health and SNS publishing with AWS SDK v4."
---

`Relayer.AmazonSqs` implements native SQS sending and standard/FIFO receivers, and SNS topic publishing with AWS SDK v4. Receivers use native long polling, visibility changes and deletion after durable handling. Floci tests cover the transport contracts and production poison-message fallback.

## Register sending

```csharp
using Amazon;
using Amazon.SQS;
using Amazon.SQS.Model;
using Relayer;
using Relayer.AmazonSqs;

services.AddRelayer(relayer =>
{
    relayer.AddMessageTypesFromThisAssembly();
    relayer.AmazonSqs("orders", sqs =>
    {
        sqs.Options.Configure(options =>
        {
            options.Sqs = new AmazonSQSConfig { RegionEndpoint = RegionEndpoint.EUWest2 };
            options.Queues.Add(new CreateQueueRequest("orders"));
        });
        sqs.Send<OrderPlaced>("orders");
    });
});

await sender.SendAsync(new OrderPlaced(customerId));
```

Give messages a stable `[Message]` identity. `AddMessageTypesFromThisAssembly()` registers their [generated JSON metadata](../guides/messages.md#serialization). Start the host before sending. Every route destination must match a declared **queue name**, not a URL. Relayer resolves native queue URLs at startup. Named options support the [Options pattern](../guides/configuration.md).

The native `AmazonSQSConfig` passes unchanged to an owned client, which uses the AWS credential chain. Alternatively, set `SqsClient` to an existing `IAmazonSQS`; its configuration and credentials stay caller-owned, and Relayer never disposes it. Do not mutate captured configuration after startup. Use the separate typed `AmazonSns` declaration below for SNS topics.

## Queue topology

Declare queues with native `CreateQueueRequest` objects in `Queues`, including native attributes and tags. The default `Validate` mode calls only read APIs: `GetQueueUrl`, `GetQueueAttributes`, and, when tags are declared, `ListQueueTags`. It sends no probe message and requires no create, update or delete permission. Missing queues and mismatched declared settings fail startup with a topology report.

`Provision` creates missing queues with the original request and reports drift on existing queues. It never modifies or deletes existing queues. Declare native `RedrivePolicy` and related queue settings in `Attributes`; receivers validate that `maxReceiveCount` exceeds the longest run of consecutive native visibility retries by at least one, leaving a receive to schedule a replacement or copy failure metadata. A durably scheduled replacement starts with a fresh SQS receive count. For example, `[1 second, 13 hours, 1 second]` needs a durable scheduler and a `maxReceiveCount` of at least 2. Policy JSON is compared structurally rather than by whitespace or property order. Additional undeclared attributes/tags are allowed. `None` skips topology validation and provisioning, but startup still resolves queue names through `GetQueueUrl` because native sending needs URLs. Message-size limits are read during topology validation; without validation the SDK enforces the queue's configured limit.

## Receiving and durable settlement

Declare both source and dead-letter queues, then register a single-message handler:

```csharp
relayer.AmazonSqs("orders", sqs =>
{
    sqs.Options.Configure(options =>
    {
        options.Queues.Add(new CreateQueueRequest("orders"));
        options.Queues.Add(new CreateQueueRequest("orders-errors"));
    });
    sqs.Consume("orders", consumer =>
    {
        consumer.Handler<OrderHandler>();
        consumer.Concurrency(4);
        consumer.Options.ConfigureReceive = request =>
        {
            request.WaitTimeSeconds = 20;
            request.VisibilityTimeout = 120;
            request.MaxNumberOfMessages = 10; // Standard queues only; FIFO requires 1.
        };
        consumer.Failures(new FailureOptions
        {
            RedeliveryDelays = new[] { TimeSpan.FromSeconds(5) },
            DeadLetterEndpoint = "orders-errors",
        });
    });
});
```

Each worker owns one reusable native `ReceiveMessageRequest` and retains pooled UTF-8 body storage at its high-water mark. Native request configuration runs once per worker. Standard queues accept `MaxNumberOfMessages` from 1–10 (default 1); FIFO queues require 1. `QueueUrl`, all message/system attributes and a long poll of 1–20 seconds are required; invalid customization fails before receives start. A fixed `ReceiveRequestAttemptId` is rejected because it would replay old requests. `Concurrency` sets the worker count. Handlers remain single-message and execute serially within each worker. Batch handlers are not supported.

For a standard queue configured to receive multiple messages, the worker retains each borrowed delivery independently and waits until its handler and durable work finish before adding its receipt to native `DeleteMessageBatch`. It deletes completed receipts together after processing the returned batch. Each pipeline stays active until its own delete entry succeeds. A partial batch failure or missing result fails that delivery; successful entries complete normally. The SDK owns request retries; failed entries remain subject to native visibility and redelivery. Handler or failure-copy errors never enter the delete batch. Graceful shutdown drains every returned message and its deletion; an aborted drain leaves unprocessed deliveries undeleted. Set visibility to cover the entire batch, including serial handler work, retries and the final delete request. Holding up to ten delivery contexts per worker increases bounded startup storage; it adds no per-message adapter allocations.

FIFO ordering comes from SQS's invisible-message-group semantics, including across application instances. No group dispatcher is added. Set native visibility long enough for all durable work and in-process retries: this adapter does not renew visibility automatically, and native expiration can cause duplicate delivery and release the group. Graceful stop cancels polls while draining active work; the host's drain deadline cancels processing. An aborted drain requires a new host. The SDK owns request retries. Workers issue another poll after any exhausted native request failure, with cancellable exponential backoff from one to thirty seconds, reset by a successful poll. This also allows recovery after credentials, permissions or queue availability change; persistent configuration errors remain visible in warning logs.

`context.AmazonSqs()` returns a readonly `SqsMessageContext` view exposing the borrowed native `Message`, `IAmazonSQS`, queue URL and receive request ID. Keep these references within the handler lifetime. The view has no disposal API; the receiver owns and reuses the underlying delivery storage. Use the portable settlement feature for completion, retry or dead-letter decisions so settlement follows durable work.

Consumers use the default `ce_` CloudEvents headers. For a custom send binding such as `new HeaderNames("event_")`, set `consumer.Options.HeaderNames` to the same binding. The receiver uses it for envelope decoding and preserves those attributes in failure copies.

### Redelivery and dead-lettering

Visibility retries round positive delays up to whole seconds. Native visibility retries must fit strictly within the original receive request's remaining twelve-hour budget. Relayer measures that budget with monotonic time, including polling and SDK retries. Changing visibility never restarts it. A native retry that no longer fits fails without deleting the message.

For configured delays above 43,199 seconds, including exactly twelve hours, register a durable `IFailureScheduler` in the application's dependency injection container. Outbox registration alone leaves this requirement unmet, and the consumer fails to start without the scheduler. Relayer uses the scheduler to persist the copy for delivery to the source queue before deleting the original. For FIFO redelivery, the copy and its replay use the source queue's CloudEvents identity header for deduplication. A local CloudEvents send route supplies that header when declared. On a receive-only host, set `consumer.Options.HeaderNames` to the producer's binding to schedule and replay a retry with a custom prefix.

SQS visibility changes cannot update attributes. The binding combines `ApproximateReceiveCount - 1` with any redelivery count on a previous durable copy, so retry policies terminate without a process-local retry cache. Previously stored history is preserved; the eventual copy adds the current delivery's in-process failure history. Errors from earlier visibility deliveries cannot be reconstructed from SQS (contributor decision D67).

SQS redrive cannot attach application failure metadata. `Failure.DeadLetterEndpoint` is required and must name a separately declared queue of the same standard/FIFO kind. The existing failure pipeline sends a copy with original identity, body, attributes, trace context and group key, then deletes the source only after that send succeeds. Copy or settlement failures leave the source available for native redelivery; duplicates are possible. Reserve room within SQS's ten-attribute limit for up to four failure attributes, and within the destination's native message-size limit for error history. These limits apply to externally produced messages too. Error history is bounded and shortened to the declared destination size budget as described in [failure handling](../guides/failure-handling.md). Native attribute type bytes and properties applied by native callbacks still count toward SQS’s size limit. If the copy still cannot fit, Relayer retains the original and logs the failure; it preserves application attributes and the body. Configure native `RedrivePolicy` as the broker safety net for messages that cannot be copied; native redrive preserves the original attributes but cannot add failure metadata. Direct failure copies preserve native binary and typed attributes through a native callback. For scheduled redelivery, Relayer rejects copies that require a native callback and leaves the original available for redelivery. FIFO copies preserve the native deduplication identity when the source or destination envelope identity is missing, cannot be used as a native SQS deduplication token (1–128 ASCII alphanumeric or punctuation characters, without spaces), or is a native typed attribute rather than an exact `String` attribute. The copy preserves the original attributes, including a valid destination identity attribute when the source binding has no usable identity. That attribute cannot replace the native fallback. This fallback also requires a non-persistable native callback. Standard fair-queue copies preserve native `MessageGroupId` through the same non-persistable callback without requesting portable ordering. Text-only copies with both identities and no standard-queue group remain persistable.

### Start consumers later

Consumers start polling with the host by default. Set `AutoStartConsumers = false` to bring the host up first and start the long polls yourself through the transport's keyed `IConsumerControl`:

```csharp
using Microsoft.Extensions.DependencyInjection;
using Relayer.Transports;

sqs.Options.Configure(options => options.AutoStartConsumers = false);

// Later, when the application is ready to receive:
var consumers = app.Services.GetRequiredKeyedService<IConsumerControl>("orders");
await consumers.StartAsync(cancellationToken);
```

The host still resolves queue URLs, validates redrive policies and composes every consumer at startup, so the queues must exist when the host starts; sending works while the consumers are stopped. `StartAsync` fails before the host has started or after it stops. `StopAsync` cancels the long polls and drains in-flight work like host shutdown, so the same control pauses and resumes consumption; both calls do nothing when the consumers are already in that state. A stop whose token expires aborts the drain, and the consumers cannot start again until the host is recreated. Host shutdown stops consumers that the control started. While stopped, consumer [health](#health) reports them as unavailable.

If automatic or deferred consumer startup fails after polling begins, Relayer stops polling and drains active deliveries. Cancelling startup during rollback also cancels active handlers and returns the original startup error. Recreate the host after an aborted drain.

If the host shutdown deadline expires during deferred startup, Relayer cancels startup, stops any polls already in progress, and cancels active handlers. The deadline still applies while `ConfigureReceive` runs. When the callback returns, startup checks for cancellation before launching further workers.

Overlapping stop calls keep their cancellation state until they finish, even if another caller restarts or disposes the consumer after the workers drain.

## Health

Reference `Relayer.AmazonSqs.HealthChecks` and register before host startup:

```csharp
services.AddRelayerAmazonSqsHealthProviders("orders");
services.AddRelayerHealthChecks();
```

Both optional health contracts use the exact, case-sensitive transport name. Connectivity calls native `GetQueueUrl`
and `GetQueueAttributes` without sending, receiving or settling probe messages. Receiver snapshots distinguish idle
pollers from active handlers and report the oldest active worker's activation time. A busy healthy worker cannot hide
a stalled one. Receivers report unavailable state during startup and after stopping. A faulted worker reports failure.
Tracking is enabled only with this companion and adds no per-message allocation.

`SqsReceiveBindingBenchmarks` compares pooled receive-body preparation with raw-client UTF-8 preparation using `MemoryDiagnoser`. This isolates adapter work; it does not establish the end-to-end 5% default-JSON throughput budget.

## FIFO ordering and timers

Declare FIFO queues with both the `.fifo` suffix and the native `FifoQueue = "true"` attribute:

```csharp
options.Queues.Add(new CreateQueueRequest("orders.fifo")
{
    Attributes = new() { ["FifoQueue"] = "true" },
});
// Inside the transport declaration:
sqs.Send<OrderPlaced>("orders.fifo", route => route.OrderingKey(order => order.CustomerId));
```

The resolved ordering key becomes native `MessageGroupId`. The CloudEvents identity header selected by `SendRouteOptions.HeaderNames` (default `ce_id`) becomes `MessageDeduplicationId`, preserving native FIFO deduplication across retries and stored-message replay. Custom prefixes such as `new HeaderNames("event_")` are supported. All CloudEvents routes to the same FIFO queue must use the same identity header; conflicting bindings fail startup with an actionable error. Low-level replay uses the identity header from the queue's CloudEvents send routes, then the queue's consumer binding if there is no such route. With neither binding, it uses `ce_id`. Consumers on the same FIFO queue must agree on the identity header when it comes from the consumer binding. Low-level sends without an event ID need native deduplication configuration or an explicit native deduplication ID. FIFO sends require a message group; standard queues reject portable ordering requirements. Relayer adds no ordering layer.

For standard queues, a future `SendOptions.DeliverAt` becomes native `DelaySeconds`, rounded up to the next second so delivery is not early. Each batch request calculates its timers at submission, including when one send splits into several requests. A scheduled message already due at submission uses an explicit zero-second timer, overriding the queue's default delay. The outbox also uses a zero-second timer after it has waited for a scheduled send or retry to become due. Unscheduled sends without an outbox due time leave `DelaySeconds` unset, preserving the queue's native default delay.

Timers support at most 15 minutes. FIFO queues do not support per-message timers. Longer delays and FIFO scheduling require a configured [durable outbox](../guides/sending.md). Unsupported low-level scheduled sends fail explicitly.

## Wire format and native callbacks

CloudEvents attributes use string message attributes with the configured prefix (`ce_` by default). `content-type`, `traceparent` and `tracestate` remain native message attributes. SQS allows at most **10 attributes total**, including CloudEvents, tracing and application headers; exceeding the limit fails without discarding metadata. Serialized bodies must be valid UTF-8 text allowed by SQS. Invalid UTF-8 fails rather than replacing bytes. Binary serializers must supply an SQS-compatible text encoding themselves; Relayer does not silently base64-encode bodies.

```csharp
await sender.SendAsync(order, options => options.AmazonSqs(request =>
{
    request.MessageAttributes["priority"] = new MessageAttributeValue
    {
        DataType = "String",
        StringValue = "high",
    };
}));
```

Callbacks receive native `SendMessageRequest` for both single and batch sends. They run once per message before native admission. Original envelope headers, resolved destination, portable group key and scheduled time take precedence over conflicting callback values. The portable group key is decoded once and the same native string is restored after the callback. Do not retain borrowed request objects. Native callbacks cannot be persisted in the outbox.

## Batching and telemetry

Serialized transport batches use native `SendMessageBatch`, splitting at destination changes, 10 entries, or the aggregate 1 MiB byte limit, including attributes. Input order is preserved; native FIFO groups govern broker ordering. An individual message exceeding the resolved queue limit fails before native sending. Batches are not atomic: earlier batches or entries can succeed before a failure or cancellation.

Even HTTP 200 responses can contain failed entries. `AmazonSqsBatchException.Response` exposes the unmodified native successful/failed lists; `BatchOffset` identifies the native batch's start within its contiguous destination group. Entry IDs are zero-based indexes within that native batch. Successful entries are not automatically resent, and a partial failure stops submission of later sub-batches. Application retries must account for native partial delivery and FIFO deduplication semantics.

Native send attempts count actual entries in `messaging.client.sent.messages`; each SDK call records `messaging.client.operation.duration` through acknowledgement or failure, including partial batch failure. Preparation failures and empty batches record neither metric.

Send spans use `messaging.system = aws_sqs`, `aws.sqs.queue.url`, and the native response's `aws.request_id`. Native AWS exceptions retain their request IDs. Creation context stays in message attributes. SNS spans use `aws.sns`; receive/process spans use `aws_sqs`, `aws.sqs.queue.url` and the receive request ID. Successful completion uses `delete` settlement spans with the native delete request ID.

## Performance validation

`SqsSendBenchmarks` compares acknowledged single sends and 10-message batches with the raw AWS SDK using `MemoryDiagnoser`. Both paths reuse native requests and include text/header conversion. The benchmark creates and deletes a fresh standard queue per case. Use a test AWS account/region with create/delete rights, or set `RELAYER_SQS_SERVICE_URL` to LocalStack (test credentials, `us-east-1`):

```shell
dotnet run -c Release --project benchmarks/Relayer.Benchmarks -- --filter "*SqsSendBenchmarks*"
```

Native request, entry and attribute containers are pooled per concurrent operation. SQS requires a text body, so decoding serialized UTF-8 bytes allocates a native-body string on each send. Nonempty UTF-8 header values and group keys also allocate strings. Under the approved native-boundary allocation policy (D52), these required `SendMessageRequest.MessageBody`, `MessageAttributeValue.StringValue`, and `MessageGroupId` strings count as native-client cost when the equivalent raw-client preparation allocates at least as much. All other pipeline allocations remain subject to the zero-allocation requirement. The `DefaultJson` category compares `ISender.SendAsync` with raw SDK sending, including the same default JSON serialization and fresh CloudEvents metadata on both paths. `Single` and `Batch` retain the pre-serialized comparison. Benchmark coverage alone does not establish the 5% throughput budget.

`SqsBindingBenchmarks` isolates these conversions without network, SDK response, or serializer allocations. It writes the default binary-mode envelope with changing IDs/timestamps and a 256-byte body on each invocation, for both standard messages and FIFO messages with a UTF-8 group key. Run it with `--filter "*SqsBindingBenchmarks*"` to pin Relayer's additional allocation cost against equivalent raw-client preparation, independently of native request overhead. The earlier default-envelope probe reported 856 B/message on each path (0 additional B); that historical report is not a measurement of every payload or configuration.

## SNS topic publishing

Use the same package and sender API with an explicit SNS transport and native topic declarations. Configure SNS through `AmazonSnsTransportOptions.Sns`; SQS transport options configure only SQS and do not contain an SNS client configuration:

```csharp
relayer.AmazonSns("events", sns =>
{
    sns.Options.Configure(options =>
    {
        options.Sns = new Amazon.SimpleNotificationService.AmazonSimpleNotificationServiceConfig
        {
            RegionEndpoint = Amazon.RegionEndpoint.EUWest2,
        };
        options.Topics.Add(new Amazon.SimpleNotificationService.Model.CreateTopicRequest("orders"));
    });
    sns.Send<OrderPlaced>("orders");
});
```

Routes target declared topic **names**. The SDK's native topic paginator resolves topic ARNs at startup. `SnsClient` accepts a caller-owned `IAmazonSimpleNotificationService`, which Relayer never disposes. Otherwise `Sns` passes unchanged to the native client, using the AWS credential chain. Multiple named SQS and SNS transports can coexist.

SNS FIFO topics require a `.fifo` name and native `FifoTopic = "true"` attribute. Route ordering keys become `MessageGroupId`; the configured CloudEvents identity header becomes `MessageDeduplicationId`, including replay and custom header prefixes. All CloudEvents routes to the same FIFO topic need matching identity bindings. Without an envelope identity, configure native deduplication explicitly or use the topic's content-based deduplication. SNS provides no native scheduling or tombstones; use the durable outbox for future delivery.

`SendOptions.AmazonSns(Action<PublishRequest>)` exposes the native outgoing request for single sends and batch entries. For example, set `Subject` or native message attributes. Portable destination, group key and envelope headers take precedence; callbacks run once per message and must not retain borrowed requests. Plain topic publishing rejects `TargetArn`, `PhoneNumber` and `MessageStructure`, which would change destination or wire format. Bodies must be valid UTF-8. Raw SQS delivery accepts at most **10 attributes**, so the SNS adapter enforces that total including CloudEvents and tracing rather than dropping metadata.

SNS uses the topic's native [`MaximumMessageSize`](https://docs.aws.amazon.com/sns/latest/dg/large-message-payloads.html) for individual messages and the combined size of a batch, including attributes. The default is 256 KiB. Set the attribute on the broker or in a native `CreateTopicRequest` when provisioning a new topic. SNS supports values from 1 KiB to 1 MiB. Limits above 256 KiB require supported subscriptions, including SQS, and at most 100 subscriptions per topic.

Relayer reads the broker's setting at startup and when applying topology. Grant `sns:GetTopicAttributes` even when `TopologyMode.None` skips topology checks. Native `PublishBatch` splits at destination changes, 10 messages or the topic's aggregate byte limit. `AmazonSnsBatchException.Response` exposes the original native successful and failed entries, and `BatchOffset` identifies the native batch within its destination group. Successful entries are never automatically resent, and later batches stop after a failure. Native FIFO ordering and deduplication govern delivery.

A FIFO topic supports portable route ordering when every declared SQS subscriber uses a FIFO queue. A declared standard SQS subscriber delivers with best-effort ordering, so startup rejects routes that use `OrderingKey` for that topic. For native FIFO publishing to a mixed topology, set `PublishRequest.MessageGroupId` through `SendOptions.AmazonSns`. Relayer checks the declared subscriptions by topic name and does not discover undeclared subscribers for this check.

### SNS topics, subscriptions and queue policies

`Topics` contains native `CreateTopicRequest` declarations with attributes and tags. `Subscriptions` contains native `SubscribeRequest` declarations with explicit topic and queue ARNs, `Protocol = "sqs"`, and `Attributes["RawMessageDelivery"] = "true"`. Raw delivery preserves the serialized body and CloudEvents message attributes on the queue. Other subscription protocols and wrapped SNS notification bodies are rejected at startup.

For example, inside `sns.Options.Configure(options => ...)`, declare the queue and subscription with your actual region and account ARNs:

```csharp
var topicArn = "arn:aws:sns:eu-west-2:123456789012:orders";
var queueArn = "arn:aws:sqs:eu-west-2:123456789012:orders";
options.QueueTopology.Sqs = new Amazon.SQS.AmazonSQSConfig
{
    RegionEndpoint = Amazon.RegionEndpoint.EUWest2,
};
options.QueueTopology.Queues.Add(new Amazon.SQS.Model.CreateQueueRequest("orders")
{
    Attributes = new()
    {
        ["Policy"] = $$"""
            {"Version":"2012-10-17","Statement":[{"Effect":"Allow",
            "Principal":{"Service":"sns.amazonaws.com"},"Action":"sqs:SendMessage",
            "Resource":"{{queueArn}}","Condition":{"ArnEquals":{"aws:SourceArn":"{{topicArn}}"}}}]}
            """,
    },
});
options.Subscriptions.Add(new Amazon.SimpleNotificationService.Model.SubscribeRequest
{
    TopicArn = topicArn,
    Protocol = "sqs",
    Endpoint = queueArn,
    ReturnSubscriptionArn = true,
    Attributes = new() { ["RawMessageDelivery"] = "true" },
});
```

`QueueTopology` reuses SQS declarations and policy validation described above. Its optional `SqsClient` is caller-owned. Set its region/configuration independently from SNS. Policies are explicit infrastructure input; Relayer does not infer or broaden permissions. Queues managed elsewhere can omit `QueueTopology` and supply only the subscription ARNs, with the required queue permissions already provisioned.

Default `Validate` uses read APIs for topics, tags, subscriptions and optional queues, and reports missing resources or declared attribute drift. `Provision` creates only missing topics, queues and subscriptions with the native declarations. It never updates policies, subscription attributes, tags or existing resources; reconcile reported differences through infrastructure configuration. `None` skips topology checks; startup still resolves declared topics. Unconfirmed subscriptions are reported rather than treated as ready. There is no SNS receiver: consume from the subscribed queue with the SQS receiver above.

On a running SNS transport, Relayer refreshes the cached topic ARNs and size limits after validation or provisioning completes. If either operation is canceled or throws, the previous values stay cached. Each single send or batch group for one destination captures its ARN and size limit together. Later sends use the refreshed cache. Amazon SNS enforces the topic's current settings when it receives each request.

SNS send spans and native metrics use `messaging.system = aws.sns`, with `aws.sns.topic.arn` and native `aws.request_id` on sends. Duration covers each native request through acknowledgement or failure; partial batch failures remain failures, and native attempt counts include each submitted entry.

`SnsSendBenchmarks` compares acknowledged single and batch publishing against raw AWS requests with `MemoryDiagnoser`, including optional metrics. Set `RELAYER_SNS_SERVICE_URL` for LocalStack or use the native AWS credential chain in a test account. `SnsBindingBenchmarks` isolates default envelope/body conversion for standard and FIFO sends. Required native `PublishRequest.Message`, `MessageAttributeValue.StringValue` and UTF-8 `MessageGroupId` strings are native-client cost under D52; request, entry and attribute containers are reused. Its `DefaultJson` category compares `ISender.SendAsync` with raw SDK publishing of the same default-JSON model and fresh CloudEvents metadata. `Single` and `Batch` isolate pre-serialized sends. Local measurements are diagnostic; benchmark coverage alone does not establish the 5% throughput budget.

`SqsReceiveBatchBenchmarks` compares equivalent native receive/delete batches (2 and 10 messages) with retained adapter delivery storage and per-entry settlement. Cached native responses exclude SDK HTTP/serialization costs so the benchmark isolates adapter scheduling and binding; it does not establish end-to-end throughput against a live queue.

### Send pool retention

SQS and SNS each retain at most 256 idle send-state objects per transport. Concurrent sends above
the bound remain supported and allocate temporary overflow state on each burst. Native completion
still controls the lifetime of borrowed message memory; native request references are cleared before
retention or collection.
The bound limits object count, not bytes: retained state can keep storage sized for its largest batch.
There is no retention option.

`TransportPoolRetentionBenchmarks` measures warmed pool reuse at concurrency 1 and 256 and repeated
512-operation bursts with `MemoryDiagnoser`. Measurements isolate pooling from broker I/O and do not
establish whole-path throughput parity.
