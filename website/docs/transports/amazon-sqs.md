---
title: Amazon SQS
description: "Named native SQS sending, FIFO groups, timers, batch failures and queue topology."
---

`Relayer.AmazonSqs` implements native SQS queue sending with AWS SDK v4. Receiver composition and full LocalStack transport conformance remain planned in [#224](https://github.com/thomhurst/Relayer/issues/224), SNS topics/subscriptions in [#225](https://github.com/thomhurst/Relayer/issues/225), and connectivity/liveness health adapters with receivers in [#224](https://github.com/thomhurst/Relayer/issues/224). Consumer declarations fail at startup until receivers are implemented.

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
    relayer.UseJsonSerializer(AppJsonContext.Default);
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

Messages need a stable `[Message]` identity and a source-generated JSON context. Start the host before sending. Every route destination must match a declared **queue name**, not a URL. Relayer resolves native queue URLs at startup. Named options support the [Options pattern](../guides/configuration.md).

The native `AmazonSQSConfig` passes unchanged to an owned client, which uses the AWS credential chain. Alternatively, set `SqsClient` to an existing `IAmazonSQS`; its configuration and credentials stay caller-owned, and Relayer never disposes it. Do not mutate captured configuration after startup. `Sns` remains reserved for the planned SNS adapter.

## Queue topology

Declare queues with native `CreateQueueRequest` objects in `Queues`, including native attributes and tags. The default `Validate` mode calls only read APIs: `GetQueueUrl`, `GetQueueAttributes`, and, when tags are declared, `ListQueueTags`. It sends no probe message and requires no create, update or delete permission. Missing queues and mismatched declared settings fail startup with a topology report.

`Provision` creates missing queues with the original request and reports drift on existing queues. It never modifies or deletes existing queues. Declare native `RedrivePolicy` and related queue settings in `Attributes`; receiver-driven dead-letter handling remains planned. Policy JSON is compared structurally rather than by whitespace or property order. Additional undeclared attributes/tags are allowed. `None` skips topology validation and provisioning, but startup still resolves queue names through `GetQueueUrl` because native sending needs URLs. Message-size limits are read during topology validation; without validation the SDK enforces the queue's configured limit.

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

The resolved ordering key becomes native `MessageGroupId`. The CloudEvents identity header selected by `SendRouteOptions.HeaderNames` (default `ce_id`) becomes `MessageDeduplicationId`, preserving native FIFO deduplication across retries and stored-message replay. Custom prefixes such as `new HeaderNames("event_")` are supported. All CloudEvents routes to the same FIFO queue must use the same identity header; conflicting bindings fail startup with an actionable error. Low-level replay uses that queue's configured identity header, or `ce_id` when no CloudEvents route declares one. Low-level sends without an event ID need native deduplication configuration or an explicit native deduplication ID. FIFO sends require a message group; standard queues reject portable ordering requirements. Relayer adds no ordering layer.

For standard queues, a future `SendOptions.DeliverAt` becomes native `DelaySeconds`, rounded up to the next second so delivery is not early. Timers support at most 15 minutes. FIFO queues do not support per-message timers. Longer delays and FIFO scheduling require a configured [durable outbox](../guides/sending.md); unsupported low-level scheduled sends fail explicitly. Unscheduled sends leave `DelaySeconds` unset, preserving the queue's native default delay.

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

Callbacks receive native `SendMessageRequest` for both single and batch sends. They run once per message before native admission. Original envelope headers, resolved destination, portable group key and scheduled time take precedence over conflicting callback values. Do not retain borrowed request objects. Native callbacks cannot be persisted in the outbox.

## Batching and telemetry

Serialized transport batches use native `SendMessageBatch`, splitting at destination changes, 10 entries, or the aggregate 1 MiB byte limit, including attributes. Input order is preserved; native FIFO groups govern broker ordering. An individual message exceeding the resolved queue limit fails before native sending. Batches are not atomic: earlier batches or entries can succeed before a failure or cancellation.

Even HTTP 200 responses can contain failed entries. `AmazonSqsBatchException.Response` exposes the unmodified native successful/failed lists; `BatchOffset` identifies the native batch's start within its contiguous destination group. Entry IDs are zero-based indexes within that native batch. Successful entries are not automatically resent, and a partial failure stops submission of later sub-batches. Application retries must account for native partial delivery and FIFO deduplication semantics.

Native send attempts count actual entries in `messaging.client.sent.messages`; each SDK call records `messaging.client.operation.duration` through acknowledgement or failure, including partial batch failure. Preparation failures and empty batches record neither metric.

Send spans use `messaging.system = aws_sqs`, `aws.sqs.queue.url`, and the native response's `aws.request_id`. Native AWS exceptions retain their request IDs. Creation context stays in message attributes. SNS telemetry and SQS `delete` settlement spans arrive with their respective adapters.

## Performance validation

`SqsSendBenchmarks` compares acknowledged single sends and 10-message batches with the raw AWS SDK using `MemoryDiagnoser`. Both paths reuse native requests and include text/header conversion. The benchmark creates and deletes a fresh standard queue per case. Use a test AWS account/region with create/delete rights, or set `RELAYER_SQS_SERVICE_URL` to LocalStack (test credentials, `us-east-1`):

```shell
dotnet run -c Release --project benchmarks/Relayer.Benchmarks -- --filter "*SqsSendBenchmarks*"
```

Native request, entry and attribute containers are pooled per concurrent operation. SQS requires a text body, so decoding serialized UTF-8 bytes allocates a native-body string on each send. Nonempty UTF-8 header values and group keys also allocate strings. Under the approved native-boundary allocation policy (D52), these required `SendMessageRequest.MessageBody`, `MessageAttributeValue.StringValue`, and `MessageGroupId` strings count as native-client cost when the equivalent raw-client preparation allocates at least as much. All other pipeline allocations remain subject to the zero-allocation requirement. The 5% throughput budget is unchanged; the serialized-send benchmark does not establish the full default-JSON pipeline budget.

`SqsBindingBenchmarks` isolates these conversions without network, SDK response, or serializer allocations. It writes the default binary-mode envelope with changing IDs/timestamps and a 256-byte body on each invocation. Run it with `--filter "*SqsBindingBenchmarks*"` to pin Relayer's additional allocation cost against equivalent raw-client preparation, independently of native request overhead. The earlier default-envelope probe reported 856 B/message on each path (0 additional B); that historical report is not a measurement of every payload or configuration.
