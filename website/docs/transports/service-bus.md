---
title: Azure Service Bus
description: "Named native Service Bus senders, client ownership, sessions, scheduling and topology."
---

`Relayer.AzureServiceBus` implements named queue/topic sending on `Azure.Messaging.ServiceBus`. [Receive primitives](service-bus-receive.md) are available separately. Hosted receiver composition, session processor bindings, health adapters and full transport conformance remain planned in [#130](https://github.com/thomhurst/Relayer/issues/130).

## Register sending

```csharp
using Azure.Messaging.ServiceBus.Administration;
using Relayer;
using Relayer.AzureServiceBus;

services.AddRelayer(relayer =>
{
    relayer.AddMessageTypesFromThisAssembly();
    relayer.UseJsonSerializer(AppJsonContext.Default);
    relayer.ServiceBus("orders", bus =>
    {
        bus.Options.Configure(options =>
        {
            options.ConnectionString = connectionString;
            options.Queues.Add(new CreateQueueOptions("orders"));
        });
        bus.Send<OrderPlaced>("orders");
    });
});

await sender.SendAsync(new OrderPlaced(customerId));
```

Messages need a stable `[Message]` identity and a source-generated JSON context. Start the host before sending. Service Bus routes default to the AMQP `cloudEvents:` header binding; creation context uses `traceparent` and `tracestate`. Named options support the [Options pattern](../guides/configuration.md). Set `ClientOptions` to the native `ServiceBusClientOptions`; Relayer passes it unchanged to the SDK. Relayer owns connection-string clients and disposes them with the provider. Alternatively, set `Client` to an existing `ServiceBusClient`, including a client constructed with `TokenCredential`. Relayer disposes its cached senders and leaves that client open. Configure exactly one of `ConnectionString` or `Client`; do not mutate captured native options after startup.

Declare destinations with native `CreateQueueOptions` or `CreateTopicOptions` in `Queues` or `Topics`. Every send destination needs exactly one declaration. Native `CreateSubscriptionOptions` in `Subscriptions` declare subscriptions of a declared topic, independently of receiver composition. Consumer registrations currently fail with an actionable error rather than starting no receiver.

For single and batch sends, the serializer's `content-type` header maps to native `ServiceBusMessage.ContentType`, as required by the CloudEvents AMQP binary binding. It is not copied to application properties. The serializer's media type takes precedence over a native callback's `ContentType`; when no `content-type` header is present, a callback may set the native property.

## Topology and permissions

`Validate` is the default. It creates an empty native message batch to open each declared destination's authenticated send link, then disposes the batch and sender. No probe message is sent. It peeks declared subscriptions with a native receiver; peeking does not lock, consume or settle a message. Send destinations need Send rights; subscription checks need Listen rights. Validate never constructs or calls the administration client, even when one is supplied.

This verifies endpoint accessibility and existence. It cannot compare entity settings or distinguish a queue from a topic through a send link. [Service Bus requires Manage rights to read entity descriptions](https://learn.microsoft.com/en-us/azure/service-bus-messaging/service-bus-sas#rights-required-for-service-bus-operations). Native entity options specify what Provision should create and compare. Validate reports declared `RequiresSession = true` as unresolved and fails startup; it never infers session support from a name or silently grants ordering capabilities. Manage permission is never an automatic fallback.

Use `bus.TopologyMode = TopologyMode.Provision` explicitly to create missing entities through the native administration APIs and verify their properties. Existing queues, topics and subscriptions are never updated or deleted. Remaining differences name the entity, property, expected value and actual value. Relayer handles a concurrent create by reading and comparing the winning entity. Other SDK failures and cancellation propagate.

Provision uses `AdministrationClient` when supplied. With an owned connection-string client it otherwise creates a native administration client using that connection string. With a caller-owned messaging client, supply an administration client explicitly; Relayer cannot extract credentials from the messaging client. The Service Bus emulator has a separate management endpoint: use its HTTP connection string for the administration client. Native administration clients are caller-owned and the SDK does not require disposal.

`TopologyMode.None` skips all checks and writes. Native scheduling and batching remain available. Ordering capabilities stay unresolved in Validate and None, so portable `OrderingKey` requires Provision with verified session declarations. No transaction support, receive capabilities or namespace-dependent message-size limit is advertised by this send-only adapter.

## Sessions, scheduling and native callbacks

For native ordered queue sends, declare `CreateQueueOptions("orders") { RequiresSession = true }`, choose Provision and configure `route.OrderingKey(order => order.CustomerId)`. For a topic, declare its session-enabled subscriptions; every declared subscription must require sessions before the topic advertises ordering. Relayer maps the resolved ordering key to native `SessionId`. Broker sessions and the receiver determine ordering; Relayer adds no ordering layer.

`SendOptions.DeliverAt` becomes native `ScheduledEnqueueTime`. Native callbacks run once for each message before native admission:

```csharp
await sender.SendAsync(order, options => options.ServiceBus(message =>
{
    message.Subject = "priority";
}));
```

Callbacks receive the SDK's own `ServiceBusMessage` and may set session IDs, partition keys and other native properties. Do not retain the borrowed native message. Original CloudEvents and creation-context application properties take precedence over callback changes. Native callbacks cannot be persisted in the outbox.

Serialized transport batches use `CreateMessageBatchAsync`, `TryAddMessage` and `SendMessagesAsync`. Relayer splits batches at the native byte limit, destination changes, and native session/partition key changes, preserving input order. A message too large for an empty native batch fails with `MessageSizeExceeded`; use individual sending when a namespace permits single messages larger than its batch limit. All calls pass cancellation to the SDK. Batches are not atomic: cancellation or failure can follow successful earlier batches.

Relayer emits send spans with `messaging.system = servicebus`; enabling the Azure SDK's activity source adds native spans. The original `traceparent` and `tracestate` application properties remain creation context.

## Performance validation

`ServiceBusSendBenchmarks` compares acknowledged single sends and 32-message batches against the raw SDK, including native message and header preparation, with `MemoryDiagnoser`. Its `DefaultJson` category also compares the real `ISender.SendAsync` path with direct SDK sending: both serialize the same model using the registered default JSON serializer and write a fresh CloudEvents envelope with changing IDs and timestamps. The raw baseline writes text directly into native application properties; Relayer also includes routing, its reusable UTF-8 envelope storage and send pipeline. The `Single` and `Batch` categories use pre-serialized bodies and isolate the transport adapter.

Set `RELAYER_SERVICEBUS_CONNECTION_STRING` to a test namespace with Manage rights. The benchmark creates and deletes a fresh non-session queue per case, so earlier cases do not change the next case's queue depth. For the emulator, also set `RELAYER_SERVICEBUS_ADMIN_CONNECTION_STRING` to its HTTP management connection string:

```shell
dotnet run -c Release --project benchmarks/Relayer.Benchmarks -- --filter "*ServiceBusSendBenchmarks*"
```

Native message containers and batch lease lists are pooled. Serialized body memory is borrowed without a body copy, and batch leases remain live through native send completion and batch disposal. Text application properties and session IDs require SDK strings. Relayer materializes a string for every nonempty UTF-8 `WireString` value before calling the SDK. The default send envelope stores its ID, type, source, timestamp and specification version as UTF-8, so all five conversions allocate even after warming the message pool. Tracing can add further conversions. These allocations belong to Relayer; they are not excluded as native SDK or serializer allocations. This adapter does not currently meet the zero-allocation target.

The registered serializer's content type is already a string, so the normal `content-type` mapping reuses that string. Explicit UTF-8 content types require conversion. [The upstream API request](https://github.com/Azure/azure-sdk-for-net/issues/64450) tracks borrowed UTF-8 **text** support: passing bytes as application-property values currently produces AMQP binary values, which would change the CloudEvents wire types. Relayer keeps the supported string conversion and native AMQP encoder until a compatible native API exists; it does not cache changing IDs or mutate immutable strings.

`ServiceBusSendAllocationTests` limits warmed preparation allocations to the existing string conversions, with changing IDs/timestamps, stored creation context and alternating registered serializer content types. It also checks that reusing caller buffers after lease release cannot change native snapshots and that clearing a pooled message detaches the borrowed body. This regression ceiling records the current limitation; it does not waive the zero-allocation target tracked in [#295](https://github.com/thomhurst/Relayer/issues/295).

`ServiceBusHeaderBenchmarks` isolates these conversions from SDK/network and serializer allocations. It writes the actual envelope with changing IDs and timestamps on every invocation, for both a single route and alternating routes, and compares direct native property assignment with the adapter. The raw baseline performs the same string conversions; matching that baseline does not waive Relayer's own allocation budget. The `DefaultJson` acknowledged comparison can measure the full pipeline's overhead, but adding a benchmark does not establish the 5% throughput budget: that requires measurements against the same native client and environment. Receive performance and full transport conformance remain planned.
