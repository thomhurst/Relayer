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
    message.ContentType = "application/json";
}));
```

Callbacks receive the SDK's own `ServiceBusMessage` and may set session IDs, partition keys and other native properties. Do not retain the borrowed native message. Original CloudEvents and creation-context application properties take precedence over callback changes. Native callbacks cannot be persisted in the outbox.

Serialized transport batches use `CreateMessageBatchAsync`, `TryAddMessage` and `SendMessagesAsync`. Relayer splits batches at the native byte limit, destination changes, and native session/partition key changes, preserving input order. A message too large for an empty native batch fails with `MessageSizeExceeded`; use individual sending when a namespace permits single messages larger than its batch limit. All calls pass cancellation to the SDK. Batches are not atomic: cancellation or failure can follow successful earlier batches.

Relayer emits send spans with `messaging.system = servicebus`; enabling the Azure SDK's activity source adds native spans. The original `traceparent` and `tracestate` application properties remain creation context.

## Performance validation

`ServiceBusSendBenchmarks` compares acknowledged single sends and 32-message batches against the raw SDK, including native message and header preparation, with `MemoryDiagnoser`. Set `RELAYER_SERVICEBUS_CONNECTION_STRING` to a test namespace with Manage rights. The benchmark creates and deletes a fresh non-session queue per case, so earlier cases do not change the next case's queue depth. For the emulator, also set `RELAYER_SERVICEBUS_ADMIN_CONNECTION_STRING` to its HTTP management connection string:

```shell
dotnet run -c Release --project benchmarks/Relayer.Benchmarks -- --filter "*ServiceBusSendBenchmarks*"
```

Native message containers and batch lease lists are pooled. Serialized body memory is borrowed without a body copy, and batch leases remain live through native send completion and batch disposal. Text application properties and session IDs require SDK strings; UTF-8 `WireString` values require native string materialization. The raw baseline performs the same conversion. This diagnostic comparison does not establish the full default-JSON pipeline's 5% throughput budget. Receive performance and full transport conformance remain planned.
