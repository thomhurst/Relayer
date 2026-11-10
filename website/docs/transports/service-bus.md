---
title: Azure Service Bus
description: "Named native Service Bus senders and processors, sessions, settlement, health and topology."
---

`Relayer.AzureServiceBus` implements named queue/topic sending and native queue/subscription processors on `Azure.Messaging.ServiceBus`. Native session processors preserve Service Bus session ordering and state. [Receive primitives](service-bus-receive.md) remain available for applications that own their processor lifecycle.

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

Declare destinations with native `CreateQueueOptions` or `CreateTopicOptions` in `Queues` or `Topics`. Every send destination needs exactly one declaration. Native `CreateSubscriptionOptions` in `Subscriptions` declare subscriptions of a declared topic. Every consumer needs a matching native queue or topic/subscription declaration.

For single and batch sends, the serializer's `content-type` header maps to native `ServiceBusMessage.ContentType`, as required by the CloudEvents AMQP binary binding. It is not copied to application properties. The serializer's media type takes precedence over a native callback's `ContentType`; when no `content-type` header is present, a callback may set the native property.

## Topology and permissions

`Validate` is the default. Send destinations use an empty native message batch to open an authenticated send link, then dispose the batch and sender. No probe message is sent. Receive-only queues and declared subscriptions use native peek operations, which do not lock, consume or settle a message. Receive-only topic consumers validate their subscriptions without opening a topic send link. Send destinations need Send rights; receive checks need Listen rights. Validate never constructs or calls the administration client, even when one is supplied.

This verifies endpoint accessibility and existence. It cannot compare entity settings or distinguish a queue from a topic through a send link. [Service Bus requires Manage rights to read entity descriptions](https://learn.microsoft.com/en-us/azure/service-bus-messaging/service-bus-sas#rights-required-for-service-bus-operations). Native entity options specify what Provision should create and compare. Unless explicitly trusted with `TrustedSessionEntities` (below), Validate reports declared `RequiresSession = true` as unresolved and fails startup. It never infers session support from a name. Manage permission is never an automatic fallback.

Use `bus.TopologyMode = TopologyMode.Provision` explicitly to create missing entities through the native administration APIs and verify their properties. Existing queues, topics and subscriptions are never updated or deleted. Remaining differences name the entity, property, expected value and actual value. Relayer handles a concurrent create by reading and comparing the winning entity. Other SDK failures and cancellation propagate.

Provision uses `AdministrationClient` when supplied. With an owned connection-string client it otherwise creates a native administration client using that connection string. With a caller-owned messaging client, supply an administration client explicitly; Relayer cannot extract credentials from the messaging client. The Service Bus emulator has a separate management endpoint: use its HTTP connection string for the administration client. Native administration clients are caller-owned and the SDK does not require disposal.

`TopologyMode.None` skips all checks and writes. Native scheduling and batching remain available. Portable `OrderingKey` requires either Provision with verified session declarations or explicit trust in Validate and None. Consumers support native immediate abandon and dead-lettering. No transaction support or namespace-dependent message-size limit is advertised.

## Register receiving

Add a consumer to the same named transport declaration:

```csharp
bus.Consume("orders", consumer => consumer.Handler<OrderPlacedHandler>());
```

Relayer composes generated handlers, default JSON serialization, the core failure pipeline, process telemetry and native settlement before starting the processor. The default native `ServiceBusProcessorOptions` uses `PeekLock`, one concurrent callback and `AutoCompleteMessages = false`. Successful handling completes the message only after durable work and asynchronous scope disposal. Poison messages go to the native dead-letter queue without stopping healthy messages.

Pass native options through `consumer.Options.ProcessorOptions`. Set `AutoCompleteMessages = false` on replacement options. `ReceiveAndDelete`, auto-complete, undeclared entities and batch handlers fail at startup. The portable concurrency declaration must match the native concurrency; Relayer never rewrites native options:

```csharp
bus.Consume("orders", consumer =>
{
    consumer.Options.ProcessorOptions = new ServiceBusProcessorOptions
    {
        AutoCompleteMessages = false,
        MaxConcurrentCalls = 8,
        PrefetchCount = 32,
    };
    consumer.Handler<OrderPlacedHandler>().Concurrency(8);
});
```

For a topic subscription, consume the topic name and set `consumer.Options.Subscription` to the declared subscription name. Native processors own delivery, prefetch, lock renewal and reconnect. Shutdown stops new receives and drains callbacks through settlement; the host's shutdown cancellation token bounds the drain. A stopped processor can restart on the same host services.

The SDK cancels its callback token as soon as processor shutdown starts. The composed pipeline uses `context.CancellationToken` for durable work and settlement; Relayer cancels it only when the host's drain deadline expires or the adapter is disposed. The original callback and its native token remain unchanged. Use the context token when draining application work.

Configure the common `FailureOptions` through `consumer.Options.Failure`. In-process retry delays and immediate durable redelivery use the core policy and native abandon. Service Bus has no delayed abandon operation: positive `RedeliveryDelays` fail at startup rather than broadcasting a retry copy to every topic subscription. Delayed redelivery requires a separately designed durable executor; it is not implemented by this adapter.

Inside a handler, `context.ServiceBus()` exposes the unchanged received message and native callback. `CompleteAsync`, `AbandonAsync`, `DeferAsync` and `DeadLetterAsync` call the SDK, use the context cancellation token and suppress automatic settlement only after success. Native defer retains the message for later retrieval by sequence number. Persist that sequence number before deferring. See [manual native settlement](service-bus-receive.md).

## Session processors

Declare `RequiresSession = true` on the native queue or subscription and supply `SessionProcessorOptions` to select `ServiceBusSessionProcessor`:

```csharp
bus.Consume("customer-orders", consumer =>
{
    consumer.Options.SessionProcessorOptions = new ServiceBusSessionProcessorOptions
    {
        AutoCompleteMessages = false,
        MaxConcurrentSessions = 8,
        MaxConcurrentCallsPerSession = 1,
    };
    consumer.Handler<OrderPlacedHandler>().Concurrency(8);
});
```

The common concurrency must equal `MaxConcurrentSessions * MaxConcurrentCallsPerSession`. Use one callback per session when the handler depends on ordered completion. Relayer passes these native options unchanged and adds no ordering layer. `context.ServiceBus().SessionCallback` exposes the original `ProcessSessionMessageEventArgs`, including native session state and session lock operations. `Callback` is for non-session deliveries; using it on a session delivery fails with an actionable error. The common native settlement helpers work for both processor kinds.

## Receive telemetry

Each callback has a `process` span. Each actual completion, abandon, defer or dead-letter operation has a child settle span, including manual helper settlement. Spans use `messaging.system = servicebus`, subscription name, native delivery count, enqueued time and disposition status. The SDK's messaging `ActivitySource` is experimental and requires its own opt-in. Set
`AZURE_EXPERIMENTAL_ENABLE_ACTIVITY_SOURCE=true` before process startup and subscribe to
`Azure.Messaging.ServiceBus.*` to add native spans beneath the active Relayer settlement operation.
Relayer does not mutate that process switch. See the [Azure SDK tracing sample](https://github.com/Azure/azure-sdk-for-net/blob/main/sdk/servicebus/Azure.Messaging.ServiceBus/samples/Sample19_DistributedTracing.md). Original `traceparent` and `tracestate` application properties remain creation context.

## Health

Install `Relayer.AzureServiceBus.HealthChecks` and register the exact, case-sensitive transport key before host startup:

```csharp
using Relayer.AzureServiceBus;
using Relayer.HealthChecks;

services.AddRelayerServiceBusHealthProviders("orders");
services.AddRelayerHealthChecks();
```

The companion registers `ITransportHealthProbe` and `IConsumerHealthProvider` under that key. Connectivity uses native read-only operations and never sends or consumes a probe message. Consumer snapshots belong to each exact declaration. An idle consumer has no active callback; an active consumer reports its oldest active worker's progress time, so a busy worker cannot hide a stalled sibling. Stopped or unavailable receivers do not report a false healthy idle state. Configure the common health check stall threshold for your workload.

The Testcontainers suite uses Service Bus emulator 2.0.0 with SQL Server. It covers queues, topic subscriptions, session ordering and manual settlement, native retry/dead-lettering, scheduled sends, creation context, drain/restart, read-only health and connection recovery. Immediate native redelivery passes; positive delayed redelivery is an explicit capability skip. Emulator results do not establish production throughput, Azure identity/RBAC behavior, transaction support or service tier quotas.

Emulator entities created through the administration endpoint do not survive an emulator container restart. Recovery tests declare their owned endpoints in the emulator's startup configuration so the same client and processors reconnect to the same configured endpoint names. This test setup does not add topology persistence to the adapter or imply that a managed namespace loses entities on restart.

## Sessions, scheduling and native callbacks

For native ordered queue sends, declare `CreateQueueOptions("orders") { RequiresSession = true }`, choose Provision (or explicitly trust the entity below) and configure `route.OrderingKey(order => order.CustomerId)`. For a topic, declare its session-enabled subscriptions; every declared subscription must require sessions before the topic advertises ordering. Relayer maps the resolved ordering key to native `SessionId`. Broker sessions and the receiver determine ordering; Relayer adds no ordering layer.

For externally provisioned topology and a runtime identity without Manage rights, explicitly trust each native session declaration:

```csharp
bus.Options.Configure(options =>
{
    options.Queues.Add(new CreateQueueOptions("orders") { RequiresSession = true });
    options.TrustedSessionEntities.Add("orders");
});
bus.Send<Order>("orders", route => route.OrderingKey(order => order.CustomerId));
```

This works with the default `TopologyMode.Validate` and with `None`. The queue advertises ordering by key, so ordered outbox sends retain their key and receive lanes through the existing outbox integration. Validate still opens its Send link without an administration call. For a topic, set `RequiresSession = true` on **every declared subscription** and trust each full path, for example `options.TrustedSessionEntities.Add("events/Subscriptions/audit")`. A send-only application does not peek trusted subscriptions; a declared consumer still needs Listen rights and validates its receive link. Undeclared subscriptions cannot be discovered with Send rights, so the application owns the completeness of these declarations.

Trust is an assertion, not broker configuration or verification. Names must match session-enabled native queue or subscription declarations. Provision always compares the actual `RequiresSession` setting and reports drift, even for trusted entities. Without trust, Validate/None retain the unresolved-ordering error. A broker rejects a message without `SessionId` sent to a session-enabled queue or subscription; a message **with** `SessionId` is accepted by a non-session entity, so a false assertion can silently lose ordering. Topic fan-out follows each subscription's native behavior. Keep infrastructure definitions and declarations aligned; send-only startup cannot detect this mismatch.


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

Set `RELAYER_SERVICEBUS_CONNECTION_STRING` to a test namespace with Manage rights. The benchmark creates and deletes a fresh non-session queue per case, so earlier cases do not change the next case's queue depth. Hosted namespaces use the SDK's default queue size. For the emulator, also set `RELAYER_SERVICEBUS_ADMIN_CONNECTION_STRING` to its HTTP management connection string, including `UseDevelopmentEmulator=true`; this selects the emulator's 100 MB queue size:

```shell
dotnet run -c Release --project benchmarks/Relayer.Benchmarks -- --filter "*ServiceBusSendBenchmarks*"
```

Native message containers and batch lease lists are pooled. Serialized body memory is borrowed without a body copy, and batch leases remain live through native send completion and batch disposal. Text application properties and session IDs require SDK strings. Relayer materializes a string for every nonempty UTF-8 `WireString` value before calling the SDK. The default send envelope stores its ID, type, source, timestamp and specification version as UTF-8, so all five conversions allocate even after warming the message pool. Tracing can add further conversions. D52 classifies strings required by the native SDK as native-client cost when the raw-client comparison allocates at least as much. Relayer must still add zero pipeline allocations beyond native-required objects and application lifetime costs.

The registered serializer's content type is already a string, so the normal `content-type` mapping reuses that string. Explicit UTF-8 content types require conversion. [The upstream API request](https://github.com/Azure/azure-sdk-for-net/issues/64450) tracks borrowed UTF-8 **text** support: passing bytes as application-property values currently produces AMQP binary values, which would change the CloudEvents wire types. Relayer keeps the supported string conversion and native AMQP encoder until a compatible native API exists; it does not cache changing IDs or mutate immutable strings.

`ServiceBusSendAllocationTests` limits warmed preparation allocations to the existing string conversions, with changing IDs/timestamps, stored creation context and alternating registered serializer content types. It also checks that reusing caller buffers after lease release cannot change native snapshots and that clearing a pooled message detaches the borrowed body. This regression ceiling pins the native-required conversions tracked in [#295](https://github.com/thomhurst/Relayer/issues/295) under D52.

`ServiceBusHeaderBenchmarks` isolates these conversions from SDK/network and serializer allocations. It writes the actual envelope with changing IDs and timestamps on every invocation, for both a single route and alternating routes, and compares direct native property assignment with the adapter. The raw baseline performs the same string conversions; D52 uses that comparison to attribute native-required objects while retaining the pipeline budget. The `DefaultJson` acknowledged comparison can measure the full pipeline's overhead, but adding a benchmark does not establish the 5% throughput budget: that requires measurements against the same native client and environment. Receive benchmarks and transport conformance tests also cover native processors.

`ServiceBusReceiveBenchmarks` compares raw SDK processor callbacks with the composed Relayer receive pipeline, using the same source-generated default JSON and native completion. Each iteration seeds 128 messages outside the timed region, then measures processor start and handler progress for both cases. Both signal progress before native completion; iteration cleanup drains the final settlement and stops the processor outside timing. This excludes at most the last settlement from each measured workload. Both clients use the same three-second native try timeout. It uses `MemoryDiagnoser` and the same namespace environment variables as sending. Run `--filter "*ServiceBusReceiveBenchmarks*"`; use an isolated test namespace. Startup, the final settlement boundary and emulator latency limit its use as a steady-state comparison; these numbers do not establish the production budget.

## Receive allocations

The native processor callback must return a single-use `Task`. When processing suspends, Relayer's
`ValueTask.AsTask()` bridge allocates 80 B per delivery in the measured .NET 10 arm64 configuration;
a raw asynchronous SDK callback allocates 128 B. D52 counts this required object as native-client cost.
Health tracking uses a pooled frame and adds no allocation.

Default scoped handlers also allocate their Microsoft DI scope, service cache and handler instance
(measured at 336 B + 24 B). These are application lifetime costs under D52. Singleton-only consumers
open a delivery scope only when `context.Services` is read, so handlers that do not need that scope
avoid its allocation.

The retained measurements show Relayer adds 288 B per delivery over the raw SDK callback for a scoped
handler, 305 B with eight concurrent workers, and -48 B for a singleton handler. These totals include
the native-required Task and application lifetime costs above; they are diagnostic measurements,
not universal runtime sizes. `ServiceBusReceiveBenchmarks` pins the raw-client comparison, and
`FailureConsumerScopeBenchmarks` and the consume-pipeline allocation tests pin scope behavior.
