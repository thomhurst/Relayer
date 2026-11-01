---
title: Configuration
description: "Declare transports, routes and consumers, bind native options, and understand what Relayer validates at startup."
---

`AddRelayer` is the single entry point. Inside it you register message types and handlers, then declare each transport with its routes and consumers. Transport packages wrap `AddTransport` in extensions such as `AddKafka` or `AddAzureServiceBus` and supply their native option types; until they ship, the core surface below is what you call.

```csharp
var services = new ServiceCollection();

services.AddRelayer(relayer =>
{
    relayer.AddMessageTypesFromThisAssembly();
    relayer.AddTransport<BrokerOptions, RouteOptions, ConsumerOptions>("events", broker =>
    {
        broker.Options.Bind(configuration.GetSection("Events"))
            .Validate(o => !string.IsNullOrWhiteSpace(o.Server), "Server is required.");

        broker.Send<OrderPlaced>("orders.placed", route => route.OrderingKey(m => m.OrderId));
        broker.Consume("orders.placed", consumer =>
        {
            consumer.Options.Group = "billing";
            consumer.Handler<OrderPlacedHandler>().Concurrency(8)
                .Batch(maxSize: 10, maxWait: TimeSpan.FromMilliseconds(20));
        });
    });
    relayer.TopologyMode = TopologyMode.Validate;
});

// Example option types; a transport package supplies its actual native option types.
public sealed class BrokerOptions { public string Server { get; set; } = ""; }
public sealed class RouteOptions { }
public sealed class ConsumerOptions { public string Group { get; set; } = ""; }
```

`Send<T>` declares a destination. Publishing versus sending is native routing (a topic or exchange fans out, a queue does not), not a separate API. An ordering selector derives the native partition key, session ID or message group ID. `string` and `IUtf8SpanFormattable` values such as `Guid` stay typed, so a scalar key is formatted without boxing.

Transport names are case-sensitive and unique across all transport kinds. Several transports can use the same option type with independent named configuration; read them through `IOptionsMonitor<BrokerOptions>.Get("events")`. Call `AddRelayer` once per service collection; a second call throws.

## Concurrency and batching

`Concurrency` defaults to 1. `BatchSize` defaults to 1, which disables batching; `BatchWait` defaults to 100 ms. Transports honor these with native behavior and the core scheduler where applicable, and validate their native limits before starting. Declaring them never installs an emulated batching or ordering feature.

## Startup and reload


The core validator is inserted before transport hosted services and checks all transport names, destinations, stable message registrations, duplicate routes, handler presence, positive concurrency and batching settings, topology enum values, and each transport's named Options validators. Malformed bound values and Options validation failures are included in the same error list. Options validation failures during adapter construction also preserve declaration errors. Other exception types from arbitrary configuration callbacks propagate.

For runtime adapters, it also checks endpoint capability declarations: delays must be null or non-negative, the native batch count must be at least one, a declared message byte limit must be positive, and the consumption model must be defined. Null message limits remain unknown rather than unlimited. Null capability records and endpoint resolution failures (`ArgumentException`, `InvalidOperationException`, or `NotSupportedException`) become startup errors with transport and endpoint context. Unrelated exceptions propagate. An ordering key requires `OrderedByKey` on that route's destination; there is no ordering fallback. Errors identify the route and recommend an ordered endpoint or removal of the key.

Feature-specific validation ships with its executable feature: redelivery and dead-letter policy integration, scheduling and transaction integration with the outbox, and native/client-side batch execution. Merely declaring a batch size or registering capability values does not install a fallback executor. See [design §2.3](../concepts/capabilities.md) for native mechanisms and documented core fallbacks. Capability validation performs no per-message work and does not send messages or call `ITransport.StartAsync`.

The validator captures the topology mode and declaration lists at registration. Builder methods cannot mutate declarations after their callback ends. Native option objects and the normal Options registration pipeline retain their native mutability semantics; do not mutate them concurrently with a running adapter.

Binding supports configuration reload through `IOptionsMonitor<TOptions>`. This updates options, not routing or topology declarations. A transport decides which native settings it can safely apply live; changing a connection or entity declaration does not implicitly restart a native client. Use the named `Get` method rather than `CurrentValue`, which addresses the unnamed instance.

For Native AOT, applications binding configuration enable `<EnableConfigurationBindingGenerator>true</EnableConfigurationBindingGenerator>` and call `Bind` with a concrete options type. The generic core never performs reflection-based binding. Topology provisioning and feature-specific execution remain separate from declaration validation.
