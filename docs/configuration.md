# Configuration contract

`IRelayerBuilder.AddTransport<TTransportOptions, TRouteOptions, TConsumerOptions>` is the core seam for a transport package's typed registration extension. It declares routes and consumers; it does not substitute for a native client or start a broker receive loop. The implemented example is in [design §3.1](design.md#31-configuration), with executable coverage in `BuilderTests`.

## Transport authors

Pass native option types through unchanged. `TransportBuilder.Options` is an ordinary named `OptionsBuilder<TTransportOptions>`: use `Configure`, `Bind`, and `Validate` rather than inventing another configuration model. Every transport has a unique, case-sensitive name, shared by its named options. Register native client factories and hosted services in the same `IServiceCollection` exposed by `IRelayerBuilder.Services`.

Register each runtime adapter as an `ITransport` singleton with `Name` equal to its transport declaration's name. Before receive services start, the core calls `GetCapabilities(endpoint)` once for each distinct declared source or destination on that adapter. Resolve endpoint-specific values from native options or already available topology metadata; capability discovery must not start consumers. Duplicate adapter names fail startup. When any runtime adapter is registered, every transport declaration must have a matching adapter. With no runtime adapters, declaration-only applications can still validate and inspect configuration without a broker.

Use typed extensions on `RouteBuilder<TMessage, YourRouteOptions>` and `ConsumerBuilder<YourConsumerOptions>` for native features such as consumer groups or sessions. `Options` exposes the exact option instance. Do not add Kafka settings to another transport's builder. Do not expose both `Produce<T>` and `Send<T>` for the same route: the core uses `Send<T>` for destinations of every native kind.

At startup, resolve `RelayerConfiguration`. Each transport definition contains its routes and consumers in declaration order. Cast a route or consumer to its known closed generic definition once during composition. `RouteDefinition<TMessage, TRouteOptions>.GetOrderingKeySelector<TKey>()` returns the original typed delegate; `null` means no ordering key. An incompatible key type fails explicitly. The direct send path invokes the typed selector without reflection, boxing, or a new wrapper delegate. Format scalar keys into a pooled buffer when the native client needs bytes; a native adapter decides how its partition key, session ID, or message group ID maps to the selector.

`IConsumerDefinition.Handlers` contains explicit handler types for generated dispatch. `Handler<THandler>` requires `IHandlerMarker`, inherited by `IHandler<T>` and `IBatchHandler<T>`. It registers the handler scoped unless the application already registered it. Do not discover message contracts by reflection. Receive-pipeline composition uses generated descriptors, and checks handler/consumer compatibility there.

`Concurrency` defaults to 1. `BatchSize` defaults to 1, which disables batching; `BatchWait` defaults to 100 ms. Native adapters must honor the declarations using native behavior and the core scheduler where applicable, and validate their native limits before starting. These declarations do not emulate a native batching or ordering feature.

## Startup and reload

The core validator is inserted before transport hosted services and checks all transport names, destinations, stable message registrations, duplicate routes, handler presence, positive concurrency and batching settings, topology enum values, and each transport's named Options validators. Malformed bound values and Options validation failures are included in the same error list. Options validation failures during adapter construction also preserve declaration errors. Other exception types from arbitrary configuration callbacks propagate.

For runtime adapters, it also checks endpoint capability declarations: delays must be null or non-negative, the native batch count must be at least one, a declared message byte limit must be positive, and the consumption model must be defined. Null message limits remain unknown rather than unlimited. Null capability records and endpoint resolution failures (`ArgumentException`, `InvalidOperationException`, or `NotSupportedException`) become startup errors with transport and endpoint context. Unrelated exceptions propagate. An ordering key requires `OrderedByKey` on that route's destination; there is no ordering fallback. Errors identify the route and recommend an ordered endpoint or removal of the key.

Feature-specific validation ships with its executable feature: redelivery and dead-letter policy integration, scheduling and transaction integration with the outbox, and native/client-side batch execution. Merely declaring a batch size or registering capability values does not install a fallback executor. See [design §2.3](design.md#23-capabilities) for native mechanisms and documented core fallbacks. Capability validation performs no per-message work and does not send messages or call `ITransport.StartAsync`.

The validator captures the topology mode and declaration lists at registration. Builder methods cannot mutate declarations after their callback ends. Native option objects and the normal Options registration pipeline retain their native mutability semantics; do not mutate them concurrently with a running adapter.

Binding supports configuration reload through `IOptionsMonitor<TOptions>`. This updates options, not routing or topology declarations. A transport decides which native settings it can safely apply live; changing a connection or entity declaration does not implicitly restart a native client. Use the named `Get` method rather than `CurrentValue`, which addresses the unnamed instance.

For Native AOT, applications binding configuration enable `<EnableConfigurationBindingGenerator>true</EnableConfigurationBindingGenerator>` and call `Bind` with a concrete options type. The generic core never performs reflection-based binding. Topology provisioning and feature-specific execution remain separate from declaration validation.
