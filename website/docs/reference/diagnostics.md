---
title: Diagnostics
description: "Compile-time analyzers RLY001 to RLY004 and RLYH001, generator errors RLYM001 to RLYM009, and preview opt-ins RLYEXP001 to RLYEXP004: what each reports and how to fix it."
---

Relayer ships compile-time analyzers and an IDE code fix in its NuGet package. They inspect symbols, not naming conventions, and add no runtime work. Startup validation still checks the complete configuration, including declarations assembled through helpers or other assemblies.

| ID | Default severity | Problem |
| --- | --- | --- |
| [RLYEXP001](#rlyexp001) | Error | Explicit opt-in for sagas. |
| [RLYEXP002](#rlyexp002) | Error | Explicit opt-in for PostgreSQL local transport. |
| [RLYEXP003](#rlyexp003) | Error | Explicit opt-in for PostgreSQL change capture. |
| [RLYEXP004](#rlyexp004) | Error | Explicit opt-in for PostgreSQL tenant catalog dispatch. |
| [RLY001](#rly001) | Warning | A message used by a route, request, or handler has neither `[Message]` nor an explicit identity registration. |
| [RLY002](#rly002) | Error | Two types declare the same message name and version. |
| [RLY003](#rly003) | Warning | A non-public concrete handler is never registered in this compilation. |
| [RLY004](#rly004) | Warning | An inline consumer configuration has no handler. |
| [RLYH001](#rlyh001) | Error | A generated handler registration selects an inaccessible message type. |
| [RLYM001](#rlym001) | Error | A message declaration has type parameters on itself or a containing type. |
| [RLYM002](#rlym002) | Error | A declared message type is inaccessible to generated registration and JSON metadata. |
| [RLYM003](#rlym003) | Error | A `[Message]` type is ref-like. |
| [RLYM004](#rlym004) | Error | A declared message type is static. |
| [RLYM005](#rlym005) | Error | A declared message name is null. |
| [RLYM006](#rlym006) | Error | A message name is empty or whitespace. |
| [RLYM007](#rlym007) | Error | A message version is below 1. |
| [RLYM008](#rlym008) | Error | `GenerateMessageTypes` specifies an invalid exported registration name. |
| [RLYM009](#rlym009) | Error | `GenerateMessageJsonType` specifies an invalid payload type. |

## RLY001

Declare a stable wire identity on each message used by `Send<TMessage>`, `IHandler<TMessage>` or `IBatchHandler<TMessage>`. For `Request<TRequest, TResponse>`, declare identities for both the request and the response.

```csharp
[Message("orders.placed", Version = 1)]
public sealed record OrderPlaced(string OrderId);
```

The code fix adds `[global::Relayer.MessageAttribute("OrderPlaced")]` to the message declaration. **Edit this suggestion to the permanent wire name before publishing.** It is an explicit source declaration, not a runtime fallback to CLR names. The fix preserves comments and supports classes, structs and records. For a type from a referenced assembly, add the attribute in its owning project or register its identity explicitly. Use explicit registration for a closed generic contract or a contract nested in a generic type. The code fix is unavailable for those declarations.

RLY001 reports a source-declared message type once at its declaration. For a type from a referenced assembly, it reports each use that it checks.

Alternatively, call `builder.Infrastructure().MessageTypes.Add<OrderPlaced>("orders.placed")`. RLY001 recognizes calls to Relayer's `MessageTypeRegistry.Add<T>` anywhere in the current compilation, including after the route or handler use. The registration covers that CLR type and its `Tombstone<T>` wrapper. For example, `ValueTuple<int, string>` covers `(int Id, string Account)`, and `Envelope<object>` covers `Envelope<dynamic>`. Register each distinct closed generic type separately. Calls hidden inside a referenced assembly or generic helper are not inferred; mark the helper instead, as below. The analyzer does not prove that a visible registration executes; startup validation remains authoritative.

For a generic handler whose message is nested in a generic type, such as `IHandler<Outer<T>.Message>`, RLY001 checks the closed message when you register the concrete handler. For example, `consumer.Handler<OuterHandler<int>>()` can use an identity registered with `builder.Infrastructure().MessageTypes.Add<Outer<int>.Message>("orders")`.

When a generic local handler registration uses a message type parameter from an enclosing helper, the message stays open inside that helper. If the enclosing helper constrains its handler to `IHandler<TMessage>` or `IBatchHandler<TMessage>`, RLY001 checks the message identity at the call that supplies the concrete message type.

When a helper registers the identity, for example a shared messaging library that registers it together with the route, mark the helper's type parameter with `[RegistersMessageIdentity]`. Each call with a concrete type argument then counts as a registration, including calls to a helper in a referenced assembly:

```csharp
[RegistersMessageIdentity(nameof(TMessage))]
public static void PublishRaw<TMessage>(this IRelayerBuilder builder, string name) =>
    builder.Infrastructure().MessageTypes.Add<TMessage>(name);

builder.PublishRaw<OrderPlaced>("orders.placed"); // OrderPlaced counts as registered
```

Pass the name with `nameof` so a rename keeps it in step: a name that matches none of the helper's type parameters registers nothing, and RLY001 keeps reporting.

When startup registers every closed form of a generic contract, for example `Envelope<OrderPlaced>` from a catalog (`[Message]` cannot go on a generic type, [RLYM001](#rlym001)), declare that once per assembly:

```csharp
[assembly: MessageIdentitiesRegisteredAtStartup(typeof(Envelope<>))]
```

RLY001 then skips every `Envelope<T>` in that compilation, and their `Tombstone<T>` wrappers, and stays on for every other type. Any other type, such as `typeof(Envelope<OrderPlaced>)` or `typeof(OrderPlaced[])`, covers only itself. If registration happens somewhere the analyzer cannot see and neither attribute fits, suppress RLY001 at the reported location after checking that startup registers the identity. Turning RLY001 off in `.editorconfig` also turns it off for `[Message]` contracts.

## RLY002

Message names are case-sensitive. The default version is 1. Two different types cannot both declare `[Message("orders.placed")]` or an equivalent explicit `Version = 1`. Choose a distinct stable name or a new contract version. The analyzer reports each conflicting declaration in the current compilation; partial declarations of one type count once. Startup validation also checks identities registered from referenced assemblies.

## RLY003

Select a concrete, assembly-accessible handler inside its consumer configuration callback:

```csharp
kafka.Consume("orders", consumer =>
{
    consumer.Options.Group = "orders-handler";
    consumer.Handler<OrderPlacedHandler>();
});
```

This call registers the generated descriptors and DI service as well as binding the consumer. Builder-level `Handler<T>()` and `AddHandlersFromThisAssembly()` are no longer generated; move each registration into the consumer that handles its messages. Both static and extension forms of generated consumer registration calls are recognized throughout the compilation. Batch handlers, inherited contracts and explicit interface implementations are supported. Abstract, open generic, inaccessible and file-local handlers are excluded. Closed generic and referenced handlers can be registered explicitly. A handler passed as a type argument to a generic method whose type parameter is constrained to `IHandler<T>`, `IBatchHandler<T>` or `IHandlerMarker` also counts, so `Handler<THandler, TMessage>()` and helpers that forward to it, including helpers in referenced assemblies, register it ([Registering from a generic helper](../guides/handlers.md#registering-from-a-generic-helper)).

Public handlers are not reported: a handler that is public, and nested only in public types, can be registered by any assembly that references it, so a module library can leave registration to its host. The warning covers handlers that only their own compilation can register, such as `internal` handlers and public handlers nested in an `internal` type.

This is compilation-local analysis, not a proof of runtime reachability. A registration in a helper counts even if the helper is not called. If a library intentionally leaves registration of an internal handler to another assembly, for example through `InternalsVisibleTo`, suppress this warning in that library with `.editorconfig`:

```ini
[*.cs]
dotnet_diagnostic.RLY003.severity = none
```

## RLY004

A consumer needs a handler even if its callback sets native options, concurrency, batching, or failure behavior with `Failures` or `Classify`. This check applies to both `TransportBuilder<TTransportOptions, TConsumerOptions>` and `ITransportConsumerBuilder`.

```csharp
broker.Consume("orders", consumer => consumer.Handler<OrderPlacedHandler>());
```

The analyzer checks inline lambdas and anonymous delegates. It does not follow method-group callbacks, arbitrary helper calls, aliases or captures that can register handlers. These cases remain subject to startup validation. A handler on another consumer or the global builder does not satisfy an empty callback.

## RLYH001

Generated `Handler<THandler>()` and `Consume<THandler>()` register every contract on the handler. This error points to a call that selects a concrete handler with an inaccessible message type. To register all contracts, make the message types and their containing types assembly-accessible (for example, `internal` or `public`) and non-file-local. To select one ordinary `IHandler<TMessage>` contract, call [`Handler<THandler, TMessage>()`](../guides/handlers.md#registering-from-a-generic-helper). The typed overload can use a handler with other inaccessible contracts.

## RLYM001

`[Message]` requires a non-generic declaration outside generic containing types. An open generic message such as `Order<T>`, or a non-generic message nested inside `Container<T>`, cannot be referenced from generated registration and JSON metadata. Declare a non-generic contract at namespace scope or inside a non-generic containing type. Abstract contracts are supported with a serializer configured for polymorphism. See [message contracts](../guides/messages.md#stable-message-names).

The generator reports this error at the unsupported declaration and excludes it from both generated outputs. Other valid messages still generate normally.

## RLYM002

A `[Message]` declaration must be accessible from the generated assembly-wide registration and JSON context. Make the message and all containing types `internal` or `public` (or otherwise accessible within the assembly), and remove any `file` modifier. Unsupported declarations are excluded from both generated outputs; other accessible messages still generate normally. See [message registration](../guides/messages.md).

## RLYM003

A wire message cannot be a `ref struct` or `readonly ref struct`: stack-only types cannot be used by registration and serializer generic APIs. Replace the declaration with an ordinary struct, class or record. The generator reports the message declaration and excludes it from generated registration and JSON metadata; valid messages in the same compilation still generate.

## RLYM004

A static class cannot be a wire message because generated registration and JSON metadata require an instance type. Declare a non-static class, struct or record instead. The unsupported declaration is excluded from both generated outputs; ordinary messages nested inside accessible static containers remain supported. See [message registration](../guides/messages.md).

## RLYM005

A `[Message]` name cannot be `null`, including `null!` (the null-forgiving operator does not supply a value). Replace it with a permanent wire identity, such as `[Message("orders.placed")]`. The generator reports the error at the attribute and excludes the invalid declaration from registration and JSON metadata; other valid messages still generate normally. An assembly containing only invalid declarations produces no message registration or JSON context.

## RLYM006

`[Message]` names must contain at least one non-whitespace character, matching the runtime `MessageTypeRegistry.Add<TMessage>` contract. Replace an empty or whitespace-only name with a stable wire name such as `[Message("orders.placed")]`. Valid names are preserved exactly, including surrounding whitespace. Invalid declarations are excluded from generated registration and JSON metadata.

## RLYM007

`[Message]` versions must be at least 1, matching the runtime `MessageTypeRegistry.Add<TMessage>` contract. Remove `Version` to use the default 1, or specify a positive version such as `[Message("orders.placed", Version = 2)]`. Invalid declarations are excluded from generated registration and JSON metadata. Explicit runtime registration retains the same validation guards.

## Suppression and generated code

Use `dotnet_diagnostic.<ID>.severity` in `.editorconfig`, or a targeted `#pragma warning disable <ID>` for intentionally invalid test fixtures. Generated code is excluded. Diagnostics run concurrently, with all mutable state scoped to one compilation.

## RLYM008 — Invalid exported message registration name {#rlym008}

`[assembly: GenerateMessageTypes("AddOrderMessages")]` exports an explicit public registration extension for a contracts library. Supply a non-empty C# identifier that is not a keyword or `AddMessageTypesFromThisAssembly`. Choose distinct names for libraries used together, for example `AddOrderMessages` and `AddBillingMessages`. Invalid names stop generation of the exported method; local registration remains available.

## RLYM009 — Invalid JSON metadata type {#rlym009}

`[GenerateMessageJsonType]` requires a closed type that the generated JSON context can reference. Pass `typeof` with an accessible, non-file-local type. For a generic type, supply all type arguments, for example `typeof(Envelope<OrderPlaced>)`. Static types and ref structs cannot be serialized. See [payload metadata names](../guides/messages.md#payload-types-that-need-distinct-metadata-names).

## Preview feature opt-in

These compiler diagnostics come from `ExperimentalAttribute`. Preview APIs may change outside the first stable release compatibility promise. Suppress only the ID for the feature you accept, using `#pragma warning disable RLYEXP001` around the use or `<NoWarn>$(NoWarn);RLYEXP001</NoWarn>` in your project. This does not change runtime behavior.

## RLYEXP001

Sagas, diagram tooling and EF Core saga persistence require explicit preview opt-in. Suppress `RLYEXP001` only after accepting the preview compatibility boundary. When opting in with pragmas, cover your saga declarations, EF model configuration that uses preview properties, and explicit preview calls such as `consumer.Saga<TContext, TState>()`. The generated `consumer.Handler<THandler>()` already suppresses the diagnostic for its internal preview API use. See the [saga registration example](../guides/sagas.md#explicit-registration).

## RLYEXP002

PostgreSQL local transport requires explicit preview opt-in. Suppress `RLYEXP002` only after accepting the preview compatibility boundary.

## RLYEXP003

PostgreSQL change capture requires explicit preview opt-in. Suppress `RLYEXP003` only after accepting the preview compatibility boundary.

## RLYEXP004

PostgreSQL tenant catalog dispatch requires explicit preview opt-in. Suppress `RLYEXP004` only after accepting the preview compatibility boundary.
