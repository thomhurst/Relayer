---
title: Diagnostics
description: "Compile-time analyzers RLY001 to RLY004 and generator errors RLYH001 and RLYM001 to RLYM004: what each reports and how to fix it."
---

Relayer ships compile-time analyzers and an IDE code fix in its NuGet package. They inspect symbols, not naming conventions, and add no runtime work. Startup validation still checks the complete configuration, including declarations assembled through helpers or other assemblies.

| ID | Default severity | Problem |
| --- | --- | --- |
| [RLY001](#rly001) | Warning | A message used by a route or handler has neither `[Message]` nor an explicit identity registration. |
| [RLY002](#rly002) | Error | Two types declare the same message name and version. |
| [RLY003](#rly003) | Warning | A concrete handler is never registered in this compilation. |
| [RLY004](#rly004) | Warning | An inline consumer configuration has no handler. |
| [RLYH001](#rlyh001) | Error | A handler message type is inaccessible to generated dispatch. |
| [RLYM001](#rlym001) | Error | A message declaration has type parameters on itself or a containing type. |
| [RLYM002](#rlym002) | Error | A declared message type is inaccessible to generated registration and JSON metadata. |
| [RLYM003](#rlym003) | Error | A `[Message]` type is ref-like. |
| [RLYM004](#rlym004) | Error | A declared message type is static. |

## RLY001

Declare a stable wire identity on each message used by `Send<TMessage>`, `IHandler<TMessage>` or `IBatchHandler<TMessage>`:

```csharp
[Message("orders.placed", Version = 1)]
public sealed record OrderPlaced(string OrderId);
```

The code fix adds `[global::Relayer.MessageAttribute("OrderPlaced")]` to the message declaration. **Edit this suggestion to the permanent wire name before publishing.** It is an explicit source declaration, not a runtime fallback to CLR names. The fix preserves comments and supports classes, structs and records. Referenced types cannot be edited; add the attribute in their owning project or register their identity explicitly. Each missing message type is reported once per compilation, even when routes and multiple handlers use it.

Alternatively, call `builder.MessageTypes.Add<OrderPlaced>("orders.placed")`. RLY001 recognizes calls to Relayer's `MessageTypeRegistry.Add<T>` anywhere in the current compilation, including after the route or handler use. The registration covers that exact type and its `Tombstone<T>` wrapper; registering one closed generic type does not cover other instantiations. Calls hidden inside a referenced assembly or generic helper are not inferred. If registration happens outside this compilation, suppress RLY001 at the reported location or through `.editorconfig` after checking that startup registers the identity. The analyzer does not prove that a visible registration executes; startup validation remains authoritative.

## RLY002

Message names are case-sensitive. The default version is 1. Two different types cannot both declare `[Message("orders.placed")]` or an equivalent explicit `Version = 1`. Choose a distinct stable name or a new contract version. The analyzer reports each conflicting declaration in the current compilation; partial declarations of one type count once. Startup validation also checks identities registered from referenced assemblies.

## RLY003

Register a concrete, assembly-accessible handler explicitly:

```csharp
builder.Handler<OrderPlacedHandler>();
// Or explicitly register all discoverable handlers in this assembly:
builder.AddHandlersFromThisAssembly();
```

Registration inside `consumer.Handler<THandler>()` also counts. Both static and extension forms of generated registration calls are recognized throughout the compilation. Batch handlers, inherited contracts and explicit interface implementations are supported. Abstract, open generic, inaccessible and file-local handlers are not candidates for assembly registration. Closed generic and referenced handlers can be registered explicitly.

This is compilation-local analysis, not a proof of runtime reachability. A registration in a helper counts even if the helper is not called. If a library intentionally leaves registration to another assembly, suppress this warning in that library with `.editorconfig`:

```ini
[*.cs]
dotnet_diagnostic.RLY003.severity = none
```

## RLY004

An empty consumer, including one that only sets native options, concurrency or batching, needs a handler:

```csharp
broker.Consume("orders", consumer => consumer.Handler<OrderPlacedHandler>());
```

The analyzer checks inline lambdas and anonymous delegates. It does not follow method-group callbacks, arbitrary helper calls, aliases or captures that can register handlers. These cases remain subject to startup validation. A handler on another consumer or the global builder does not satisfy an empty callback.

## RLYH001

Generated dispatch must be able to access each handler's message type. Make the message type and any containing types assembly-accessible (for example, `internal` or `public`) and non-file-local. A private nested message type or a file-local type cannot be referenced by generated dispatch; see [handler discovery](../guides/handlers.md#registration-without-scanning).

## RLYM001

`[Message]` requires a concrete, non-generic declaration outside generic containing types. An open generic message such as `Order<T>`, or even a non-generic message nested inside `Container<T>`, cannot be referenced from generated registration and JSON metadata. Declare a concrete contract such as `OrderPlaced` at namespace scope or inside a non-generic containing type. The generator reports this error at the unsupported declaration and excludes it from both generated outputs; other valid messages still generate normally.

## RLYM002

A `[Message]` declaration must be accessible from the generated assembly-wide registration and JSON context. Make the message and all containing types `internal` or `public` (or otherwise accessible within the assembly), and remove any `file` modifier. Unsupported declarations are excluded from both generated outputs; other accessible messages still generate normally. See [message registration](../guides/messages.md).

## RLYM003

A wire message cannot be a `ref struct` or `readonly ref struct`: stack-only types cannot be used by registration and serializer generic APIs. Replace the declaration with an ordinary struct, class or record. The generator reports the message declaration and excludes it from generated registration and JSON metadata; valid messages in the same compilation still generate.

## RLYM004

A static class cannot be a wire message because generated registration and JSON metadata require an instance type. Declare a non-static class, struct or record instead. The unsupported declaration is excluded from both generated outputs; ordinary messages nested inside accessible static containers remain supported. See [message registration](../guides/messages.md).

## Suppression and generated code

Use `dotnet_diagnostic.<ID>.severity` in `.editorconfig`, or a targeted `#pragma warning disable <ID>` for intentionally invalid test fixtures. Generated code is excluded. Diagnostics run concurrently, with all mutable state scoped to one compilation.
