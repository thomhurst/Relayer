# Configuration diagnostics

Relayer ships compile-time analyzers and an IDE code fix in its NuGet package. They inspect symbols, not naming conventions, and add no runtime work. Startup validation still checks the complete configuration, including declarations assembled through helpers or other assemblies.

| ID | Default severity | Problem |
| --- | --- | --- |
| [RLY001](#rly001) | Warning | A message used by a route or handler has no `[Message]` attribute. |
| [RLY002](#rly002) | Error | Two types declare the same message name and version. |
| [RLY003](#rly003) | Warning | A concrete handler is never registered in this compilation. |
| [RLY004](#rly004) | Warning | An inline consumer configuration has no handler. |

## RLY001

Declare a stable wire identity on each message used by `Send<TMessage>`, `IHandler<TMessage>` or `IBatchHandler<TMessage>`:

```csharp
[Message("orders.placed", Version = 1)]
public sealed record OrderPlaced(string OrderId);
```

The code fix adds `[global::Relayer.MessageAttribute("OrderPlaced")]` to the message declaration. **Edit this suggestion to the permanent wire name before publishing.** It is an explicit source declaration, not a runtime fallback to CLR names. The fix preserves comments and supports classes, structs and records. Referenced types cannot be edited; add the attribute in their owning project or introduce a named message contract. Each missing message type is reported once per compilation, even when routes and multiple handlers use it.

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

## Suppression and generated code

Use `dotnet_diagnostic.<ID>.severity` in `.editorconfig`, or a targeted `#pragma warning disable <ID>` for intentionally invalid test fixtures. Generated code is excluded. Diagnostics run concurrently, with all mutable state scoped to one compilation.

`RLYH001` is the existing generator error for handler message types that generated dispatch cannot access. Make those message types assembly-accessible and non-file-local; see [handler discovery](design.md#33-registration-without-scanning).
