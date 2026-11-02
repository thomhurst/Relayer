---
title: Messages and serialization
description: "Stable message names and versions, source-generated System.Text.Json serialization, custom serializers and versioning."
---

## Stable message names

Messages are identified by a stable string name and version, never by the CLR type name:

```csharp
[Message("billing.charge-card", Version = 2)]
public sealed record ChargeCard(Guid OrderId, decimal Amount);
```

On the wire this becomes the CloudEvents type `billing.charge-card;v=2`. Renaming or moving the class never breaks consumers. Names are case-sensitive and the default version is 1. Two types cannot declare the same name and version; analyzer [RLY002](../reference/diagnostics.md#rly002) reports the conflict at compile time and startup validation checks referenced assemblies too.

Register message types explicitly, or with the source-generated method that lists every `[Message]` type in the assembly:

```csharp
services.AddRelayer(relayer => relayer.AddMessageTypesFromThisAssembly());
```

## Serialization

The default serializer is System.Text.Json with a source-generated `JsonSerializerContext`. The generator builds it from the declared message types, so serialization is Native AOT safe and never falls back to reflection.

`AddMessageTypesFromThisAssembly()` registers the messages and their generated JSON context together. Pass `JsonSerializerOptions` to customize JSON, or add your own context with `UseJsonSerializer`:

```csharp
services.AddRelayer(relayer =>
{
    relayer.AddMessageTypesFromThisAssembly(new JsonSerializerOptions
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase
    });
    // Optional: override metadata for the types covered by this context.
    relayer.UseJsonSerializer(MyJsonContext.Default);
});
```

When contexts overlap, the last registered context supplies the metadata for a type. A custom context can also cover explicitly registered message types from a contracts assembly. Missing metadata fails when the serializer is first resolved, which includes host startup through Relayer's hosted validator.

Serialization writes to pooled `IBufferWriter<byte>` storage and deserialization reads the `ReadOnlySequence<byte>` directly. Null bodies and trailing JSON values are rejected.

## Custom serializers

Register your own `IMessageSerializer` in the service collection, before, inside or after `AddRelayer`, to replace JSON. The default uses a lazy DI factory, so the final registration decides whether JSON metadata is needed at all.

## Versioning and unknown types

The version is part of the type name. Register `IUpcaster<TFrom, TTo>` instances explicitly to convert older payloads before the selected handler runs:

```csharp
[Message("orders.placed", Version = 1)]
public sealed record OrderPlacedV1(Guid OrderId);

[Message("orders.placed", Version = 2)]
public sealed record OrderPlacedV2(Guid OrderId, string Currency);

public sealed class OrderUpgrade : IUpcaster<OrderPlacedV1, OrderPlacedV2>
{
    public OrderPlacedV2 Upcast(OrderPlacedV1 message) => new(message.OrderId, "GBP");
}

services.AddRelayer(relayer =>
{
    relayer.AddMessageTypesFromThisAssembly();
    relayer.Upcasters.Add(new OrderUpgrade());
    // Select an IHandler<OrderPlacedV2> or IBatchHandler<OrderPlacedV2>
    // on the consumer using the normal transport configuration.
});
```

Register every contract, including intermediate versions. Each upgrade must keep the same stable name and increase the version. Each source contract has one upgrade; duplicate sources, missing identities, name changes and non-increasing versions fail during configuration. Chains such as v1 to v2 to v3 are composed once when the consumer is built, regardless of registration order. A consumer must select only one reachable handler per incoming identity; selecting both an old handler and its upgraded handler is ambiguous and fails composition.

The serializer reads the original wire contract once. Upcasters then pass typed values to the selected single-message or batch handler, without reserialization, reflection or boxing. Batch handlers can receive a mix of supported wire versions, all converted to their handled contract. The context, headers, original body and native message retain the original wire identity. Retry attempts repeat deserialization and upcasting from the original payload.

Upcaster instances are shared across consumers and concurrent deliveries. Keep them thread-safe; use value types or reuse immutable data where appropriate to avoid per-message allocations. Exceptions from an upcaster enter the normal failure policy, like handler exceptions. Malformed payloads remain permanent deserialization failures.

A consumer that receives a type or version with neither a selected handler nor a registered chain to one treats the message as poison: it enters [failure handling](failure-handling.md) as a permanent failure and is dead-lettered with a reason that includes the wire identity. It is never ignored silently and never crashes the consumer. An incomplete chain does not enable an unselected handler.
