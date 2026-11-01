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

The version is part of the type name. A consumer that receives a type or version it has no handler for treats the message as poison: it enters [failure handling](failure-handling.md) as a permanent failure and is dead-lettered with a reason. It is never ignored silently and never crashes the consumer.
