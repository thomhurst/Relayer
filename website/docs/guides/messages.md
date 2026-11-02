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

Use `transport.UseSerializer(provider => ...)` inside a named transport declaration to override serialization for that transport. Send routes and consumers use the same DI-owned singleton. Other transports keep the application serializer. Names are case-sensitive; selecting twice on one declaration fails. The factory must return a thread-safe serializer. If every transport selects its own serializer, the unused default JSON serializer does not require metadata.

Custom serializers can implement `IMessageSerializerInitializer`. The host awaits `InitializeAsync(CancellationToken)` during its startup lifecycle, before any hosted receiver starts, including concurrent service startup. A shared instance is initialized once. Preparation failures stop startup. Preparation does not add work to the per-message path. See [Kafka schema registries](../transports/kafka.md#independent-schema-registries) for a concrete registration.

## Protobuf

Reference `Relayer.Serialization.Protobuf` to use generated Google.Protobuf contracts. Generate C# from your `.proto` files with a matching `protoc` toolchain (3.13 or newer), then register each generated parser and its stable Relayer message identity explicitly:

```csharp
using Relayer.Serialization;
using Relayer.Serialization.Protobuf;

// OrderPlaced is generated from your .proto contract and exposes OrderPlaced.Parser.
services.AddSingleton<IMessageSerializer>(new ProtobufMessageSerializer(types =>
    types.Add(OrderPlaced.Parser)));
services.AddRelayer(relayer =>
    relayer.MessageTypes.Add<OrderPlaced>("orders.placed"));
```

For a named transport, return a `ProtobufMessageSerializer` from `transport.UseSerializer(_ => ...)` instead. Register every contract that transport sends or receives. Missing parsers throw an actionable `InvalidOperationException` when used; duplicate parsers fail during construction. Parser registration does not register a Relayer wire identity.

The serializer captures an immutable parser registry during construction and is safe to share across concurrent sends and receives. It writes directly to `IBufferWriter<byte>` and parses segmented `ReadOnlySequence<byte>` input. The content type is `application/protobuf`. Bodies contain native binary Protobuf, without a length prefix or schema registry envelope. CloudEvents headers still carry the stable Relayer message identity.

Empty Protobuf bodies represent default messages. Unknown fields follow the supplied parser's settings; for example, pass `OrderPlaced.Parser.WithDiscardUnknownFields(true)` to discard them. Malformed input throws the native `InvalidProtocolBufferException` and enters Relayer's normal deserialization failure policy.

Only generated contracts implementing `IMessage<T>` and `IBufferMessage` are supported. There is no reflection fallback, runtime model generation, JSON conversion, or descriptor-based dispatch. Native AOT and trimming smoke coverage verifies generated buffer serialization and parsing; applications must also validate their own generated contracts and any custom parser factories. Protobuf JSON and reflection-based descriptor operations are outside this integration. MessagePack remains planned.

## Avro

Reference `Relayer.Serialization.Avro` for Dekaf's source-generated Avro codecs and Schema Registry client. Opt each contract into generation with `[AvroRecord]`, then register its generated `AvroCodec` explicitly:

```csharp
using Dekaf.SchemaRegistry;
using Dekaf.SchemaRegistry.Avro.Poco;
using Relayer.Serialization;
using Relayer.Serialization.Avro;

services.AddSingleton<ISchemaRegistryClient>(_ => new SchemaRegistryClient(
    new SchemaRegistryConfig { Url = "http://localhost:8081" }));
services.AddSingleton<IMessageSerializer>(sp => new AvroMessageSerializer(
    sp.GetRequiredService<ISchemaRegistryClient>())
    .Add<OrderPlaced, OrderPlaced.AvroCodec>("orders"));
services.AddRelayer(relayer => relayer.MessageTypes.Add<OrderPlaced>("orders.placed"));

[AvroRecord(Name = "OrderPlaced", Namespace = "orders")]
public sealed partial class OrderPlaced
{
    public int Number { get; init; }
    public string Customer { get; init; } = "";
}
```

Host startup awaits registry registration or lookup and prepares the native reader before receivers start. Standalone callers must await `InitializeAsync(cancellationToken)` before use. Registry errors fail startup; initialization failures require a new serializer. The registry client remains caller-owned. Dispose the serializer after all operations finish. Declare every contract before initialization, then share the prepared serializer across concurrent sends and receives. Missing and duplicate codecs fail with actionable errors; codec registration does not register the Relayer message identity.

For independent registries, return this serializer from `transport.UseSerializer(sp => ...)`. The `topic` argument selects the registry subject, not routing; use the same topic in transport declarations. Pass native `AvroSerializerConfig` as the second argument to `Add` to select fixed schema IDs, disable auto-registration, normalize schemas or change the synchronous subject strategy. Native client configuration passes through unchanged.

Bodies use Confluent's five-byte schema ID prefix followed by native Avro binary, with content type `avro/binary`. CloudEvents headers retain the stable Relayer identity. Only the prepared writer schema ID for that message is accepted; unknown IDs fail without registry I/O. Declare older wire contracts separately and use [upcasters](#versioning-and-unknown-types). Invalid prefixes, truncated values and malformed Avro enter the normal deserialization failure policy. Value parsing follows Dekaf, including its acceptance of bytes after the decoded record.

Writes go directly to `IBufferWriter<byte>`. Contiguous input reads directly from native memory; segmented `ReadOnlySequence<byte>` input uses one pooled copy because Dekaf reads contiguous memory. The rented buffer is cleared on return. No reflection fallback, runtime codec generation or per-message registry discovery is used. Only generated `IAvroPocoCodec<T>` contracts are supported; Apache Avro reflection/specific-record serializers are outside this adapter. Latest-version selection, asynchronous/associated subject strategies, registry rules/validation and header/GUID identities fail during configuration because they require per-record preparation.

**Native AOT and trimming are currently unsupported for this optional package.** Although record encoding uses generated codecs, Dekaf 1.25.0's startup schema preparation reaches Apache.Avro/Newtonsoft.Json reflection and dynamic-code paths. A Native AOT publish probe fails with IL2026/IL3050; [Dekaf #4060](https://github.com/thomhurst/Dekaf/issues/4060) tracks the upstream limitation. `Add` carries `RequiresUnreferencedCode` and `RequiresDynamicCode` so affected applications receive build diagnostics. Relayer does not suppress those diagnostics or substitute a reflection serializer. Use the default generated JSON serializer or Protobuf in AOT applications.

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
