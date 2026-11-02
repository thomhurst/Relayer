---
title: Topology
description: "Declared topology: validate or provision broker entities at startup, and export JSON or AsyncAPI documents."
---

Declare routes and consumers normally. Relayer builds an inspectable topology from those declarations; it never infers broker entities from CLR names.

```csharp
services.AddRelayer(relayer =>
{
    relayer.AddMessageTypesFromThisAssembly();
    relayer.AddTransport<BrokerOptions, RouteOptions, ConsumerOptions>("events", transport =>
    {
        transport.TopologyMode = TopologyMode.Provision; // Development only; default is Validate.
        transport.Send<OrderPlaced>("orders");
        transport.Consume("orders", consumer => consumer.Handler<OrderHandler>());
    });
});

// No broker connection is needed to inspect or export declarations.
var topology = serviceProvider.GetRequiredService<ITopology>();
using var output = File.Create("topology.json");
topology.ExportJson(output);
```

The generic options in this example stand for native transport option types. Transport packages supply typed builder extensions over this declaration surface.

## Modes

`IRelayerBuilder.TopologyMode` sets the application default (`Validate`). Each transport's nullable `TopologyMode` overrides it; null inherits the default. Modes are captured when registration completes.

- `Validate` reads broker state with least-privilege permissions and fails startup with all remaining differences.
- `Provision` uses native APIs to create missing entities. It must report settings it cannot reconcile and must not delete entities.
- `None` skips topology operations entirely. Configuration and capability validation still run.

## Startup order

Configuration validation runs first, followed by topology operations, in the host's `IHostedLifecycleService.StartingAsync` phase. The host completes this phase before calling any receive service's `StartAsync`, even with `HostOptions.ServicesStartConcurrently` enabled. A failure prevents receive services from starting. A declaration-only application without runtime adapters can inspect and export topology without broker access. Once runtime adapters are registered, every transport declaration must have exactly one named adapter.

## Transport adapters

Adapters implement `ITransport.ApplyTopologyAsync(mode, cancellationToken)`. They resolve their named declaration through `ITopology`, retain native route and consumer options, and return a `TopologyReport` containing remaining differences. Include entity names and expected/actual native settings in each difference. An empty report means the declaration matches. Broker exceptions become startup failures with transport and mode context; requested cancellation propagates unchanged. Provisioning is not transactional across transports.

## JSON export

JSON export uses schema version 1, transport declaration order, effective modes, destinations, sources, stable message names/versions, ordering requirements, concurrency and batch size. Consumer messages come from explicitly registered handler descriptors. Native option objects and credentials are deliberately excluded from JSON; inspect the typed declarations for native settings. The writer uses explicit JSON operations and works with reflection disabled under Native AOT. The output stream remains open. CLI, Bicep and Terraform exports belong to later tooling work.

## AsyncAPI export

Export an [AsyncAPI 3.0.0 document](https://www.asyncapi.com/docs/reference/specification/v3.0.0) from the same declarations:

```csharp
var topology = serviceProvider.GetRequiredService<ITopology>();
using var output = File.Create("asyncapi.json");
topology.ExportAsyncApi(output, new AsyncApiOptions
{
    Title = "Order processing",
    Version = "1.0.0" // Application API version, not the AsyncAPI specification version.
});
```

Channels contain declared destinations and consumer sources. An endpoint shared by sends and consumers has one channel within its named transport; identical addresses in different transports remain separate channels. Operations use `send` and `receive` from this application's perspective. Consumer messages come from explicit handler descriptors, with duplicate message types removed. Components contain every registered message, including messages with no route or consumer. Message `name`, `x-relayer-version` and `x-relayer-wire-type` preserve the stable wire identity. Encoded component keys avoid collisions between versions and names containing punctuation or Unicode.

Payloads use .NET's `JsonTypeInfo.GetJsonSchemaAsNode` over the same source-generated contexts registered for JSON serialization. As with serialization, the last context covering a message wins. Register contexts through `AddMessageTypesFromThisAssembly()` or `UseJsonSerializer(context)`. Missing metadata fails export before writing the document; supplying a custom `IMessageSerializer` alone does not provide schema metadata. Recursive schemas have their own `$id` so local references stay inside the payload. Export works with JSON reflection disabled and under Native AOT.

Supported format and limits:

- The document version is **AsyncAPI 3.0.0**. Payloads are Multi Format Schema Objects with **JSON Schema Draft 2020-12**, identified by `application/schema+json;version=draft-2020-12`. This custom schema format requires tooling that supports Draft 2020-12; AsyncAPI 3.0.0 tools are only required to support Draft 07.
- **No protocol bindings** are exported for Kafka, Service Bus, SQS/SNS, RabbitMQ, in-memory or custom transports. The document records transport names through `x-relayer-transport`, without guessing broker URLs, protocols, security settings or native options.
- Schemas describe the registered JSON contract. The exporter does not inspect custom or named transport serializers, schema registries, Protobuf, MessagePack or Avro wire formats. It omits `contentType` rather than claiming that every transport uses JSON. Custom JSON converters inherit .NET's schema coverage and may produce an unconstrained schema.
- Native headers, CloudEvents header encodings, Kafka tombstone bodies, native routing/bindings and broker-discovered entities are not modeled. Ordering requirements, consumer concurrency and batch size appear as `x-relayer-*` extensions.
- Export is explicit tooling work, outside message processing. Declaration and registration order determine output order. Identical declarations and metadata produce identical UTF-8 JSON; no timestamps or CLR type names are added. Both document title and application version must be non-empty. The destination remains open.
