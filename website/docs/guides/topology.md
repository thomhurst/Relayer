---
title: Topology
description: "Declared topology: validate or provision broker entities at startup, and export JSON, AsyncAPI or Service Bus infrastructure."
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

JSON export uses schema version 1, transport declaration order, effective modes, destinations, sources, stable message names/versions, ordering requirements, concurrency and batch size. Consumer messages come from explicitly registered handler descriptors. Native option objects and credentials are deliberately excluded from JSON; inspect the typed declarations for native settings. The writer uses explicit JSON operations and works with reflection disabled under Native AOT. The output stream remains open.

## Service Bus infrastructure export

`Relayer.AzureServiceBus` provides `ServiceBusTopologyExporter` for **one explicitly selected named Service Bus transport**. It reads `ITopology` and the same named `AzureServiceBusTransportOptions` used by the application, including native `CreateQueueOptions`, `CreateTopicOptions` and `CreateSubscriptionOptions`. It does not create clients, resolve runtime adapters, start the host, connect to Azure or provision anything. Other named transports are outside this selection; exporting one of them through this exporter fails.

Here is a standalone export application. Create a .NET 10 console project, add `Relayer.AzureServiceBus` and `Microsoft.Extensions.DependencyInjection`, and replace `Program.cs` with:

```csharp
using Azure.Messaging.ServiceBus.Administration;
using Microsoft.Extensions.DependencyInjection;
using Relayer;
using Relayer.AzureServiceBus;

if (args.Length != 2 || args[0] is not ("bicep" or "terraform"))
    throw new ArgumentException("Usage: <bicep|terraform> <output-path>");

var services = new ServiceCollection();
services.AddRelayer(relayer => relayer.ServiceBus("events", transport =>
{
    transport.Options.Configure(options =>
    {
        // Share this registration with the application's actual declarations.
        // No connection string or Azure credentials are needed for export.
        options.Queues.Add(new CreateQueueOptions("orders"));
        options.Topics.Add(new CreateTopicOptions("order-events"));
        options.Subscriptions.Add(new CreateSubscriptionOptions("order-events", "billing")
        {
            MaxDeliveryCount = 5
        });
    });
}));

using var provider = services.BuildServiceProvider();
using var output = new MemoryStream();
provider.GetRequiredService<ServiceBusTopologyExporter>().Export("events", output,
    args[0] == "bicep" ? ServiceBusTopologyFormat.Bicep : ServiceBusTopologyFormat.Terraform);
// Publish the file only after validation succeeds; the exporter leaves output open.
File.WriteAllBytes(args[1], output.ToArray());
```

Run it from your console project's directory:

```bash
dotnet run -- bicep topology.bicep
dotnet run -- terraform topology.tf
```

The Bicep artifact targets an existing namespace in the deployment's resource group through `param serviceBusNamespaceName string`. For example, the generated topic/subscription declarations include the following (property lists abbreviated):

```bicep
param serviceBusNamespaceName string

resource serviceBusNamespace 'Microsoft.ServiceBus/namespaces@2024-01-01' existing = {
  name: serviceBusNamespaceName
}

resource topic1 'Microsoft.ServiceBus/namespaces/topics@2024-01-01' = {
  parent: serviceBusNamespace
  name: 'order-events'
  properties: {
    enableBatchedOperations: true
    requiresDuplicateDetection: false
    status: 'Active'
    supportOrdering: false
  }
}

resource subscription2 'Microsoft.ServiceBus/namespaces/topics/subscriptions@2024-01-01' = {
  parent: topic1
  name: 'billing'
  properties: {
    maxDeliveryCount: 5
    requiresSession: false
    deadLetteringOnFilterEvaluationExceptions: true
  }
}
```

Terraform output is HCL (`.tf`) using the **Azure/AzAPI v2** provider and the same ARM 2024-01-01 resource properties. The artifact includes the provider requirement, an empty provider configuration and a required `servicebus_namespace_id` string variable. Configure provider authentication externally. The corresponding subscription excerpt is:

```hcl
resource "azapi_resource" "subscription2" {
  type      = "Microsoft.ServiceBus/namespaces/topics/subscriptions@2024-01-01"
  name      = "billing"
  parent_id = azapi_resource.topic1.id
  body = {
    properties = {
      maxDeliveryCount = 5
      requiresSession = false
      deadLetteringOnFilterEvaluationExceptions = true
    }
  }
}
```

The exporter preserves all native public entity settings supported by these ARM schemas: idle/TTL/duplicate-detection durations, batching, partitioning, duplicate detection, sessions, size limits, locks, delivery counts, dead-letter settings, forwarding, topic ordering and entity status. Durations use ISO 8601. Idle/TTL values of `TimeSpan.MaxValue` are omitted to use the service defaults, matching the native SDK's serialization. Optional message sizes are omitted when unset. Declaration order fixes resource symbols and output order; repeated exports of unchanged declarations are identical UTF-8 without a BOM or timestamps. Resource names are escaped as literals, including interpolation syntax and Unicode.

Subscriptions reference their declared topic. Forwarding to another exported queue/topic matches names case-insensitively and adds a deployment dependency while preserving the native forwarding address; undeclared forwarding targets are treated as externally managed entities and must already exist. Cyclic forwarding/parent dependencies fail before output is written. No deployment is performed or validated against an Azure account, and namespace SKU/service restrictions still apply when you deploy.

The exporter rejects nonempty `UserMetadata` (absent from these ARM schemas), entity `AuthorizationRules` (manage credentials separately), slash-containing entity names (unsupported ARM child names), unknown entity statuses, duplicate entities/subscriptions (including queue/topic names or subscription names within the same topic that differ only by case), missing subscription parents, and missing native route/consumer entities with actionable errors before touching the output stream. It never exports connection strings, credentials, processor settings or trusted-session assertions as infrastructure. Processor options and trusted assertions are application settings; the native `RequiresSession` entity property is exported.

The `dotnet relayer topology export --format json|bicep|terraform` tool entry point and SQS/SNS Terraform export remain **planned** ([#502](https://github.com/thomhurst/Relayer/issues/502), [#501](https://github.com/thomhurst/Relayer/issues/501)). Use the runnable application above for Service Bus exports today. Existing `ITopology.ExportJson` behavior is unchanged.

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

Payloads use .NET's `JsonTypeInfo.GetJsonSchemaAsNode` over the same source-generated contexts registered for JSON serialization. As with serialization, the last context covering a message wins. Register contexts through `AddMessageTypesFromThisAssembly()` or `UseJsonSerializer(context)`. Recursive schemas have their own `$id` so local references stay inside the payload. Export works with JSON reflection disabled and under Native AOT.

**Every registered message requires source-generated JSON metadata**, including unrouted messages and messages sent through a custom or named transport serializer. If any message lacks metadata, `ExportAsyncApi` throws `InvalidOperationException` before writing the document; it does not emit partial channels or operations. Registering a custom `IMessageSerializer` does not satisfy this requirement. For custom-only or mixed-format applications, provide JSON metadata for all messages to export their JSON contracts, or use `ExportJson` for declarations without payload schemas. The JSON contracts do not describe custom wire formats.

Supported format and limits:

- The document version is **AsyncAPI 3.0.0**. Payloads are Multi Format Schema Objects with **JSON Schema Draft 2020-12**, identified by `application/schema+json;version=draft-2020-12`. This custom schema format requires tooling that supports Draft 2020-12; AsyncAPI 3.0.0 tools are only required to support Draft 07.
- **No protocol bindings** are exported for Kafka, Service Bus, SQS/SNS, RabbitMQ, in-memory or custom transports. The document records transport names through `x-relayer-transport`, without guessing broker URLs, protocols, security settings or native options.
- Schemas describe the registered JSON contract. The exporter does not inspect custom or named transport serializers, schema registries, Protobuf, MessagePack or Avro wire formats. It omits `contentType` rather than claiming that every transport uses JSON. Custom JSON converters inherit .NET's schema coverage and may produce an unconstrained schema.
- Native headers, CloudEvents header encodings, Kafka tombstone bodies, native routing/bindings and broker-discovered entities are not modeled. Ordering requirements, consumer concurrency and batch size appear as `x-relayer-*` extensions.
- Export is explicit tooling work, outside message processing. Declaration and registration order determine output order. Identical declarations and metadata produce identical UTF-8 JSON; no timestamps or CLR type names are added. Both document title and application version must be non-empty. The destination remains open.
