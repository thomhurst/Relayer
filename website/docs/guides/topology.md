---
title: Topology
description: "Declared topology: validate or provision broker entities at startup, and export JSON, AsyncAPI, Service Bus infrastructure or AWS Terraform documents."
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

The `dotnet relayer topology export --format json|bicep|terraform` tool entry point remains **planned** ([#502](https://github.com/thomhurst/Relayer/issues/502)). SQS/SNS Terraform export is available through the explicit API below. Use the runnable application above for Service Bus exports today. Existing `ITopology.ExportJson` behavior is unchanged.

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

## AWS Terraform export

Install `Relayer.AmazonSqs` and `Microsoft.Extensions.DependencyInjection`, then call `ExportAmazonTerraform` on `ITopology`. This complete console application exports declarations without starting a host, resolving transport clients or contacting AWS:

```csharp
using Amazon.SQS.Model;
using Microsoft.Extensions.DependencyInjection;
using Relayer;
using Relayer.AmazonSqs;

var services = new ServiceCollection();
services.AddRelayer(relayer => relayer.AmazonSqs("aws", transport =>
{
    transport.Options.Configure(options => options.Queues.Add(new CreateQueueRequest("q")
    {
        Attributes = new() { ["VisibilityTimeout"] = "45" },
        Tags = new() { ["environment"] = "development" }
    }));
}));
using var provider = services.BuildServiceProvider();
using var output = File.Create("topology.tf.json");
provider.GetRequiredService<ITopology>().ExportAmazonTerraform(provider, output,
    new AmazonTerraformOptions { AccountId = "123456789012", Region = "eu-west-1" });
```

The generated file uses [Terraform JSON syntax](https://developer.hashicorp.com/terraform/language/syntax/json):

```json
{
  "resource": {
    "aws_sqs_queue": {
      "r_71": {
        "name": "q",
        "visibility_timeout_seconds": 45,
        "tags": { "environment": "development" }
      }
    }
  }
}
```

Configure the HashiCorp AWS provider separately for the **same account, region and partition** as the export. Put this beside the generated file in `provider.tf`, then run `terraform init`, `terraform validate` and `terraform plan` with your normal AWS credentials:

```hcl
terraform {
  required_providers {
    aws = { source = "hashicorp/aws", version = ">= 6.0" }
  }
}
provider "aws" {
  region              = "eu-west-1"
  allowed_account_ids = ["123456789012"]
}
```

The exporter reads named options from the supplied service provider. Use the provider that owns the topology. Every registered transport must be SQS or SNS; use a separate declaration-only service collection for an AWS-only export from a mixed application. Native client credentials, endpoints, retry settings and other connection configuration are not infrastructure declarations and are not exported. An explicitly configured native region must match the export region. Caller-owned clients are never inspected or called.

### Supported native declarations

Both `AmazonSqsTransportOptions.Queues` and SNS `QueueTopology.Queues` contribute `aws_sqs_queue` resources. SNS `Topics` contribute `aws_sns_topic`, and SQS `Subscriptions` contribute `aws_sns_topic_subscription`. Declare each queue/topic once across the topology; duplicates fail rather than combining conflicting declarations. Resource labels are `r_` followed by lowercase UTF-8 hex of the native name (subscriptions use topic ARN, `|`, then endpoint ARN). Resources, attributes and tags sort ordinally, so insertion order does not affect output. Literal `${` and `%{` sequences are escaped for Terraform, including inside policy strings.

| Declaration | Supported native attributes |
| --- | --- |
| Queue | `DelaySeconds`, `MaximumMessageSize`, `MessageRetentionPeriod`, `ReceiveMessageWaitTimeSeconds`, `VisibilityTimeout`, `FifoQueue`, `ContentBasedDeduplication`, `DeduplicationScope`, `FifoThroughputLimit`, `KmsMasterKeyId`, `KmsDataKeyReusePeriodSeconds`, `SqsManagedSseEnabled`, `RedrivePolicy`, `Policy`; native tags |
| Topic | `DisplayName`, `Policy`, `DeliveryPolicy`, `FifoTopic`, `ContentBasedDeduplication`, `KmsMasterKeyId`, `SignatureVersion`, `TracingConfig`, `FifoThroughputScope`; native tags |
| SQS subscription | `RawMessageDelivery`, `FilterPolicy`, `FilterPolicyScope`, `RedrivePolicy`, `DeliveryPolicy`, `SubscriptionRoleArn`; explicit topic and queue ARNs |

FIFO names and flags are preserved; the exporter does not infer them. When `MaximumMessageSize` is omitted, the exporter writes `max_message_size = 1048576` to preserve the native SQS 1 MiB default rather than inheriting a Terraform provider's smaller default. Explicit message size limits are preserved. Boolean values must be `true` or `false`, numeric settings must be nonnegative integers, and policy settings must be JSON objects. AWS/provider semantic validation still applies during `plan`/`apply`. `ReturnSubscriptionArn` controls only the native API response and has no Terraform setting.

### SNS relationships and access policies

Use native SNS `Topics`, `Subscriptions` and `QueueTopology` as described in the [SNS transport guide](../transports/amazon-sqs.md), with the same export call. An ARN exactly matching a declared entity in the export account/region/partition becomes a Terraform reference; other ARNs remain explicit external dependencies. For example, topic `t` and queue `q` produce subscription fields equivalent to:

```json
{
  "topic_arn": "${aws_sns_topic.r_74.arn}",
  "endpoint": "${aws_sqs_queue.r_71.arn}",
  "protocol": "sqs",
  "raw_message_delivery": true,
  "depends_on": ["aws_sqs_queue_policy.r_71"]
}
```

Queue `Policy` is emitted unchanged as an `aws_sqs_queue_policy` resource, referencing the queue URL and depending on declared topics. Subscriptions wait for exported queue policies. **Supply the policy explicitly**: the exporter never invents or broadens SNS permissions. Policy ARN strings remain literal, so policies must target the configured deployment identity. A queue's `RedrivePolicy` adds a dependency on its declared dead-letter queue when the ARN matches; externally managed dead-letter queues must already exist.

Unsupported native attributes (including queue `RedriveAllowPolicy`, SNS feedback settings and subscription replay settings), topic `DataProtectionPolicy`, non-SQS subscription protocols, malformed ARNs and other transports throw before any bytes are written to the destination. Remove unsupported settings from the exported declarations and manage them explicitly in separate Terraform resources. Streams remain open. The exporter buffers the document before copying it; this is tooling work and adds no per-message processing cost. Existing `ExportJson` output is unchanged.
