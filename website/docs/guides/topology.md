---
title: Topology
description: "Declared topology: validate or provision broker entities at startup, and export the topology as JSON."
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
