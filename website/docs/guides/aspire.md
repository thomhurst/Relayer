---
title: Aspire integration
description: "Connect Relayer transports to Aspire resources and show health, traces and metrics in the dashboard."
---

`Relayer.Kafka` and `Relayer.AzureServiceBus` provide typed `UseAspireConnection`
extensions that read connections supplied by Aspire's native `WithReference`.
The optional `Relayer.Aspire` package subscribes OpenTelemetry to Relayer's existing
ActivitySource and Meter and references no transport packages. Applications reference
only the transports they use. These integrations do not create another broker client
or change delivery, settlement or outbox transactions.
While packages are unpublished, use project references from a repository checkout.

## AppHost resources

Use the standard `Aspire.Hosting.Kafka` and `Aspire.Hosting.Azure.ServiceBus`
integrations in your AppHost. Resource names identify connections; transport names
remain independent. This example uses the local Service Bus emulator:

```csharp
var kafka = builder.AddKafka("kafka");
var serviceBus = builder.AddAzureServiceBus("servicebus").RunAsEmulator();
serviceBus.AddServiceBusQueue("orders");

builder.AddProject<Projects.RelayerSample_Api>("api")
    .WithReference(kafka)
    .WithReference(serviceBus)
    .WaitFor(kafka)
    .WaitFor(serviceBus)
    .WithHttpEndpoint(env: "ASPNETCORE_HTTP_PORTS")
    .WithHttpHealthCheck("/health");
```

For existing brokers, use `builder.AddConnectionString("kafka")` or
`builder.AddConnectionString("servicebus")` with `WithReference`. Configure those
connections in your AppHost's configuration or secrets. Aspire owns endpoint
resolution and injects `ConnectionStrings__<name>` into the consuming service.
Never commit production credentials.

## Application registration

Keep the existing transport APIs, handlers and native options:

```csharp
using Relayer.AzureServiceBus;
using Relayer.Kafka;

builder.Services.AddRelayer(relayer =>
{
    relayer.AddMessageTypesFromThisAssembly();
    relayer.Kafka("events", kafka =>
    {
        kafka.UseAspireConnection(builder.Configuration, "kafka");
        kafka.TopologyMode = TopologyMode.None; // Kafka topology validation remains planned.
        kafka.Send<OrderPlaced>("orders");
        // kafka.Options.Configure(o => o.ConfigureProducer = native => ...);
    });
    relayer.ServiceBus("commands", bus =>
    {
        bus.UseAspireConnection(builder.Configuration, "servicebus");
        bus.Options.Configure(o => o.Queues.Add(new("orders")));
        bus.Send<CreateInvoice>("orders");
        // bus.Options.Configure(o => o.ClientOptions = new ServiceBusClientOptions { ... });
    });
});
```

Call `UseAspireConnection` once per transport, before further `Options` configuration.
Connections are captured at registration; restart the application to change them.
Missing or blank values fail during registration and identify the configuration key.
Kafka accepts comma-separated bootstrap endpoints, including bracketed IPv6 hosts;
empty entries fail early. This supplies addresses only: configure native Dekaf
producer, consumer and admin security independently, as described in the
[Kafka guide](../transports/kafka.md). No connection string value is included in errors.

Service Bus passes its connection string unchanged and preserves native
`ServiceBusClientOptions`. It creates a Relayer-owned client through the existing
transport. For Entra authentication or an Aspire-registered caller-owned
`ServiceBusClient`, use the existing `Client` option directly instead of
`UseAspireConnection`; configuring both still fails native transport validation.
See [Service Bus](../transports/service-bus.md) for topology and client ownership.

## Dashboard health and telemetry

Reference `Relayer.Kafka.HealthChecks` and `Relayer.AzureServiceBus.HealthChecks`
from the application and register the existing adapters explicitly:

```csharp
using Relayer.Aspire;
using Relayer.HealthChecks;

builder.Services.AddRelayerHealthChecks();
builder.Services.AddRelayerKafkaHealthProviders("events");
builder.Services.AddRelayerServiceBusHealthProviders("commands");
builder.Services.AddOpenTelemetry().AddRelayerInstrumentation();
// Later, after builder.Build():
app.MapHealthChecks("/health");
```

The AppHost's native `WithHttpHealthCheck("/health")` probes this endpoint and
surfaces readiness in the dashboard. Relayer's transport, consumer and outbox checks
use their existing semantics; configure [health options](health-checks.md) and
native security as usual. The endpoint is for internal orchestration; apply your
normal network/access policy if you expose it publicly.

`AddRelayerInstrumentation` adds only the `Relayer` source and meter to the existing
OpenTelemetry providers. Keep sampling, resource attributes, log configuration and
OTLP exporters in your ServiceDefaults or application. Do not register a second
exporter if ServiceDefaults already supplies one. Aspire injects OTLP endpoint and
authentication configuration when it starts the project. The sample calls
`UseOtlpExporter` when `OTEL_EXPORTER_OTLP_ENDPOINT` is configured. Send requests
then inspect **Traces**, **Metrics** and **Structured logs** in the dashboard; the
existing [telemetry conventions](telemetry.md) apply unchanged.

## Outbox connections

An outbox database is an ordinary Aspire database resource. For PostgreSQL, add
`Aspire.Hosting.PostgreSQL`, declare `builder.AddPostgres("postgres").AddDatabase("outbox")`,
then use `.WithReference(outbox).WaitFor(outbox)` on the application. Read its native
connection string when constructing the data source:

```csharp
using Npgsql;
using Relayer.HealthChecks;
using Relayer.Outbox.PostgreSql;

await using var dataSource = NpgsqlDataSource.Create(
    builder.Configuration.GetConnectionString("outbox")
        ?? throw new InvalidOperationException("Missing ConnectionStrings:outbox."));
builder.Services.AddRelayerPostgreSqlOutboxStore("orders", dataSource);
builder.Services.AddRelayerOutboxHealthProvider("orders");
builder.Services.AddRelayerHealthChecks(new RelayerHealthCheckOptions
{
    OutboxNames = ["orders"]
});
// Keep dataSource alive until the application and its workers have stopped.
```

Configure health checks once with all expected stores. Apply the
[PostgreSQL schema](../reference/postgres-schema.md) using your normal deployment
mechanism. Resource wiring does not create tables, enlist a transaction or start
dispatch workers. Register the existing [ADO.NET/Dapper](../reference/postgres-outbox.md)
or [EF Core](../reference/ef-core-outbox.md) transaction adapter and the appropriate
[dispatch](../reference/ordered-dispatch.md) or [recovery](../reference/postgres-maintenance.md)
services explicitly. SQL Server and MySQL likewise consume their native connection
strings through their [existing adapters](../reference/outbox-stores.md); Aspire
adds no transaction semantics.

## Runnable sample

The repository contains
[`samples/RelayerSample.AppHost`](https://github.com/thomhurst/Relayer/tree/main/samples/RelayerSample.AppHost)
and its API project. With .NET 10, Aspire CLI 13.6 and Docker running:

```shell
aspire start --non-interactive --apphost samples/RelayerSample.AppHost/RelayerSample.AppHost.csproj
aspire wait api --non-interactive --apphost samples/RelayerSample.AppHost/RelayerSample.AppHost.csproj
aspire describe --non-interactive --apphost samples/RelayerSample.AppHost/RelayerSample.AppHost.csproj
```

Use the returned API URL to `POST /kafka` and `POST /servicebus`. Both return 202
once the native send completes. `GET /health` reports the registered Relayer checks.
The Service Bus emulator is for local development and requires acceptance of its
license; Aspire's emulator integration handles its local configuration. The sample
provisions its Service Bus queue through Aspire and otherwise keeps Relayer's
normal Service Bus topology validation. Kafka explicitly uses `TopologyMode.None`
because Kafka topology validation remains planned; provision production topics using
your normal tooling. The local broker permits automatic topic creation. It demonstrates sending; add consumers through the
existing transport builders when needed. Outbox resource wiring is documented above
and is intentionally opt-in.

Stop this sample when finished:

```shell
aspire stop --non-interactive --apphost samples/RelayerSample.AppHost/RelayerSample.AppHost.csproj
```
