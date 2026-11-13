---
title: Health checks
description: "Readiness checks for transport connectivity, consumer liveness and outbox backlogs, with explicit adapter registration."
---

Reference `Relayer.HealthChecks` and register its three checks in the application:

```csharp
using Relayer.HealthChecks;

var builder = WebApplication.CreateBuilder(args);
builder.Services.AddRelayer(relayer =>
{
    // Declare transports, routes and consumers here.
});
builder.Services.AddRelayerHealthChecks();
var app = builder.Build();
app.MapHealthChecks("/health/ready");
app.Run();
```

The call returns the standard `IHealthChecksBuilder`. It works before or after `AddRelayer` and registers
`relayer.transport`, `relayer.consumer` and `relayer.outbox`, tagged `relayer` and `ready`. Calling it again
without options does not duplicate checks. Supplying options again throws to prevent conflicting configuration.
In a standalone `ServiceCollection`, also register logging before resolving `HealthCheckService`, as required by
Microsoft's health service. Normal application hosts already provide logging.

These checks describe readiness. Do not use broker connectivity or a dispatch backlog to trigger process restarts.
The core package has no health-check dependency. Polls read receiver snapshots; optional native adapters
record progress in their receive paths. Kafka health tracking adds no per-message allocations. Service Bus tracks
active callback slots only when its optional adapter is registered, and that tracking adds no allocation to
the callback. Tracking never affects delivery: a callback that finds no free slot is processed but not reported.

## Explicit adapters

`Relayer.HealthChecks` supplies an outbox health adapter for `IOutboxStore`; use the [PostgreSQL read/claim store](../reference/postgres-store.md)
or an application-supplied store.
Registering a transport does not establish native connectivity. Register adapters explicitly as keyed services.
For Kafka, reference `Relayer.Kafka.HealthChecks` and call `AddRelayerKafkaHealthProviders("orders-broker")`
with the exact transport name; see [Kafka health checks](../transports/kafka.md#health-checks) for native
configuration and receiver state.

For Service Bus, reference `Relayer.AzureServiceBus.HealthChecks` and call
`services.AddRelayerServiceBusHealthProviders("orders-broker")` before host startup, in addition to
`AddRelayerHealthChecks`. See [Service Bus health](../transports/service-bus.md#health).
Connectivity opens native send links or peeks receive endpoints without sending, consuming, locking or
settling probe messages. Receiver snapshots report the oldest active callback's activation time;
stopped processors report unavailable state.

For RabbitMQ, reference `Relayer.RabbitMQ.HealthChecks` and call
`services.AddRelayerRabbitMqHealthProviders("orders-broker")` before host startup. It reads existing native
connection state without probe messages and reports the oldest active callback or explicit idle state.
See [RabbitMQ health](../transports/rabbitmq.md#health).

For SQS, reference `Relayer.AmazonSqs.HealthChecks` and call
`services.AddRelayerAmazonSqsHealthProviders("orders-broker")` before host startup.
Native queue-attribute queries establish connectivity without probe messages. Receiver-owned snapshots
report idle state or the oldest active worker. See [SQS health](../transports/amazon-sqs.md#health).

Without adapters, configured transports, consumers and stores report unhealthy.
Applications can also supply their own providers, including an outbox store:

```csharp
using Relayer.Transports;
// Adapter types implement the contracts below and use the application's existing clients/state.
builder.Services.AddKeyedSingleton<ITransportHealthProbe, OrdersConnectivity>("orders-broker");
builder.Services.AddKeyedSingleton<IConsumerHealthProvider, OrdersConsumers>("orders-broker");
// OrdersOutboxStore is an application implementation of IOutboxStore, not a built-in database provider.
builder.Services.AddKeyedScoped<IOutboxStore, OrdersOutboxStore>("orders-store");
builder.Services.AddRelayerOutboxHealthProvider("orders-store");
builder.Services.AddRelayerHealthChecks(new RelayerHealthCheckOptions
{
    OutboxNames = ["orders-store"],
    ConsumerStallTimeout = TimeSpan.FromMinutes(2),
    OutboxDegradedThreshold = 500,
    OutboxUnhealthyThreshold = 5_000
});
```

Use exactly the case-sensitive transport name declared in `AddRelayer`. Register one provider of each kind
per key. The consumer provider serves all consumer declarations on that transport. Checks resolve adapters
inside the health service's scope; adapters may be scoped if their native dependencies require it.

- `ITransportHealthProbe.CheckAsync` returns `HealthCheckResult` from a read-only native connectivity operation.
  Keep native status, description, data and exception. Do not publish a test message, consume a message, invent
  broker semantics or create a separate client per poll.
- `IConsumerHealthProvider.GetStateAsync` receives the exact `IConsumerDefinition` from `RelayerConfiguration`,
  so declarations on the same source remain distinct. Return `ConsumerHealthState` from receiver-owned state.
  `IsIdle` is true only when all workers are waiting for messages. For active workers, `LastProgressAt` is the
  oldest last-progress time, or activation time before first progress. One busy worker must not hide another
  stalled worker. Return null if state is unavailable; lack of progress alone does not establish idleness.
- `IOutboxHealthProvider.GetPendingCountAsync` returns pending rows eligible for dispatch. Exclude rows whose
  scheduled delivery time is in the future, parked/dead rows and rows with live claims. Return null if the count
  is unavailable. List every expected store in `OutboxNames`, including stores whose adapter is not available yet.
  A registered `IOutboxEnlistment` with an empty `OutboxNames` fails health to
  catch an accidentally omitted declaration without constructing a scoped transaction service.

`AddRelayerOutboxHealthProvider(name)` registers a scoped keyed `IOutboxHealthProvider` that resolves the
keyed `IOutboxStore` with exactly the same case-sensitive name. The adapter queries
`GetPendingCountAsync(now, cancellationToken)` with the DI `TimeProvider`'s current UTC instant on every poll.
It preserves null counts, errors and cancellation. EF Core integrations can use scoped store dependencies;
the health scope disposes them after the poll. Repeated registration preserves an existing provider for that name.
Applications may still register `IOutboxHealthProvider` directly for other storage integrations.

Register the adapter in addition to `AddRelayerHealthChecks`, and include its name in `OutboxNames`.
Adapter registration does not change the expected names, create a database store, or start dispatch. A missing
keyed store reports unhealthy. An adapter registered through this extension whose name is absent from
`OutboxNames` also reports unhealthy, including when another declared store is healthy.
See the [durable store contract](../reference/outbox-stores.md) for provider requirements.

A missing provider fails its configured resource. Null consumer state or backlog, a negative count and an active
consumer's future progress timestamp also fail health. No configured resources in a category means healthy
with an explicit description. All resources in a category are checked; the worst status wins. The description
names every resource. Each result's `Data` retains the individual `HealthCheckResult`, including native data and
exceptions. Consumer data keys include transport, declaration index and source. Backlog result data includes
`pending_count`. The aggregate exception is the first reported exception; other exceptions remain in the data.

Idle consumers are healthy regardless of elapsed time. Active consumers become unhealthy at or beyond
`ConsumerStallTimeout`. Backlogs become degraded at or beyond `OutboxDegradedThreshold` and unhealthy at or
beyond `OutboxUnhealthyThreshold`. Defaults are five minutes, 1,000 rows and 10,000 rows respectively.
All thresholds must be positive; the unhealthy threshold must exceed the degraded threshold. Outbox names must
be nonblank and unique. Invalid options fail host startup through Options validation.

All adapters must observe the supplied cancellation token. Caller cancellation propagates; an unrelated native
timeout or exception becomes unhealthy with the original exception and useful failure text. Health polling does
not swallow cancellation or turn unavailable probes into successful results.

## Configuration and Options

Use the configuration overload for immutable options; it binds explicitly without reflection and works under
Native AOT. The .NET configuration binding generator skips init-only setters, so do not use its generic `Bind`
for these options.

```csharp
builder.Services.AddRelayerHealthChecks(builder.Configuration.GetSection("RelayerHealth"));
```

```json
{
  "RelayerHealth": {
    "ConsumerStallTimeout": "00:02:00",
    "OutboxDegradedThreshold": 500,
    "OutboxUnhealthyThreshold": 5000,
    "OutboxNames": ["orders-store"]
  }
}
```

Options use `IOptions<RelayerHealthCheckOptions>`, `IOptionsSnapshot<RelayerHealthCheckOptions>` and
`IOptionsMonitor<RelayerHealthCheckOptions>` as usual, including registered validators. The checks read current
monitor values for each poll. Configuration reload rebinds and validates the values. Values use invariant
culture; malformed numbers or durations raise `OptionsValidationException` with the offending key.
Applications may replace the DI `TimeProvider` to control consumer-time measurements in tests.
