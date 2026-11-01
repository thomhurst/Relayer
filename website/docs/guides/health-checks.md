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
The core package has no health-check dependency. Health polls add no work to the message pipeline.

## Explicit adapters

Native broker connectivity and outbox health adapters are not supplied yet. This package defines the adapter
seams; it does not claim that registering a transport makes its native client healthy. Applications or future native
integrations register their adapters explicitly as keyed services. Without these adapters, configured transports,
consumers and stores report unhealthy. Built-in adapter work is tracked with the
[Kafka](https://github.com/thomhurst/Relayer/issues/21),
[Service Bus](https://github.com/thomhurst/Relayer/issues/24),
[SQS/SNS](https://github.com/thomhurst/Relayer/issues/39),
[RabbitMQ](https://github.com/thomhurst/Relayer/issues/40) and
[outbox foundation](https://github.com/thomhurst/Relayer/issues/28) implementations:

```csharp
// Adapter types implement the contracts below and use the application's existing clients/state.
builder.Services.AddKeyedSingleton<ITransportHealthProbe, OrdersConnectivity>("orders-broker");
builder.Services.AddKeyedSingleton<IConsumerHealthProvider, OrdersConsumers>("orders-broker");
builder.Services.AddKeyedSingleton<IOutboxHealthProvider, OrdersBacklog>("orders-store");
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
  scheduled delivery time is in the future. Return null if the count is unavailable. List every expected store
  in `OutboxNames`, including stores whose adapter is not available yet. No store declaration model exists in
  the current outbox placeholders. A registered `IOutboxEnlistment` with an empty `OutboxNames` fails health to
  catch an accidentally omitted declaration without constructing a scoped transaction service.

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
