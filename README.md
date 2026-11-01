# Relayer

Modern .NET messaging that **wraps** the native client for your broker (Kafka, Azure Service Bus, Amazon SQS/SNS, RabbitMQ, and more) and handles the ceremony around it:

- resiliency (retries, delayed redelivery, dead-lettering, redrive)
- transactional outbox and inbox
- OpenTelemetry tracing, metrics, and trace propagation
- explicit topology
- a first-class test harness

Relayer never changes how the underlying technology sends, receives, orders, or acknowledges messages. It is Native AOT friendly, reflection-free on the hot path, and MIT licensed.

> **Status:** early design and scaffolding. Not ready for use.

## Documentation

- [relayer.tomlonghurst.dev](https://relayer.tomlonghurst.dev/) — guides, concepts and reference (source in [`website/`](website/))
- [Design](docs/design.md) — how Relayer works
- [Decision record](docs/decisions.md) — why it works that way

## Building

Requires the .NET 10 SDK.

```shell
dotnet build
dotnet test
```

Benchmarks:

```shell
dotnet run -c Release --project benchmarks/Relayer.Benchmarks
```
