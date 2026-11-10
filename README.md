# Relayer

Modern .NET messaging that **wraps** the native client for your broker (Kafka, Azure Service Bus, Amazon SQS/SNS, RabbitMQ, and more) and handles the ceremony around it:

- resiliency (retries, delayed redelivery, dead-lettering, redrive)
- transactional outbox and inbox
- OpenTelemetry tracing, metrics, and trace propagation
- explicit topology
- a first-class test harness

Relayer never changes how the underlying technology sends, receives, orders, or acknowledges messages. It is Native AOT friendly, reflection-free on the hot path, and MIT licensed.

> **Status:** early development. Packages are not released yet. See the [roadmap](https://relayer.tomlonghurst.dev/docs/roadmap) for implemented features and planned work.

## Documentation

- [relayer.tomlonghurst.dev](https://relayer.tomlonghurst.dev/) — guides, concepts and reference (source in [`website/`](https://github.com/thomhurst/Relayer/tree/main/website))
- [Design](https://github.com/thomhurst/Relayer/blob/main/docs/design.md) — how Relayer works
- [Decision record](https://github.com/thomhurst/Relayer/blob/main/docs/decisions.md) — why it works that way
- [Health checks](https://relayer.tomlonghurst.dev/docs/guides/health-checks) — registration, adapter contracts and thresholds
- [Packages and releases](https://relayer.tomlonghurst.dev/docs/contributing/releases) — local artifact checks and publishing setup

## Building

Requires the .NET 11 SDK. Running the tests also needs the .NET 10 runtime, because the libraries target `net10.0` and `net11.0`.

```shell
dotnet build
dotnet test
```

Benchmarks:

```shell
dotnet run -c Release --project benchmarks/Relayer.Benchmarks
```
