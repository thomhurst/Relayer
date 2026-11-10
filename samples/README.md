# Relayer samples

Use the .NET 11 SDK and the .NET 10 runtime from a repository checkout (the samples target `net10.0`); packages are not published yet. These projects reference the actual library and source generator, and are included in `Relayer.slnx` so the normal Release build checks their call sites.

| Sample | Demonstrates | Infrastructure |
| --- | --- | --- |
| [ModularMonolith](ModularMonolith/README.md) | Checkout sends to billing through a process-local queue | None |
| [KafkaEfOutbox](KafkaEfOutbox/README.md) | PostgreSQL business transaction and EF outbox, Kafka dispatch and receive | PostgreSQL and Kafka |
| [ServiceBusWebApi](ServiceBusWebApi/README.md) | HTTP send and hosted queue handler | Azure Service Bus namespace or emulator |
| [RelayerSample.AppHost](RelayerSample.AppHost) | Existing Aspire orchestration and transport health | Docker and Aspire |

The standalone samples have no dependency on the Aspire sample. Run all commands below from the repository root unless a README says otherwise. Never use the development credentials or demonstration database initialization in production.
