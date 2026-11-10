---
title: Runnable samples
description: "Build and run the in-memory, Kafka EF Core outbox and Azure Service Bus examples."
---

Clone [the repository](https://github.com/thomhurst/Relayer) and use the .NET SDK selected by `global.json`. Packages are not published yet; each sample references the real library and source generator projects. `dotnet build -c Release` at the repository root builds these samples with the solution.

| Sample | Run from repository root | Expected result |
| --- | --- | --- |
| [Modular monolith](https://github.com/thomhurst/Relayer/tree/main/samples/ModularMonolith) | `dotnet run -c Release --project samples/ModularMonolith` | Prints an invoice and exits after billing handles the event |
| [Kafka and EF Core outbox](https://github.com/thomhurst/Relayer/tree/main/samples/KafkaEfOutbox) | `dotnet run -c Release --project samples/KafkaEfOutbox -- --urls http://localhost:5081` | POST `/orders/order-1` commits business data and an event, then a Kafka consumer logs it |
| [Service Bus web API](https://github.com/thomhurst/Relayer/tree/main/samples/ServiceBusWebApi) | `dotnet run -c Release --project samples/ServiceBusWebApi -- --urls http://localhost:5082` | POST `/orders/order-1` sends to a queue and a hosted handler logs receipt |

Read each linked README before running. The Kafka example needs PostgreSQL, externally created source/dead-letter topics and connection settings. It initializes an empty demonstration database, registers unordered direct dispatch and maintenance recovery, and makes no exactly-once claim. The Service Bus example needs an existing non-session queue and a namespace or emulator connection string with Send and Listen rights. It validates topology without creating it.

The [getting started](../getting-started.md) call site follows the modular monolith. Transport-specific details stay in the [Kafka](../transports/kafka.md) and [Service Bus](../transports/service-bus.md) guides; persistence details stay in [EF enlistment](../reference/ef-core-outbox.md), [direct dispatch](../reference/direct-dispatch.md) and [recovery](../reference/postgres-maintenance.md). The existing [Aspire guide](aspire.md) covers the separate orchestration/health sample.
