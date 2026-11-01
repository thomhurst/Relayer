---
title: Roadmap
description: "What is implemented in Relayer today and the phases that follow."
---

Relayer is in early development. Track progress in [GitHub issues](https://github.com/thomhurst/Relayer/issues).

## Implemented

- Configuration, named transports, routes and consumers, with startup validation of declarations, options and endpoint capabilities.
- Source-generated message registration, handler registration and dispatch, and System.Text.Json serialization.
- Required CI publishes and runs the Native AOT smoke app with reflection-disabled JSON, generated in-memory handler round trips, messaging spans and creation-context propagation assertions. Trimming and AOT warnings fail the build.
- Compile-time analyzers and a code fix ([diagnostics](reference/diagnostics.md)).
- The scoped send pipeline, send middleware and outbox enlistment hooks.
- Provider-owned tracing and direct single/batch send tracing, integrated with the in-memory adapter's delivery and native settlement spans. Outbox tracing remains planned.
- The [durable outbox store contract](reference/outbox-stores.md), with bounded reads, fenced claims and an optional keyed health adapter, plus the [PostgreSQL read/claim implementation](reference/postgres-store.md).
- [PostgreSQL schema scripts and database CLI](reference/postgres-schema.md), including an application-owned EF Core migration path.
- [EF Core transaction enlistment for PostgreSQL](reference/ef-core-outbox.md): implicit saves, explicit transactions, atomic rollback, multiple contexts and one batched insert per transaction.
- [Ordered PostgreSQL dispatch and lane leases](reference/ordered-dispatch.md).
- [EF Core consumer inbox](reference/ef-core-inbox.md): PostgreSQL deduplication, concurrent redelivery, atomic business/inbox/outbox commits and duplicate metrics. Automatic for the selected outbox context, opt-in for inbox-only consumers.
- [PostgreSQL ADO.NET and Dapper transactions](reference/postgres-outbox.md): scoped enlistment, owned message copies and one atomic batched insert on commit.
- [Unordered PostgreSQL direct dispatch](reference/direct-dispatch.md): background sends immediately after EF commit, fenced claims, native transport batches and fenced batch deletion. Recovery polling, poison-row handling and cleanup remain planned.
