---
title: EF Core transaction enlistment
description: "Persist outgoing messages atomically with PostgreSQL business changes using EF Core interceptors."
---

`Relayer.Outbox.EntityFrameworkCore` implements transaction enlistment for the Npgsql EF Core provider.
It buffers serialized sends, copies borrowed memory before each send completes, and writes one batched
PostgreSQL `INSERT` immediately before commit. The insert uses the same connection and transaction as
the business data. No `SaveChanges` override or outbox entity mapping is required.

Enlistment alone persists rows. Enable [unordered direct dispatch](direct-dispatch.md) to send due messages
in the background after commit, or register [ordered dispatch and lane leases](ordered-dispatch.md) separately.
Register the [PostgreSQL durable store](postgres-store.md) separately for reads, claims and backlog health queries.
Recovery polling, cleanup and outbox tracing remain planned.
The [EF Core consumer boundary](ef-core-inbox.md) supplies transactional inbox deduplication and
enables it automatically for its selected outbox context.
