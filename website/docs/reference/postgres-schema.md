---
title: PostgreSQL schema and scripts
description: "Create the shared outbox, dead outbox, inbox and lease tables with SQL or an application-owned EF Core migration."
---

Relayer implements PostgreSQL schema creation in `Relayer.Outbox.PostgreSql`, plus an EF Core migration
extension in `Relayer.Outbox.EntityFrameworkCore`. Schema creation does **not** enable durable sending,
transaction enlistment, dispatch, lease acquisition, poison-row handling or inbox deduplication by itself.
Register [EF Core transaction enlistment](ef-core-outbox.md) separately to persist outgoing rows.
[Ordered dispatch and lease acquisition](ordered-dispatch.md) and [unordered direct dispatch](direct-dispatch.md)
are implemented separately. Recovery polling and poison-row handling remain [planned](../roadmap.md).
[EF Core consumer inbox](ef-core-inbox.md) implements deduplication using the inbox table.
