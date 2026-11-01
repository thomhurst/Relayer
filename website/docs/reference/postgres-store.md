---
title: PostgreSQL durable store
description: "Register PostgreSQL outbox reads, atomic claims, fenced outcomes and backlog health queries."
---

`Relayer.Outbox.PostgreSql` implements `IOutboxStore` over the [shared PostgreSQL schema](postgres-schema.md).
It supplies durable reads, atomic claims, renewal, completion, failure persistence and eligible backlog counts.
[EF Core transaction enlistment](ef-core-outbox.md) and [ADO.NET/Dapper transactions](postgres-outbox.md)
supply atomic enqueue. [Unordered direct dispatch](direct-dispatch.md) can send due rows immediately after
an EF commit. [Ordered dispatch with lane leases](ordered-dispatch.md) is available through separate
registration. Recovery polling remains planned.
[Inbox deduplication](ef-core-inbox.md) is available through the EF Core consumer boundary.
