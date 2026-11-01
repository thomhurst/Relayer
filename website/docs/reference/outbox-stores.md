---
title: Durable outbox store contract
description: "Durable row reads, fenced claims, memory ownership and optional health integration for outbox provider authors."
---

The core package defines `IOutboxStore`, `OutboxRecord`, `OutboxReadRequest` and `OutboxClaim` for database
integration authors. These contracts, the optional health adapter and [PostgreSQL schema scripts](postgres-schema.md)
are implemented, together with [PostgreSQL durable reads and fenced claims](postgres-store.md)
and [EF Core transaction enlistment for PostgreSQL](ef-core-outbox.md).
[Unordered PostgreSQL direct dispatch](direct-dispatch.md) is available for EF commits.
[Ordered PostgreSQL dispatch and lane leases](ordered-dispatch.md) are implemented separately.
Recovery polling remains planned.
[Inbox deduplication](ef-core-inbox.md) is implemented
through the EF Core consumer boundary, separately from the read/claim store contract.
