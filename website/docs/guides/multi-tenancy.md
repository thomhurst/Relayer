---
title: Tenant scopes
description: "Restore tenant headers before handler dependencies resolve and propagate them through scoped sends."
---

Register tenant propagation explicitly, before building your service provider:

```csharp
services.AddRelayerTenancy(contextPoolCapacity: 64);
services.AddRelayer(builder =>
{
    builder.AddMessageTypesFromThisAssembly();
    // Configure native transports, routes and single-message consumers here.
});
```

`TenantContext` is a scoped service. In an application scope, set its ID after authorizing the tenant:

```csharp
await using var scope = provider.CreateAsyncScope();
var tenant = scope.ServiceProvider.GetRequiredService<TenantContext>();
tenant.Id = new WireString(authorizedTenantId);
await scope.ServiceProvider.GetRequiredService<ISender>().SendAsync(message);
```

Every scoped send carries `relayer.tenant_id` in the existing native headers. This includes
`SendNowAsync`, outbox payloads, requests and replies. A send snapshots the tenant before invoking
send middleware. While a scoped tenant is set, `SendOptions.Headers` cannot override that header;
use a separate authorized scope to send for another tenant. An empty ID means no scoped tenant.
Explicit headers continue to work when no scoped tenant is set.

For incoming messages, Relayer restores the header before resolving the scoped sender, handler
constructor dependencies, or [EF Core transaction middleware](../reference/ef-core-inbox.md).
Inject `TenantContext` into a scoped handler or database factory to read the ID. Each in-process retry
gets a fresh scope restored from the original header. Missing or empty headers produce an empty ID.
Consumers in a provider without `AddRelayerTenancy` leave tenant headers unread.

**Authorize tenant IDs before using them to select resources.** Incoming headers are untrusted routing
metadata, not authentication. Reject missing or unauthorized tenants according to your application's
policy before opening a database connection or performing business work. Relayer does not validate
membership or choose a default tenant.

IDs use `WireString`, preserving native UTF-8 storage without decoding or copying. Calling `ToString()`
on UTF-8 allocates; use `Text`, `Utf8` or `TryFormat` where possible. Borrowed values must remain valid
until processing and sends complete. Do not retain `TenantContext` beyond its DI scope or dispose it
yourself. Relayer clears its ID and returns it to a provider-owned pool during scope disposal.
Size `contextPoolCapacity` for all concurrent application and consumer scopes. Above that capacity,
scopes still work but overflow contexts are allocated and only the configured number is retained.

Batch consumers share one scope, so composition rejects them when tenancy is registered. Use
single-message consumers and their native concurrency controls. Tenant propagation does not change
native ordering, settlement, retries, or transaction semantics.

## Tenant databases and dispatch

[Native tenant store selection](../reference/tenant-stores.md) uses application-owned scoped factories;
there is no separate Relayer `IOutboxStoreResolver`. With `AddRelayerTenancy`, single-message
`EntityFrameworkCore<TContext>` consumers compose without a startup DbContext or fallback tenant.
Authorize the incoming identifier in the scoped native factory before selecting the store. The
boundary validates the selected context before its transaction, including each native retry scope.

`ITenantSource` dispatch enumeration and a shared tenant connection budget remain planned under
[#38](https://github.com/thomhurst/Relayer/issues/38). The wire header is preserved in outbox payloads;
the separate durable `tenant_id` column is populated and validated through an application-authorized
[scoped tenant binding](../reference/tenant-stores.md#durable-tenant-binding).
