# In-memory transport

The in-memory transport ships in `Relayer`. Use it for process-local delivery, tests and samples. Each instance owns its endpoints; named transports and service providers do not share messages.

```csharp
using Relayer.InMemory;

services.AddRelayer(relayer =>
{
    relayer.AddMessageTypesFromThisAssembly();
    relayer.InMemory("local", local =>
    {
        local.Send<OrderPlaced>("orders");
        local.Consume("orders", consumer => consumer.Handler<OrderHandler>());
    });
});
```

Declared endpoints already exist as native in-memory entities when the adapter is constructed. Topology validation and provisioning report no external differences; unknown names still fail.

Starting the host composes generated handlers into the core receive pipeline and starts native receivers. The default endpoint is a bounded queue with native scheduling, redelivery and dead-lettering. Handler failures go to the native dead-letter collection, so a poison message does not stall the queue. `Options.Failure` accepts an explicit core failure executor; a consumer with no native dead-letter capability must provide one. The common failure-policy implementation and durable fallbacks belong to the core failure pipeline.

Configure declared endpoints through named Options. An endpoint with no override uses queue defaults. An override for an undeclared endpoint fails startup.

```csharp
local.Options.Configure(options => options.Endpoints.Add("orders", new()
{
    Capacity = 128,
    Partitions = 4,
    Capabilities = new()
    {
        ConsumptionModel = Relayer.Transports.ConsumptionModel.Log,
        OrderedByKey = true,
        MaxDeliveryDelay = TimeSpan.FromHours(24),
        MaxRedeliveryDelay = TimeSpan.FromHours(1),
        DeadLetter = true,
        MaxBatchSize = 100,
    },
}));
local.Send<OrderPlaced>("orders", route => route.OrderingKey(order => order.CustomerId));
local.Consume("orders", consumer =>
{
    consumer.Options.Group = "billing";
    consumer.Concurrency(4).Handler<OrderHandler>();
});
```

Queue consumers compete for individual messages. Unordered queues use one partition and support multiple concurrent receivers. In ordered queues, a key hashes to one partition. Logs fan out records to each declared consumer group, assign positions in native partition order, and advance each group's position only after settlement. A group has one consumer declaration. Ordered queues and logs require concurrency equal to the partition count, with one serial receiver per partition. Retries hold that ordered partition's head; unordered queue retries allow ready messages to proceed. The core partition scheduler can later add finer key-level parallelism without changing native ordering.

`Capacity * Partitions` bounds all outstanding native messages, including scheduled sends, retries and messages retained by slow log groups. Sends await admission and observe caller cancellation. Scheduled sends complete when accepted, before their due time. The scheduler considers all accepted due times; a future message does not block immediate sends. Native size limits count UTF-8 body, header names, header values and ordering key. Batch sends preserve input order and may partially complete. Native replay, transactions, pause/resume and batch-handler composition are not implemented; requesting them fails explicitly.

`InMemoryTransport` also exposes the native SPI directly: construct it with explicit endpoints, register `InMemoryReceiveDelegate` callbacks before first start, and send `OutgoingMessage` values. Adapt a composed core consumer with `ReceivePipeline<InMemoryDelivery, InMemoryMessageBinding>`. `NativeMessage<InMemoryDelivery>` exposes the original receive view. Manual settlement uses `InMemoryDelivery.Complete`, `Redeliver` or `DeadLetter`; return `Disposition.SettledByHandler` after doing so. Do not retain native views or their storage beyond processing.

Stop first stops fetching and drains active handlers. Cancelling stop cancels active processing; unsettled messages and queued messages remain for restart. Unexpected receive or settlement failures fault `Completion` and stop fetching. The hosted service observes this failure through `BackgroundService`. `StopAsync` reports the failure; disposal releases resources without reporting the same processing failure again. Dispose closes admission and wakes blocked senders. Data and dead letters exist only in this process; disposal is not durable storage.

Inspect owned dead-letter snapshots with `transport.DeadLetters(endpoint)` and native group progress with `GetCommittedPosition`. Copied body, headers and UTF-8 ordering keys outlive the borrowed send buffers; completed messages return their storage to a bounded pool. Size `MessagePoolCapacity` for expected retained traffic and warm representative body/header sizes when measuring steady-state allocation.

The native adapter preserves `traceparent` and `tracestate` unchanged. Its system identifier is `InMemoryTransport.MessagingSystem` (`relayer.inmemory`). Shared tracing and span verification remain in #13 and the packaged transport conformance suite in #17. Parent #15 retains that integration acceptance; #87 supplies the native adapter and executable SPI contract tests.
