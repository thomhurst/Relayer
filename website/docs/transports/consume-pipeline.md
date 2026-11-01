---
title: Consume pipeline
description: "How a consumer composes generated handler descriptors, middleware and failure handling once at startup."
---

`ConsumePipelineBuilder` composes one consumer at startup. It selects generated
handler descriptors and stable identities from `HandlerRegistry` and the frozen
`MessageTypeRegistry`. It does not discover CLR types while receiving messages.
The first middleware registered with `Use` is outermost; middleware instances
are shared by the consumer and must support concurrent calls.
`Use(factory)` can inspect services in a temporary synchronous startup scope to create middleware;
resolved services must support synchronous disposal and the middleware must not retain scoped services.
The [EF Core consumer boundary](../reference/ef-core-inbox.md)
uses this seam to validate its provider and enable inbox deduplication for the selected outbox context.
Middleware implementing `IConsumeTransactionMiddleware` must be registered once, before any other
middleware. Composition rejects another transaction boundary or a boundary placed after middleware.

Transport authors can compose and invoke the execution layer as follows:

```csharp
// Once, after AddRelayer registration has completed:
var pipeline = new ConsumePipelineBuilder(scopeFactory, messageTypes,
        serializer, failures.HandleAsync)
    .Handler(handlers.GetHandler<OrderPlaced, OrderPlacedHandler>())
    .Tracing(tracing, endpoint)
    .Settle(SettleNativeMessageAsync)
    .Build(contextPoolCapacity: maxConcurrentDeliveries);

// Once per native delivery, after decoding its envelope or explicit raw mapping:
using var context = pipeline.RentContext(new ConsumeMessage
{
    MessageId = envelope.MessageId,
    MessageType = envelope.MessageType,
    Attempt = envelope.Attempt,
    Body = nativeBody,
    Headers = nativeHeaderView,
}, cancellationToken);
context.Features.Set(nativeFeature);
await pipeline.ProcessAsync(context).ConfigureAwait(false);
// The configured callback has completed native settlement inside the process span.
```

Each `Handler(descriptor)` consumes an already registered generated descriptor;
it does not register services. Transport configuration can pass non-generic
descriptors from `HandlerRegistry.Descriptors`: the typed descriptor base creates
its consume delegate without reflection, generic type construction or runtime
code generation. Register handlers inside `AddRelayer`
with `Handler<THandler>()` or `AddHandlersFromThisAssembly()` first. Several
stable message identities can share a consumer. One handler owns each identity;
that handler can coordinate several operations explicitly.

`ProcessAsync` creates a fresh DI scope for each message. Middleware, generated
handler resolution, invocation and failure handling use that scope. Processing
awaits asynchronous handler completion and asynchronous scope disposal before
returning. `MessageContext.Services` is available only during processing.
Cancellation propagates without entering the failure pipeline when the delivery
token is cancelled. A failure handler or scope-disposal error propagates too, so
the transport must not interpret it as successful processing.

The failure delegate is required. It receives the context, the original error
(or a contract error wrapping the serializer exception), and the classified
action. `DefaultFailureClassifier` classifies unknown identities and invalid
bodies as `DeadLetter`, and other errors as `Retry`. Applications can supply an
`IFailureClassifier` explicitly. An unknown type is never silently discarded:
it enters failure handling without deserializing or resolving a handler. The
delegate must finish durable failure handling before returning successfully;
it is the integration point for the failure policy and capability-aware receive
layer. The delegate constructor does not implement retries or transport fallbacks;
use the executable `FailurePipeline` constructor for those policies. The optional
`Settle` callback invokes adapter-owned native settlement after durable work and
scope disposal, inside the process span and duration. See [Tracing, metrics and logging](../guides/telemetry.md)
for `Tracing`, settlement wrappers and manual settlement.

Dispose each rental after awaiting processing, including error paths. Context
disposal clears body storage, metadata, services, cancellation and native feature
references before returning it to its consumer's bounded pool. Do not retain a
context or its native storage beyond that lifetime. The pool is preallocated;
size it for the maximum concurrent rentals. Overflow remains correct but allocates
temporary contexts rather than retaining an unbounded pool. A rental can be
processed once, by its owner, and cannot be disposed during processing.

This is the execution layer below the generic receive SPI in design section 2.4.
The SPI owns native binding, envelope decoding, native feature reuse, batch
delivery and dispositions through `ReceivePipeline<TIncoming, TBinding>`. See [Receive SPI](receive.md). The existing
`MessageContext.Headers` contract is retained here for that coordinated migration;
the pipeline does not enumerate or copy a transport's lazy header view.

## Performance measurements

Run the focused comparison with:

```powershell
dotnet run -c Release --project benchmarks/Relayer.Benchmarks -- --filter '*ConsumePipelineBenchmarks*'
```

Both cases create the same fresh Microsoft DI scope and resolve the same
generated handler. `ComposedConsumer` additionally rents a context, selects a
cached dispatcher by stable identity, runs one middleware, and returns the
context. A cached-reference serializer isolates pipeline allocations from
serializer allocations. The singleton and scoped-handler cases expose the DI
cost instead of hiding it in setup. Report total allocations and their difference:
equal allocation counts establish zero additional allocations from composition,
not zero total receive allocations. Fresh Microsoft DI scopes allocate even for
a singleton handler. No scopes or scoped handlers are reused to hide that cost.

This microbenchmark does not establish D18's end-to-end throughput budget against
a real native client, or its total receive allocation goal. The
[CI regression suite](../contributing/performance.md) includes these attribution
cases and end-to-end in-memory workloads; raw broker comparisons accompany each
adapter. Pending asynchronous operations
use a pooled `ValueTask` state machine in the execution layer; custom middleware
and user handlers retain responsibility for their own asynchronous allocations.

## Batch consumers

Configure the user call site with the same `Batch(maxSize, maxWait)` declaration:

```csharp
transport.Consume("audit", consumer => consumer
    .Handler<AuditHandler>()
    .Batch(maxSize: 10, maxWait: TimeSpan.FromMilliseconds(20)));
```

The transport composes one homogeneous batch handler at startup. Select its generated
`BatchHandlerDescriptor<T>` once; do not use reflection to find its message contract.
Pass the existing `IConsumerDefinition` to `BuildBatch` and `BatchReceiveExecutor<TIncoming>`:

```csharp
var execution = new ConsumePipelineBuilder(scopeFactory, messageTypes, serializer, failurePipeline)
    .Tracing(tracing, endpoint)
    .Settle(SettleNativeMessageAsync)
    .BuildBatch(handlers.GetBatchHandler<AuditEvent, AuditHandler>(), definition);
var receive = new ReceivePipeline<NativeDelivery, NativeBinding>(execution, headerNames,
    contextPoolCapacity: definition.BatchSize * definition.Concurrency);
var executor = new BatchReceiveExecutor<NativeDelivery>(services, transportName, definition,
    consumerName, nativeReceive: nativeBatchReceive, singleReceive: nativeSingleReceive);

// Inside the native receive loop; storage belongs to this loop until settlement finishes.
var count = await executor.ReceiveAsync(deliveries, cancellationToken).ConfigureAwait(false);
await receive.ProcessBatchAsync(deliveries[..count], dispositions[..count], cancellationToken)
    .ConfigureAwait(false);
// The settlement callback has run for each delivery inside the batch process span.
```

`BuildBatch` uses one fresh async scope per batch attempt. It rejects mixing
single-message middleware, dispatch, or endpoint metrics into that composition.
`Tracing` supplies the shared source and native endpoint metadata. The optional
`Settle` callback runs for each context after all durable work and scope disposal,
inside the batch process span. It reads the resolved native disposition and skips
`SettledByHandler`. Record batch metrics around the call. Without a settlement
callback, apply the returned dispositions through the native client's settlement API.
Message types not handled by that batch consumer enter failure handling individually.
Invalid bodies also enter failure handling individually; the handler sees only decoded
items, so partial-failure indexes refer to the batch actually passed to the handler.
A thrown handler exception fails every item passed to that invocation. Invalid result
indexes or null exceptions fail that invocation rather than losing a failure.
The executable `FailurePipeline` retries only failed items, in a fresh scope, then
produces durable per-message redelivery or dead-letter decisions. The explicit failure
delegate remains available to transport integrations that own durable failure handling.

Successful results wait for asynchronous scope disposal and buffered sends.
The batch-call token and each active delivery token are checked before handler
invocation and again after scope disposal, before flushing sends or handling failures.
Delivery cancellation also prevents that item from entering another retry invocation,
even when `ProcessBatchAsync` uses its default batch-call token. Cancellation propagates
without publishing batch dispositions; buffered sends from the cancelled attempt are discarded.
Failed attempts discard their buffered direct sends. A partially failed attempt with buffered
sends discards those sends and routes every item through the normal retry,
redelivery and dead-letter policy: one scoped sender cannot attribute those sends
to individual messages. Otherwise successful items can retry or dead-letter too;
no item settles successfully with its outgoing messages discarded. Use explicit per-message durable outbox transactions when
partial success also produces messages (D28). Failure handling and disposal errors
propagate without publishing any batch dispositions.

Configure `Tracing` with the application's shared `MessageTracing` to trace one
`process {source}` CONSUMER span for the batch, including retries. It links each valid
creation context and uses only the ambient span as parent. It includes
`messaging.batch.message_count` and the endpoint's sampling attributes. DI owns the source;
no process-wide source or second source owner is created. UTF-8 traceparent headers are parsed without allocating
an intermediate header string; UTF-8 tracestate decoding can still allocate.
The optional native settlement callback runs inside that span after durable processing.
It reuses the same operation wrappers as single-message consumers.
Do not retain the borrowed `Batch.Items`, message contexts, or native features after
handler completion. Pools retain bounded reusable storage sized by the declaration's
batch size and concurrency; overflow is temporary.

`BatchReceiveExecutor` resolves cached endpoint capabilities once during construction.
When a native executor exists and `MaxBatchSize` permits native batching, it uses that
executor with `min(BatchSize, MaxBatchSize)` slots and `BatchWait`. Native byte limits
can yield a smaller batch. Otherwise it requires an individual receive executor and
accumulates one delivery at a time, preserving delivery order. The partial-batch
deadline starts with the first delivery and never resets after later deliveries.
Each receive delegate must honor the supplied wait and cancellation, returning zero
on timeout. No background receives are left running. A declaration without either
usable executor fails construction with transport, source, consumer, and remedy.
Adapters must construct the executor before starting their receive services.
On receive exceptions, the caller still owns any deliveries already written into its
buffer and uses native shutdown/redelivery to release them; it must not acknowledge
them as successful. No broker or transport settlement behavior is emulated.

The `BatchConsumePipelineBenchmarks` comparison reports per-batch allocations against
matching fresh DI scope, scoped sender, cached serializer, and generated dispatch.
It does not establish the end-to-end native-client throughput budget; #19 owns that
transport regression measurement.
