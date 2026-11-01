# Consume pipeline composition

`ConsumePipelineBuilder` composes one consumer at startup. It selects generated
handler descriptors and stable identities from `HandlerRegistry` and the frozen
`MessageTypeRegistry`. It does not discover CLR types while receiving messages.
The first middleware registered with `Use` is outermost; middleware instances
are shared by the consumer and must support concurrent calls.

Transport authors can compose and invoke the execution layer as follows:

```csharp
// Once, after AddRelayer registration has completed:
var pipeline = new ConsumePipelineBuilder(scopeFactory, messageTypes,
        serializer, failures.HandleAsync)
    .Handler(handlers.GetHandler<OrderPlaced, OrderPlacedHandler>())
    .Use(telemetryMiddleware)
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
// The transport's receive layer now performs its native settlement.
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
layer. This execution layer does not implement retries, transport fallbacks or
native settlement itself.

Dispose each rental after awaiting processing, including error paths. Context
disposal clears body storage, metadata, services, cancellation and native feature
references before returning it to its consumer's bounded pool. Do not retain a
context or its native storage beyond that lifetime. The pool is preallocated;
size it for the maximum concurrent rentals. Overflow remains correct but allocates
temporary contexts rather than retaining an unbounded pool. A rental can be
processed once, by its owner, and cannot be disposed during processing.

This is the execution layer below the generic receive SPI in design section 2.4.
The SPI owns native binding, envelope decoding, native feature reuse, batch
delivery and dispositions. Its integration is tracked in #55. The existing
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
a real native client, or its total receive allocation goal. Those require the
transport and CI regression suites tracked in #19. Pending asynchronous operations
use a pooled `ValueTask` state machine in the execution layer; custom middleware
and user handlers retain responsibility for their own asynchronous allocations.
