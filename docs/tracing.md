# Tracing primitives

The shared tracing foundation is available for pipeline and transport composition. Automatic send and consume integration is tracked by #92 and #93; registering these primitives alone does not instrument `ISender` or `ReceivePipeline` yet. Outbox integration is tracked by #36.

Register one source owner per service provider. Repeated registration shares the source within that provider; separate providers own separate sources named `Relayer`:

```csharp
services.AddRelayer(relayer => relayer.Telemetry());
services.AddOptions<TracingOptions>().Bind(configuration.GetSection("Relayer:Tracing"));
services.AddSingleton<IActivityEnricher, ApplicationEnricher>();
```

`TracingOptions` uses init-only properties. `CreationContextAsParent` and `CreateSpansForBatches` default to true; `IncludeBodySize` defaults to false. The source is named `Relayer` and carries schema URL `https://opentelemetry.io/schemas/1.37.0`. This pin matches the `messaging.system` attribute mapping in design §8 and [semantic conventions 1.37.0](https://github.com/open-telemetry/semantic-conventions/blob/v1.37.0/docs/messaging/messaging-spans.md). Future changes to that pin require a release note and a mapping review.

Adapters compose operations once at startup, using native metadata rather than the configured transport instance name:

```csharp
var endpoint = new MessagingEndpoint
{
    System = "kafka",
    Destination = "orders",
    ConsumerGroup = "billing",
    ServerAddress = "broker",
    ServerPort = 9092,
};
var process = new MessageOperationTracing(tracing, endpoint, MessagingOperation.Process);
var commit = new MessageOperationTracing(tracing, endpoint, MessagingOperation.Settle, "commit");
```

`Start` returns an `Activity` to dispose after the operation finishes. It validates batch metadata independently of listeners, then returns null before tag or link work when no listener is attached. Batch counts must be positive, and `Create` never accepts batch metadata, even for an existing creation context. Single processing links to `MessageTraceData.CreationContext`; absent an ambient activity, that context is also the parent unless disabled. An ambient activity always takes precedence. Set `BatchCount` for batch operations and supply one `ActivityLink` per message in `Links`. Batch processing uses links only and never chooses a message as parent. Batch spans omit single-message attributes, even when a batch contains one message.

For batch sending, check `tracing.Options.CreateSpansForBatches`. When true, start and dispose a `Create` operation per message that has no creation context, then start a batch `Send` operation with links to those contexts. When false, skip the create operations; a send without existing creation contexts or links is a producer, and its context becomes the creation context for every new message. A send with an existing creation context or links is a client span. Settle operations are client spans with the native operation name; start them while processing is current so settlement remains inside the process duration.

Use `MessageCreationContext.Read<TMessage, TReader>` with the adapter's header binding to read remote W3C context. UTF-8 values are decoded on demand. `WriteIfAbsent<TMessage, TBinding>` writes only when traceparent is absent, preserving external, resend, and dead-letter headers byte for byte. Preserve the chosen context through retries and use it when writing the envelope. The native client's instrumentation must also preserve it. The tracing source is not needed for propagation.

Call `MessageOperationTracing.Fail(activity, errorType)` on failure. Pass a native error code, a cached exception type name, or `_OTHER` (also used for missing or whitespace error types); this avoids reflection on the message path. Log the exception with its trace IDs in the owning pipeline; the helper emits no exception event. Enrichers run only for recorded spans, must support concurrency, and should not throw. Enricher exceptions are ignored so diagnostics do not interrupt delivery. They can add native per-message attributes or rename spans. Supply partition IDs in `MessageTraceData` so samplers see them at span start.

`TracingBenchmarks` measures these primitives with and without a listener. It does not measure native broker overhead or the unfinished send and receive integration. Tracing tests run in a dedicated executable because TUnit's HTML reporter attaches a listener to every activity source, which would change sampling and invalidate no-listener measurements.
