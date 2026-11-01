using System.Buffers;
using System.Diagnostics;
using System.Threading.Channels;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Relayer;
using Relayer.InMemory;
using Relayer.Serialization;
using Relayer.Transports;

internal static class InMemoryTracingSmoke
{
    internal static async Task RunAsync()
    {
        var received = Channel.CreateBounded<InMemorySmokeResult>(1);
        var stopped = Channel.CreateUnbounded<Activity>();
        var services = new ServiceCollection();
        services.AddSingleton(received);
        services.AddRelayer(builder =>
        {
            builder.Telemetry();
            builder.AddMessageTypesFromThisAssembly();
            builder.InMemory("local", local =>
            {
                local.Send<Count>("counts");
                local.Consume("counts", consumer => consumer.Handler<InMemorySmokeHandler>());
            });
        });
        await using var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true });
        var tracing = provider.GetRequiredService<MessageTracing>();
        using var listener = new ActivityListener
        {
            ShouldListenTo = source => ReferenceEquals(source, tracing.Source),
            Sample = static (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
            ActivityStopped = activity => stopped.Writer.TryWrite(activity),
        };
        ActivitySource.AddActivityListener(listener);
        var hosted = provider.GetServices<IHostedService>().ToArray();
        try
        {
            foreach (var lifecycle in hosted.OfType<IHostedLifecycleService>()) await lifecycle.StartingAsync(default);
            foreach (var service in hosted) await service.StartAsync(default);
            using var scope = provider.CreateScope();
            var sender = scope.ServiceProvider.GetRequiredService<ISender>();
            foreach (var existingContext in new[] { false, true })
            {
                const string traceParent = "00-0123456789abcdef0123456789abcdef-0123456789abcdef-01";
                const string traceState = "relayer=aot";
                using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(10));
                using var request = new Activity("aot-request").SetIdFormat(ActivityIdFormat.W3C).Start();
                request.TraceStateString = traceState;
                var value = existingContext ? 124 : 123;
                if (!existingContext)
                {
                    await sender.SendAsync(new Count(value), deadline.Token);
                }
                else
                {
                    // Native sends can carry an existing creation context, as an outbox dispatch would.
                    var body = new ArrayBufferWriter<byte>();
                    provider.GetRequiredService<IMessageSerializer>().Serialize(new Count(value), body);
                    KeyValuePair<string, WireString>[] headers =
                    [
                        new("ce_type", new WireString("smoke.count;v=1")),
                        new("ce_id", new WireString("aot-existing-context")),
                        new("ce_source", new WireString("/aot-smoke")),
                        new("ce_specversion", new WireString("1.0")),
                        new("traceparent", new WireString(traceParent)),
                        new("tracestate", new WireString(traceState)),
                    ];
                    var outgoing = new OutgoingMessage("counts", body.WrittenMemory, headers);
                    await provider.GetServices<ITransport>().Single(transport => transport.Name == "local")
                        .SendAsync(outgoing, deadline.Token);
                }
                var delivery = await received.Reader.ReadAsync(deadline.Token);
                var spans = new Activity[3];
                // Handler completion precedes process-span disposal. Wait for the stopped spans, not a timer.
                for (var i = 0; i < spans.Length; i++) spans[i] = await stopped.Reader.ReadAsync(deadline.Token);
                var send = spans.Single(span => span.OperationName == "send counts");
                var process = spans.Single(span => span.OperationName == "process counts");
                var settle = spans.Single(span => span.OperationName == "complete counts");
                if (delivery.Value != value || delivery.ProcessContext != process.Context ||
                    !ActivityContext.TryParse(delivery.TraceParent, delivery.TraceState, true, out var creation))
                    throw new InvalidOperationException("In-memory generated-handler delivery or W3C headers failed under Native AOT.");
                if (delivery.TraceState != traceState || (existingContext && delivery.TraceParent != traceParent) ||
                    (!existingContext && (creation.TraceId != send.TraceId || creation.SpanId != send.SpanId)))
                    throw new InvalidOperationException("In-memory creation context was lost or overwritten under Native AOT.");
                if (send.Kind != (existingContext ? ActivityKind.Client : ActivityKind.Producer) ||
                    send.ParentSpanId != request.SpanId || send.TraceId != request.TraceId ||
                    process.Kind != ActivityKind.Consumer || process.ParentSpanId != creation.SpanId ||
                    process.TraceId != creation.TraceId || process.Links.Single().Context != creation ||
                    settle.Kind != ActivityKind.Client || settle.ParentSpanId != process.SpanId || settle.TraceId != process.TraceId)
                    throw new InvalidOperationException("In-memory send/process/settlement correlation failed under Native AOT.");
                if ((!existingContext && send.Links.Any()) ||
                    (existingContext && send.Links.Single().Context != creation))
                    throw new InvalidOperationException("In-memory send creation links failed under Native AOT.");
                CheckSpan(send, "send");
                CheckSpan(process, "process");
                CheckSpan(settle, "settle");
            }
        }
        finally
        {
            using var shutdown = new CancellationTokenSource(TimeSpan.FromSeconds(10));
            for (var i = hosted.Length - 1; i >= 0; i--) await hosted[i].StopAsync(shutdown.Token);
        }
        if (stopped.Reader.TryRead(out _))
            throw new InvalidOperationException("In-memory tracing emitted unexpected spans under Native AOT.");
        Console.WriteLine("In-memory generated-handler round trips, send/process/settlement spans and creation-context propagation passed under Native AOT.");
    }

    private static void CheckSpan(Activity span, string operation)
    {
        if (span.GetTagItem("messaging.system") is not "relayer.inmemory" ||
            span.GetTagItem("messaging.destination.name") is not "counts" ||
            !Equals(span.GetTagItem("messaging.operation.type"), operation) ||
            span.Status == ActivityStatusCode.Error || span.Source.TelemetrySchemaUrl != RelayerTelemetry.SchemaUrl)
            throw new InvalidOperationException("In-memory messaging span attributes failed under Native AOT.");
    }
}

public sealed record InMemorySmokeResult(int Value, string TraceParent, string TraceState, ActivityContext ProcessContext);

public sealed class InMemorySmokeHandler(Channel<InMemorySmokeResult> received) : IHandler<Count>
{
    public ValueTask HandleAsync(Count message, MessageContext context, CancellationToken cancellationToken)
    {
        if (context.Features.GetRequired<NativeMessage<InMemoryDelivery>>().Message.Message.Body.IsEmpty ||
            Activity.Current is not { } process ||
            !context.Headers.TryGetValue("traceparent", out var traceParent) ||
            !context.Headers.TryGetValue("tracestate", out var traceState))
            throw new InvalidOperationException("In-memory native binding, process span or creation headers were lost.");
        context.Features.GetRequired<ISettlementFeature>().Complete();
        if (!received.Writer.TryWrite(new(message.Value, traceParent.ToString(), traceState.ToString(), process.Context)))
            throw new InvalidOperationException("In-memory smoke handler received an unexpected extra delivery.");
        return default;
    }
}
