using System.Diagnostics;
using Microsoft.Extensions.Logging;

namespace Relayer;

internal sealed class ConsumeTracing(MessageTracing tracing, MessagingEndpoint endpoint)
{
    private readonly MessageOperationTracing _process = new(tracing, endpoint, MessagingOperation.Process);
    internal ILogger Logger => tracing.Logger;

    internal Activity? Start(DefaultMessageContext context) => Start(tracing, _process, context, linkCreation: true);

    internal static Activity? Start(MessageTracing tracing, MessageOperationTracing operation, DefaultMessageContext context, bool linkCreation)
    {
        if (!tracing.Source.HasListeners()) return null;
        var data = new MessageTraceData
        {
            CreationContext = linkCreation ? MessageCreationContext.Read<DefaultMessageContext, HeaderReader>(context, context.EnvelopeNames) : default,
            PartitionId = context.TracePartitionId,
        };
        var activity = operation.Start(data, enrich: false);
        if (activity is null) return null;
        try
        {
            if (activity.IsAllDataRequested)
            {
                var names = context.EnvelopeNames;
                activity.SetTag("messaging.message.id", context.GetLogMessageId());
                activity.SetTag("cloudevents.event_type", context.GetLogMessageType());
                if (context.Headers.TryGetValue(names.Source, out var source)) activity.SetTag("cloudevents.event_source", source.ToString());
                if (context.Headers.TryGetValue(names.CorrelationId, out var correlation)) activity.SetTag("messaging.message.conversation_id", correlation.ToString());
                if (tracing.Options.IncludeBodySize) activity.SetTag("messaging.message.body.size", context.Body.Length);
                context.EnrichActivity(activity);
            }
            tracing.Enrich(activity);
            return activity;
        }
        catch
        {
            MessageOperationTracing.Fail(activity, "_OTHER");
            activity.Dispose();
            throw;
        }
    }

    private readonly struct HeaderReader : IMessageHeaderReader<DefaultMessageContext>
    {
        public static bool TryGetHeader(DefaultMessageContext context, string name, out WireString value) => context.Headers.TryGetValue(name, out value);
    }
}
