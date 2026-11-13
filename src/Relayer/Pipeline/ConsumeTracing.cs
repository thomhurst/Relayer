using Relayer.Transports;
using System.Diagnostics;
using Microsoft.Extensions.Logging;

namespace Relayer.Pipeline;

internal sealed class ConsumeTracing(MessageTracing tracing, MessagingEndpoint endpoint)
{
    private readonly MessageOperationTracing _process = new(tracing, endpoint, MessagingOperation.Process);
    internal ILogger Logger => tracing.Logger;

    internal Activity? Start(DefaultMessageContext context) => Start(tracing, context.ProcessTracing ?? _process, context, linkCreation: true);

    internal KeyValuePair<string, object?>[] CreateBatchTags() => _process.CreateBatchTags();

    internal Activity? StartBatch(ReadOnlyMemory<DefaultMessageContext> contexts,
        KeyValuePair<string, object?>[] tags, List<ActivityLink> links, KeyValuePair<string, object?> countTag)
    {
        if (!tracing.Source.HasListeners()) return null;
        for (var i = 0; i < contexts.Length; i++)
        {
            var context = contexts.Span[i];
            var creation = MessageCreationContext.Read<DefaultMessageContext, HeaderReader>(context, context.EnvelopeNames);
            if (creation != default) links.Add(new(creation));
        }
        tags[^1] = countTag;
        return _process.StartBatch(tags, links);
    }

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
