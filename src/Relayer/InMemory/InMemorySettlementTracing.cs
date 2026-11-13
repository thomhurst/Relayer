using Relayer.Transports;
using System.Diagnostics;
using Microsoft.Extensions.Logging;

namespace Relayer.InMemory;

internal sealed class InMemorySettlementTracing
{
    private readonly MessageTracing _tracing;
    private readonly MessageOperationTracing _complete;
    private readonly MessageOperationTracing _redeliver;
    private readonly MessageOperationTracing _deadLetter;

    internal InMemorySettlementTracing(MessageTracing tracing, string destination)
    {
        _tracing = tracing;
        var endpoint = new MessagingEndpoint { System = InMemoryTransport.MessagingSystem, Destination = destination };
        _complete = new(tracing, endpoint, MessagingOperation.Settle, "complete");
        _redeliver = new(tracing, endpoint, MessagingOperation.Settle, "redeliver");
        _deadLetter = new(tracing, endpoint, MessagingOperation.Settle, "dead_letter");
    }

    internal Activity? Start(Disposition disposition, string partition, InMemoryMessage? message)
    {
        if (!_tracing.Source.HasListeners()) return null;
        var operation = disposition.Kind switch
        {
            DispositionKind.Redeliver => _redeliver,
            DispositionKind.DeadLetter => _deadLetter,
            _ => _complete,
        };
        var activity = operation.Start(new() { PartitionId = partition }, enrich: false);
        if (activity is null) return null;
        if (activity.IsAllDataRequested && message is not null)
        {
            activity.SetTag("messaging.message.id", Header(message, "ce_id"));
            activity.SetTag("cloudevents.event_type", Header(message, "ce_type"));
            activity.SetTag("cloudevents.event_source", Header(message, "ce_source"));
            activity.SetTag("messaging.message.conversation_id", Header(message, "ce_correlationid"));
            if (_tracing.Options.IncludeBodySize) activity.SetTag("messaging.message.body.size", message.Body.Length);
        }
        _tracing.Enrich(activity);
        return activity;
    }

    internal bool Fail(Activity? activity, Exception error, InMemoryMessage? message)
    {
        MessageOperationTracing.Fail(activity, "_OTHER");
        try
        {
            var logger = _tracing.Logger;
            if (!logger.IsEnabled(LogLevel.Error)) return false;
            using var scope = logger.BeginScope(new ConsumeLogScope(Header(message, "ce_id"), Header(message, "ce_type"), Header(message, "ce_correlationid")));
            ConsumeLog.Failed(logger, error, activity?.TraceId.ToString(), activity?.SpanId.ToString());
            return true;
        }
        catch (Exception) { return false; } // Logging cannot replace a native settlement error.
    }

    private static string? Header(InMemoryMessage? message, string name)
    {
        if (message is null) return null;
        foreach (var header in message.Headers.Span)
            if (header.Key == name) return header.Value.ToString();
        return null;
    }
}
