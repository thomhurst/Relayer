using System.Diagnostics.Tracing;
using Microsoft.Extensions.Logging;

namespace Relayer.Tests;

// The SDK's native diagnostics expose per-client closure, settlement and automatic renewal.
// Only identifiers containing this fixture's unique entity path belong to this capture.
internal sealed class ServiceBusShutdownEvents(string destination, ILogger logger) : EventListener
{
    private readonly object _gate = new();
    private readonly HashSet<string> _closed = [];
    private readonly HashSet<string> _used = [];
    private readonly List<string> _violations = [];
    private int _completed;

    protected override void OnEventSourceCreated(EventSource eventSource)
    {
        if (eventSource.Name == "Azure-Messaging-ServiceBus") EnableEvents(eventSource, EventLevel.Verbose);
    }

    protected override void OnEventWritten(EventWrittenEventArgs args)
    {
        var payload = args.Payload;
        if (payload is null || !payload.OfType<string>().Any(value => value.Contains(destination, StringComparison.Ordinal))) return;
        lock (_gate)
        {
            // IDs are from Azure.Messaging.ServiceBus 7.21.0 ServiceBusEventSource.
            if (args.EventId == 45 && payload.Count > 1 && payload[1] is string closed) _closed.Add(closed);
            // Processor renewal events use the processor identifier, not its receiver's.
            // Its native stop completion is the quiescence boundary; it may later restart.
            if (args.EventId == 54 && payload[0] is string stopped) _closed.Add(stopped);
            if (args.EventId == 51 && payload[0] is string started) _closed.Remove(started);
            if (args.EventId is 1 or 16 or 19 or 22 or 31 or 47 or 56 or 59 && payload[0] is string identifier)
            {
                _used.Add(identifier);
                if (_closed.Contains(identifier)) _violations.Add($"{args.EventName} after {identifier} closed");
            }
            if (args.EventId == 17) _completed++;
            var message = args.EventName + ": " + string.Join(", ", payload);
            if (args.Level is EventLevel.Error or EventLevel.Critical) logger.LogError("{NativeEvent}", message);
            else if (args.Level == EventLevel.Warning && message.Contains("Cancel", StringComparison.OrdinalIgnoreCase))
                logger.LogWarning(new OperationCanceledException(message), "{NativeEvent}", message);
        }
    }

    internal void AssertClean(bool requireClosure)
    {
        lock (_gate)
        {
            if (_violations.Count != 0) throw new InvalidOperationException(string.Join("; ", _violations));
            if (_completed != 1) throw new InvalidOperationException($"Expected one broker-confirmed completion, saw {_completed}.");
            if (requireClosure && (_used.Count < 2 || !_used.IsSubsetOf(_closed)))
                throw new InvalidOperationException("Native Service Bus send/settlement clients did not all close: " + string.Join(", ", _used.Except(_closed)));
        }
    }
}
