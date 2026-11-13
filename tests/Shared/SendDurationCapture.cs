using Relayer.Transports;
using System.Collections.Concurrent;
using System.Diagnostics.Metrics;

namespace Relayer.Tests;

internal sealed class SendDurationCapture : IDisposable
{
    private readonly MeterListener _listener = new();
    internal ConcurrentQueue<(double Seconds, Dictionary<string, object?> Tags)> Values { get; } = new();

    internal SendDurationCapture(RelayerInstrumentation instrumentation)
    {
        _listener.InstrumentPublished = (instrument, listener) =>
        {
            if (ReferenceEquals(instrument.Meter.Scope, instrumentation) && instrument.Name == "messaging.client.operation.duration")
                listener.EnableMeasurementEvents(instrument);
        };
        _listener.SetMeasurementEventCallback<double>((_, value, tags, _) =>
        {
            var snapshot = new Dictionary<string, object?>();
            foreach (var tag in tags) snapshot.Add(tag.Key, tag.Value);
            Values.Enqueue((value, snapshot));
        });
        _listener.Start();
    }

    public void Dispose() => _listener.Dispose();
}
