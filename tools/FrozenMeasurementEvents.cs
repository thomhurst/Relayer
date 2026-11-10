[System.Diagnostics.Tracing.EventSource(Name = "Relayer-FrozenMeasurement")]
sealed class FrozenMeasurementEvents : System.Diagnostics.Tracing.EventSource
{
    internal static readonly FrozenMeasurementEvents Log = new();
    [System.Diagnostics.Tracing.Event(1)]
    public void MeasurementStart() => WriteEvent(1);
    [System.Diagnostics.Tracing.Event(2)]
    public void MeasurementStop() => WriteEvent(2);
}
