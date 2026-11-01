namespace Relayer.Transports;

/// <summary>The endpoint's native consumption and settlement model.</summary>
public enum ConsumptionModel
{
    /// <summary>Messages are received and settled individually.</summary>
    Queue,

    /// <summary>Records are consumed by position and progress advances through a log.</summary>
    Log,
}
