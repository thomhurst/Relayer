namespace Relayer;

/// <summary>Converts an older message contract to a newer version of the same stable message name.</summary>
/// <remarks>Instances are shared across consumers. Implementations must support concurrent calls and should avoid allocations.</remarks>
public interface IUpcaster<TFrom, TTo>
{
    /// <summary>Converts a deserialized message without changing its native envelope or settlement.</summary>
    TTo Upcast(TFrom message);
}
