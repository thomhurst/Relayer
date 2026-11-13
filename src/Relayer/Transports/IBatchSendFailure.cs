namespace Relayer.Transports;

/// <summary>Exposes confirmed native successes when a batch send throws.</summary>
/// <remarks>Implemented by transport exceptions. Unconfirmed messages may have been delivered;
/// retrying them preserves at-least-once semantics and can duplicate delivery.</remarks>
public interface IBatchSendFailure
{
    /// <summary>Whether the native client confirmed delivery of the zero-based message in the supplied batch.</summary>
    bool IsConfirmed(int messageIndex);
}
