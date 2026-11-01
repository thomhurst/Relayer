namespace Relayer;

/// <summary>The wire format used by a route or consumer.</summary>
public enum MessageEnvelopeMode
{
    /// <summary>CloudEvents attributes in native headers.</summary>
    CloudEvents,
    /// <summary>Native payload only; the consumer supplies the wire type explicitly.</summary>
    Raw
}
