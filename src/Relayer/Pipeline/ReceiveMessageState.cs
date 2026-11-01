using System.Buffers;

namespace Relayer;

internal abstract class ReceiveMessageState : MessageHeaders
{
    internal abstract HeaderNames EnvelopeNames { get; }
    internal abstract string MessageId { get; }
    internal abstract MessageType MessageType { get; }
    internal abstract int Attempt { get; }
    internal abstract ReadOnlySequence<byte> Body { get; }
}
