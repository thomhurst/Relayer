using System.Buffers;

namespace Relayer;

internal abstract class ReceiveMessageState : MessageHeaders
{
    internal abstract string MessageId { get; }
    internal abstract MessageType MessageType { get; }
    internal abstract int Attempt { get; }
    internal abstract ReadOnlySequence<byte> Body { get; }
}
