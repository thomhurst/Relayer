using Relayer.Transports;
using System.Buffers;

namespace Relayer.Pipeline;

internal abstract class ReceiveMessageState : MessageHeaders
{
    internal abstract HeaderNames EnvelopeNames { get; }
    internal abstract void SetDisposition(Disposition disposition);
    internal abstract Disposition Disposition { get; }
    internal abstract string MessageId { get; }
    internal abstract MessageType MessageType { get; }
    internal abstract ConsumeDelegate? ResolvedDispatch { get; }
    internal abstract int Attempt { get; }
    internal abstract ReadOnlySequence<byte> Body { get; }
    internal abstract bool IsTombstone { get; }
    internal abstract WireString? TombstoneKey { get; }
    internal abstract string? TracePartitionId { get; }
    internal virtual MessageOperationTracing? ProcessTracing => null;
    internal abstract void EnrichActivity(System.Diagnostics.Activity activity);
    internal abstract bool TryConsumeLoggedSettlementError(Exception error);
}
