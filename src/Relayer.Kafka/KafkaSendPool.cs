using Dekaf.Serialization;

namespace Relayer.Kafka;

// Retain at most 256 idle operations; overflow becomes collectible after native completion.
internal sealed class KafkaSendPool
{
    private readonly SlotPool<KafkaSendOperation> _operations = new(capacity: 256);

    internal KafkaSendOperation Rent(int count)
    {
        var operation = _operations.TryRent() ?? new KafkaSendOperation();
        operation.Ensure(count);
        return operation;
    }

    internal void Return(KafkaSendOperation operation)
    {
        operation.Clear();
        _operations.TryReturn(operation);
    }
}

internal sealed class KafkaSendOperation
{
    internal KafkaSendState[] States = [];
    internal ValueTask<Dekaf.Producer.RecordMetadata>[] Pending = [];
    internal readonly List<(WireString? Key, (ReadOnlyMemory<byte> Body, Headers Headers)? Value)> Messages = [];
    private int _count;

    internal void Ensure(int count)
    {
        if (States.Length < count)
        {
            var previous = States.Length;
            Array.Resize(ref States, count);
            for (var i = previous; i < count; i++) States[i] = new();
        }
        Messages.EnsureCapacity(count);
        _count = count;
    }

    private int _pendingCount;
    internal void EnsurePending(int count)
    {
        if (Pending.Length < count) Array.Resize(ref Pending, count);
        _pendingCount = count;
    }

    internal void Clear()
    {
        for (var i = 0; i < _count; i++) States[i].Clear();
        if (_pendingCount != 0) Array.Clear(Pending, 0, _pendingCount);
        _pendingCount = 0;
        Messages.Clear();
        _count = 0;
    }
}
