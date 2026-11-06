using System.Runtime.CompilerServices;
using System.Threading.Channels;

namespace Relayer.Benchmarks;

/// <summary>Publishes barrier entry when a pending read's continuation is registered.</summary>
internal readonly struct PendingReadBarrier : ICriticalNotifyCompletion
{
    private readonly ValueTaskAwaiter<byte> _read;
    private readonly ChannelWriter<byte> _entered;

    public PendingReadBarrier(ValueTask<byte> read, ChannelWriter<byte> entered)
    {
        _read = read.GetAwaiter();
        if (_read.IsCompleted) throw new InvalidOperationException("Expected an asynchronous delivery.");
        _entered = entered;
    }

    public PendingReadBarrier GetAwaiter() => this;
    // The driver releases reads only after entry; taking a synchronous path would skip entry.
    public bool IsCompleted => false;
    public byte GetResult() => _read.GetResult();

    public void OnCompleted(Action continuation)
    {
        _read.OnCompleted(continuation);
        SignalEntered();
    }

    public void UnsafeOnCompleted(Action continuation)
    {
        _read.UnsafeOnCompleted(continuation);
        SignalEntered();
    }

    private void SignalEntered()
    {
        if (!_entered.TryWrite(0)) throw new InvalidOperationException("Entry channel is full.");
    }
}
