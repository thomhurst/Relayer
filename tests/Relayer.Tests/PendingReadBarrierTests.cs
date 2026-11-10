using System.Threading.Channels;
using System.Threading.Tasks.Sources;
using Relayer.Benchmarks;

namespace Relayer.Tests;

public class PendingReadBarrierTests
{
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task Entry_is_not_visible_until_the_release_continuation_is_registered(bool unsafeContinuation)
    {
        var entered = Channel.CreateBounded<byte>(1);
        var source = new ObservedRead(entered.Reader);
        var awaiter = new PendingReadBarrier(new ValueTask<byte>(source, 0), entered.Writer).GetAwaiter();
        var resumed = false;

        if (unsafeContinuation) awaiter.UnsafeOnCompleted(() => resumed = true);
        else awaiter.OnCompleted(() => resumed = true);

        await Assert.That(source.EntryVisibleDuringRegistration).IsFalse();
        await Assert.That(entered.Reader.TryRead(out _)).IsTrue();
        await Assert.That(resumed).IsFalse();
        source.Complete(42);
        await Assert.That(resumed).IsTrue();
        await Assert.That(awaiter.GetResult()).IsEqualTo((byte)42);
    }

    [Test]
    public async Task An_already_completed_read_does_not_enter_the_barrier()
    {
        var entered = Channel.CreateBounded<byte>(1);
        await Assert.That(() => new PendingReadBarrier(new ValueTask<byte>(0), entered.Writer))
            .Throws<InvalidOperationException>();
        await Assert.That(entered.Reader.TryRead(out _)).IsFalse();
    }

    private sealed class ObservedRead(ChannelReader<byte> entered) : IValueTaskSource<byte>
    {
        private ManualResetValueTaskSourceCore<byte> _source;
        public bool EntryVisibleDuringRegistration { get; private set; }
        public void Complete(byte value) => _source.SetResult(value);
        public byte GetResult(short token) => _source.GetResult(token);
        public ValueTaskSourceStatus GetStatus(short token) => _source.GetStatus(token);
        public void OnCompleted(Action<object?> continuation, object? state, short token,
            ValueTaskSourceOnCompletedFlags flags)
        {
            EntryVisibleDuringRegistration = entered.TryPeek(out _);
            _source.OnCompleted(continuation, state, token, flags);
        }
    }
}
