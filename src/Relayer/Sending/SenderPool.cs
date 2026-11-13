namespace Relayer;

// Failure-enabled consumers attach one scoped sender to each attempt and return it through EndAttempt.
// Pooled senders are not disposable, so the DI scope records nothing for them. A sender resolved from an
// ordinary application scope is simply not returned; the pool refills from later attempts.
internal sealed class SenderPool(SendPipeline pipeline)
{
    private readonly SendPipeline _pipeline = pipeline;
    private Segment? _senders;

    internal void Enable()
    {
        if (Volatile.Read(ref _senders) is null)
            Interlocked.CompareExchange(ref _senders, new Segment(this), null);
    }

    internal Sender Rent(IOutboxEnlistment? outbox, TenantContext? tenant = null)
    {
        var first = Volatile.Read(ref _senders);
        if (first is null) return new Sender(_pipeline, outbox) { Tenant = tenant };
        for (var i = 0; i < first.Items.Length; i++)
        {
            if (Interlocked.Exchange(ref first.Items[i], null) is not { } sender) continue;
            sender.Outbox = outbox;
            sender.Tenant = tenant;
            return sender;
        }
        return new PooledSender(this, _pipeline) { Outbox = outbox, Tenant = tenant };
    }

    private void Return(PooledSender sender)
    {
        var segment = _senders!;
        for (var i = 0; i < segment.Items.Length; i++)
            if (Interlocked.CompareExchange(ref segment.Items[i], sender, null) is null) return;
        // Overflow senders become collectible after the attempt instead of retaining peak concurrency.
    }

    private sealed class Segment
    {
        internal readonly PooledSender?[] Items = new PooledSender[64];
        internal Segment(SenderPool owner)
        {
            for (var i = 0; i < Items.Length; i++) Items[i] = new(owner, owner._pipeline);
        }
    }

    private sealed class PooledSender(SenderPool owner, SendPipeline pipeline) : Sender(pipeline)
    {
        // The consume pipeline ends an attempt after the entire scope finishes disposing.
        internal override void EndAttempt()
        {
            base.EndAttempt();
            Outbox = null;
            Tenant = null;
            owner.Return(this);
        }
    }
}
