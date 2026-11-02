namespace Relayer;

// Failure-enabled consumers resolve one scoped sender before dispatch. Reuse that sender across scopes
// so the failure pipeline adds no steady-state allocation beyond the DI scope's own bookkeeping.
internal sealed class SenderPool(SendPipeline pipeline)
{
    private readonly SendPipeline _pipeline = pipeline;
    private Segment? _senders;

    internal void Enable()
    {
        if (Volatile.Read(ref _senders) is null)
            Interlocked.CompareExchange(ref _senders, new Segment(this, initialize: true), null);
    }

    internal Sender Rent(IOutboxEnlistment? outbox, TenantContext? tenant = null)
    {
        var first = Volatile.Read(ref _senders);
        if (first is null) return new Sender(_pipeline, outbox) { Tenant = tenant };
        for (var segment = first; segment is not null; segment = Volatile.Read(ref segment.Next))
        {
            for (var i = 0; i < segment.Items.Length; i++)
            {
                if (Interlocked.Exchange(ref segment.Items[i], null) is not { } sender) continue;
                sender.Outbox = outbox;
                sender.Tenant = tenant;
                return sender;
            }
        }
        return new PooledSender(this, _pipeline) { Outbox = outbox, Tenant = tenant };
    }

    private void Return(PooledSender sender)
    {
        var segment = _senders!;
        while (true)
        {
            for (var i = 0; i < segment.Items.Length; i++)
                if (Interlocked.CompareExchange(ref segment.Items[i], sender, null) is null) return;
            var next = Volatile.Read(ref segment.Next);
            if (next is null)
                next = Interlocked.CompareExchange(ref segment.Next, new Segment(this), null) ?? segment.Next;
            segment = next!;
        }
    }

    private sealed class Segment
    {
        internal readonly PooledSender?[] Items = new PooledSender[64];
        internal Segment? Next;
        internal Segment(SenderPool owner, bool initialize = false)
        {
            if (initialize)
                for (var i = 0; i < Items.Length; i++) Items[i] = new(owner, owner._pipeline);
        }
    }

    private sealed class PooledSender(SenderPool owner, SendPipeline pipeline) : Sender(pipeline), IDisposable
    {
        internal override void EndAttempt()
        {
            base.EndAttempt();
            Dispose();
        }

        public void Dispose()
        {
            // The consume pipeline returns an attached sender after the entire scope finishes disposing.
            if (AttemptBuffer is not null) return;
            Outbox = null;
            Tenant = null;
            owner.Return(this);
        }
    }
}
