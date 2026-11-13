using Relayer.Transports;
namespace Relayer;

// Failure-enabled consumers attach one scoped sender to each attempt and return it through EndAttempt.
// Pooled senders are not disposable, so the DI scope records nothing for them. A sender resolved from an
// ordinary application scope is simply not returned; the pool refills from later attempts.
internal sealed class SenderPool(SendPipeline pipeline)
{
    private readonly SendPipeline _pipeline = pipeline;
    private SlotPool<PooledSender>? _senders;

    internal void Enable()
    {
        if (Volatile.Read(ref _senders) is null)
            Interlocked.CompareExchange(ref _senders, CreatePool(), null);
    }

    internal Sender Rent(IOutboxEnlistment? outbox, TenantContext? tenant = null)
    {
        var first = Volatile.Read(ref _senders);
        if (first is null) return new Sender(_pipeline, outbox) { Tenant = tenant };
        var sender = first.TryRentGuarded() ?? new PooledSender(this, _pipeline);
        sender.Outbox = outbox;
        sender.Tenant = tenant;
        return sender;
    }

    private void Return(PooledSender sender) => _senders!.TryReturnGuarded(sender);

    private SlotPool<PooledSender> CreatePool()
    {
        var pool = new SlotPool<PooledSender>(capacity: 64);
        for (var i = 0; i < 64; i++) pool.TryReturn(new(this, _pipeline), i);
        return pool;
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
