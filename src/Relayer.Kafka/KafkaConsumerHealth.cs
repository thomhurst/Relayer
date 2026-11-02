namespace Relayer.Kafka;

// Optional receiver-owned state. The health integration is the only production registrant.
internal sealed class KafkaConsumerHealth(TimeProvider clock)
{
    private readonly Dictionary<IConsumerDefinition, List<KafkaWorkerHealth>> _workers = new(ReferenceEqualityComparer.Instance);

    internal KafkaWorkerHealth Register(IConsumerDefinition consumer)
    {
        var worker = new KafkaWorkerHealth(clock);
        lock (_workers)
        {
            if (!_workers.TryGetValue(consumer, out var workers)) _workers.Add(consumer, workers = []);
            workers.Add(worker);
        }
        return worker;
    }

    internal (bool IsIdle, DateTimeOffset LastProgressAt)? GetState(IConsumerDefinition consumer)
    {
        lock (_workers)
        {
            if (!_workers.TryGetValue(consumer, out var workers)) return null;
            var oldest = long.MaxValue;
            var idleAt = 0L;
            foreach (var worker in workers)
            {
                // One atomic value: zero unavailable, negative idle, positive active progress.
                var progress = worker.Read();
                if (progress == 0) return null;
                if (progress > 0) oldest = Math.Min(oldest, progress);
                else idleAt = Math.Max(idleAt, -progress);
            }
            return oldest == long.MaxValue
                ? (true, new DateTimeOffset(idleAt, TimeSpan.Zero))
                : (false, new DateTimeOffset(oldest, TimeSpan.Zero));
        }
    }
}

internal sealed class KafkaWorkerHealth(TimeProvider clock)
{
    private long _progress;
    internal long Read() => Volatile.Read(ref _progress);
    internal void Progress() => Volatile.Write(ref _progress, clock.GetUtcNow().UtcTicks);
    internal void Idle() => Volatile.Write(ref _progress, -clock.GetUtcNow().UtcTicks);
    internal void Unavailable() => Volatile.Write(ref _progress, 0);
}
