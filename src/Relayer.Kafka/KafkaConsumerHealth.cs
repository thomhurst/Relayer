using Relayer.Transports;
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
                // Zero unavailable, negative idle, positive oldest outstanding record.
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

// Reads Dekaf's PartitionedProcessingMonitor: the oldest buffered or in-flight record, which does not
// advance while one partition is stuck and others progress.
internal sealed class KafkaWorkerHealth(TimeProvider clock)
{
    private Func<DateTimeOffset?>? _oldestOutstanding;
    internal void Start(Func<DateTimeOffset?> oldestOutstanding) => Volatile.Write(ref _oldestOutstanding, oldestOutstanding);
    internal void Unavailable() => Volatile.Write(ref _oldestOutstanding, null);
    internal long Read()
    {
        if (Volatile.Read(ref _oldestOutstanding) is not { } oldest) return 0;
        return oldest() is { } at ? Math.Max(1, at.UtcTicks) : -clock.GetUtcNow().UtcTicks;
    }
}
