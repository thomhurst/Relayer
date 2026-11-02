using System.Buffers;
using Relayer;

internal static class PartitionSchedulerSmoke
{
    internal static void Run()
    {
        var source = new Source();
        var scheduler = new PartitionScheduler<Delivery, Binding, int, Checkpoint>(source,
            static (message, _) => new(message.Completion.Task), new() { ConcurrencyPerPartition = 2 });
        var assignment = scheduler.Assign(1);
        var first = new Delivery(1);
        var second = new Delivery(2);
        scheduler.TryEnqueue(assignment, first, new(15, 3));
        scheduler.TryEnqueue(assignment, second, new(91, 4));
        scheduler.Pump();
        second.Complete();
        scheduler.Pump();
        if (source.Commits != 0) throw new InvalidOperationException("Scheduler acknowledged beyond a gap under Native AOT.");
        first.Complete();
        scheduler.Pump();
        if (source.Checkpoint != new Checkpoint(91, 4) || first.Releases != 1 || second.Releases != 1)
            throw new InvalidOperationException("Scheduler durable watermark failed under Native AOT.");

        var late = new Delivery(1);
        scheduler.TryEnqueue(assignment, late, new(100, 4));
        scheduler.Pump();
        scheduler.Revoke(assignment, TimeSpan.Zero);
        scheduler.Pump();
        var fresh = scheduler.Assign(1);
        var replay = new Delivery(1);
        replay.Complete();
        scheduler.TryEnqueue(fresh, replay, new(100, 5));
        scheduler.Pump();
        late.Complete();
        scheduler.Pump();
        if (source.Checkpoint != new Checkpoint(100, 5) || late.Releases != 1 || scheduler.BufferedCount != 0)
            throw new InvalidOperationException("Scheduler ownership fencing failed under Native AOT.");
        scheduler.Stop(TimeSpan.Zero);
        scheduler.Pump();
        Console.WriteLine("Partition scheduling, durable watermarks and ownership fencing passed under Native AOT.");
    }

    private readonly record struct Checkpoint(long NextOffset, int Epoch);
    private sealed class Source : IPartitionSource<int, Checkpoint>
    {
        internal Checkpoint Checkpoint;
        internal int Commits;
        public void Commit(int partition, Checkpoint position) { Checkpoint = position; Commits++; }
        public void Pause(int partition) { }
        public void Resume(int partition) { }
    }
    private sealed class Delivery(int key)
    {
        internal int Key { get; } = key;
        internal TaskCompletionSource<Disposition> Completion { get; } = new();
        internal int Releases;
        internal void Complete() => Completion.SetResult(Disposition.Complete);
    }
    private readonly struct Binding : IPartitionMessageBinding<Delivery>
    {
        public static bool KeysEqual(Delivery first, Delivery second) => first.Key == second.Key;
        public static void Release(Delivery message) => message.Releases++;
        public static ReadOnlySequence<byte> GetBody(Delivery message) => default;
        public static int? GetDeliveryCount(Delivery message) => null;
        public static bool TryGetHeader(Delivery message, string name, out WireString value) { value = default; return false; }
    }
}
