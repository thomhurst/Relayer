using Relayer;

internal static class PartitionCompletionSmoke
{
    internal static void Run()
    {
        var window = new PartitionCompletionWindow<Checkpoint>(2);
        if (!window.TryTrack(new(15, 3), out var first) || !window.TryTrack(new(91, 4), out var second) ||
            window.TryTrack(new(100, 4), out _) ||
            !window.TryComplete(second, out _, out var count) || count != 0 ||
            !window.TryComplete(first, out var position, out count) || count != 2 || position != new Checkpoint(91, 4))
            throw new InvalidOperationException("Partition completion watermarks failed under Native AOT.");

        window.TryTrack(new(100, 4), out var revoked);
        window.Reset();
        window.TryTrack(new(100, 5), out var replayed);
        if (window.TryComplete(revoked, out _, out _) ||
            !window.TryComplete(replayed, out position, out count) || position != new Checkpoint(100, 5) || count != 1)
            throw new InvalidOperationException("Partition completion ownership fencing failed under Native AOT.");
    }

    private readonly record struct Checkpoint(long NextOffset, int LeaderEpoch);
}
