using Relayer.Transports;
using System.Runtime.CompilerServices;

namespace Relayer.Tests;

public class PartitionCompletionWindowTests
{
    [Test]
    [Arguments(0)]
    [Arguments(-1)]
    public async Task Capacity_must_be_positive(int capacity)
        => await Assert.That(() => new PartitionCompletionWindow<long>(capacity)).Throws<ArgumentOutOfRangeException>();

    [Test]
    public async Task Every_completion_permutation_releases_only_the_contiguous_arrival_prefix()
    {
        // Deliberately unordered positions: only arrival order defines the prefix.
        int[] positions = [8, 2, 100, 4, 9];
        foreach (var order in Permutations([0, 1, 2, 3, 4], 0))
        {
            var window = new PartitionCompletionWindow<int>(positions.Length);
            var tokens = new PartitionCompletionToken[positions.Length];
            var completed = new bool[positions.Length];
            for (var i = 0; i < positions.Length; i++)
                await Assert.That(window.TryTrack(positions[i], out tokens[i])).IsTrue();
            var prefix = 0;
            foreach (var index in order)
            {
                completed[index] = true;
                var previous = prefix;
                while (prefix < positions.Length && completed[prefix]) prefix++;

                await Assert.That(window.TryComplete(tokens[index], out var position, out var count)).IsTrue();
                await Assert.That(count).IsEqualTo(prefix - previous);
                await Assert.That(position).IsEqualTo(count == 0 ? default : positions[prefix - 1]);
                await Assert.That(window.Count).IsEqualTo(positions.Length - prefix);
                await Assert.That(window.TryComplete(tokens[index], out _, out _)).IsFalse();
            }
        }
    }

    [Test]
    public async Task Unfinished_gap_keeps_completed_deliveries_and_applies_capacity_backpressure()
    {
        var window = new PartitionCompletionWindow<string>(3);
        window.TryTrack("first", out var first);
        window.TryTrack("second", out var second);
        window.TryTrack("third", out var third);
        await Assert.That(window.Capacity).IsEqualTo(3);
        await Assert.That(window.TryComplete(third, out var checkpoint, out var count)).IsTrue();
        await Assert.That(checkpoint).IsNull();
        await Assert.That(count).IsEqualTo(0);
        window.TryComplete(second, out _, out _);
        await Assert.That(window.Count).IsEqualTo(3);
        await Assert.That(window.IsFull).IsTrue();
        await Assert.That(window.TryTrack("must not skip", out var refused)).IsFalse();
        await Assert.That(window.TryComplete(refused, out _, out _)).IsFalse();

        await Assert.That(window.TryComplete(first, out checkpoint, out count)).IsTrue();
        await Assert.That(checkpoint).IsEqualTo("third");
        await Assert.That(count).IsEqualTo(3);
        await Assert.That(window.Count).IsEqualTo(0);
        await Assert.That(window.IsFull).IsFalse();
        await Assert.That(window.TryTrack("next", out _)).IsTrue();
    }

    [Test]
    public async Task Foreign_and_default_tokens_cannot_acknowledge_a_matching_slot_and_sequence()
    {
        var first = new PartitionCompletionWindow<long>(1);
        var second = new PartitionCompletionWindow<long>(1);
        first.TryTrack(10, out var firstToken);
        second.TryTrack(20, out var secondToken);

        await Assert.That(second.TryComplete(default, out var checkpoint, out var count)).IsFalse();
        await Assert.That(checkpoint).IsEqualTo(0L);
        await Assert.That(count).IsEqualTo(0);
        await Assert.That(second.TryComplete(firstToken, out _, out _)).IsFalse();
        await Assert.That(second.Count).IsEqualTo(1);
        await Assert.That(first.Count).IsEqualTo(1);
        await Assert.That(second.TryComplete(secondToken, out checkpoint, out count)).IsTrue();
        await Assert.That(checkpoint).IsEqualTo(20L);
        await Assert.That(count).IsEqualTo(1);
    }

    [Test]
    [Arguments(1)]
    [Arguments(2)]
    [Arguments(7)]
    public async Task Reused_ring_slots_reject_old_tokens_and_preserve_partial_prefixes(int capacity)
    {
        var window = new PartitionCompletionWindow<long>(capacity);
        var tokens = new PartitionCompletionToken[capacity];
        PartitionCompletionToken stale = default;
        for (var round = 0; round < 100; round++)
        {
            // Advance head by one before filling, so many fills wrap the end of the array.
            window.TryTrack(-1, out var lead);
            await Assert.That(window.TryComplete(stale, out _, out _)).IsFalse();
            window.TryComplete(lead, out _, out _);
            stale = lead;
            for (var i = 0; i < capacity; i++)
                await Assert.That(window.TryTrack(round * 100L + i, out tokens[i])).IsTrue();
            for (var i = capacity - 1; i > 0; i--)
            {
                await Assert.That(window.TryComplete(tokens[i], out _, out var released)).IsTrue();
                await Assert.That(released).IsEqualTo(0);
            }
            await Assert.That(window.TryComplete(stale, out _, out _)).IsFalse();
            await Assert.That(window.TryComplete(tokens[0], out var checkpoint, out var count)).IsTrue();
            await Assert.That(checkpoint).IsEqualTo(round * 100L + capacity - 1);
            await Assert.That(count).IsEqualTo(capacity);
            await Assert.That(window.Count).IsEqualTo(0);
        }
    }

    [Test]
    public async Task Partial_advancement_reuses_free_slots_without_discarding_an_older_gap()
    {
        var window = new PartitionCompletionWindow<int>(3);
        window.TryTrack(10, out var first);
        window.TryTrack(20, out var gap);
        window.TryTrack(30, out var third);
        window.TryComplete(third, out _, out _);
        window.TryComplete(first, out var position, out var count);
        await Assert.That(position).IsEqualTo(10);
        await Assert.That(count).IsEqualTo(1);
        window.TryTrack(40, out var wrapped);
        window.TryComplete(wrapped, out _, out _);
        await Assert.That(window.IsFull).IsTrue();
        window.TryComplete(gap, out position, out count);
        await Assert.That(position).IsEqualTo(40);
        await Assert.That(count).IsEqualTo(3);
    }

    [Test]
    public async Task Reset_discards_failed_gap_and_fences_old_workers_after_reassignment()
    {
        var window = new PartitionCompletionWindow<string>(2);
        window.TryTrack("unfinished", out var unfinished);
        window.TryTrack("completed but blocked", out var blocked);
        window.TryComplete(blocked, out _, out _);
        window.Reset();
        window.Reset();
        await Assert.That(window.Count).IsEqualTo(0);
        await Assert.That(window.IsFull).IsFalse();

        window.TryTrack("replayed", out var replayed);
        window.TryTrack("new assignment", out var current);
        await Assert.That(window.TryComplete(unfinished, out var checkpoint, out var count)).IsFalse();
        await Assert.That(checkpoint).IsNull();
        await Assert.That(count).IsEqualTo(0);
        await Assert.That(window.TryComplete(blocked, out _, out _)).IsFalse();
        window.TryComplete(current, out _, out _);
        await Assert.That(window.Count).IsEqualTo(2);
        window.TryComplete(replayed, out checkpoint, out count);
        await Assert.That(checkpoint).IsEqualTo("new assignment");
        await Assert.That(count).IsEqualTo(2);
    }

    [Test]
    public async Task Native_checkpoint_preserves_offset_gaps_and_leader_epoch_without_comparison()
    {
        var window = new PartitionCompletionWindow<Checkpoint>(2);
        window.TryTrack(new(15, 3), out var first);
        window.TryTrack(new(91, 4), out var second);
        window.TryComplete(second, out _, out _);
        window.TryComplete(first, out var checkpoint, out var count);
        await Assert.That(checkpoint).IsEqualTo(new Checkpoint(91, 4));
        await Assert.That(count).IsEqualTo(2);
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task Released_or_reset_positions_are_not_retained(bool reset)
    {
        var window = new PartitionCompletionWindow<object>(2);
        var position = TrackAndRelease(window, reset);
        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();
        await Assert.That(position.IsAlive).IsFalse();
        GC.KeepAlive(window);
    }

    [Test]
    public async Task Keeping_a_token_does_not_retain_the_window_or_its_positions()
    {
        var (token, window, position) = TrackWithoutRetainingWindow();
        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();
        await Assert.That(window.IsAlive).IsFalse();
        await Assert.That(position.IsAlive).IsFalse();
        GC.KeepAlive(token);
    }

    [Test]
    public async Task Track_and_complete_allocate_no_managed_memory_after_construction()
    {
        var window = new PartitionCompletionWindow<Checkpoint>(2);
        Exercise(window, 10_000);
        var before = GC.GetAllocatedBytesForCurrentThread();
        var checksum = Exercise(window, 10_000);
        var allocated = GC.GetAllocatedBytesForCurrentThread() - before;
        await Assert.That(checksum).IsEqualTo(20_000);
        await Assert.That(allocated).IsEqualTo(0L);
    }

    private static int Exercise(PartitionCompletionWindow<Checkpoint> window, int iterations)
    {
        var checksum = 0;
        for (var i = 0; i < iterations; i++)
        {
            window.TryTrack(new(i, 1), out var first);
            window.TryTrack(new(i + 10, 2), out var second);
            window.TryComplete(second, out _, out _);
            window.TryComplete(first, out var checkpoint, out _);
            checksum += checkpoint.LeaderEpoch;
        }
        return checksum;
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static WeakReference TrackAndRelease(PartitionCompletionWindow<object> window, bool reset)
    {
        var position = new object();
        var reference = new WeakReference(position);
        window.TryTrack(position, out var first);
        window.TryTrack(position, out var second);
        window.TryComplete(second, out _, out _);
        if (reset) window.Reset();
        else window.TryComplete(first, out _, out _);
        return reference;
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static (PartitionCompletionToken, WeakReference, WeakReference) TrackWithoutRetainingWindow()
    {
        var window = new PartitionCompletionWindow<object>(1);
        var position = new object();
        window.TryTrack(position, out var token);
        return (token, new WeakReference(window), new WeakReference(position));
    }

    private static IEnumerable<int[]> Permutations(int[] values, int start)
    {
        if (start == values.Length) { yield return values; yield break; }
        for (var i = start; i < values.Length; i++)
        {
            (values[start], values[i]) = (values[i], values[start]);
            foreach (var permutation in Permutations(values, start + 1)) yield return permutation;
            (values[start], values[i]) = (values[i], values[start]);
        }
    }

    private readonly record struct Checkpoint(long NextOffset, int LeaderEpoch);
}
