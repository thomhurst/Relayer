using System.Buffers;
using System.Threading.Tasks.Sources;

namespace Relayer.Tests;

public class PartitionSchedulerTests
{
    [Test]
    public async Task Equal_native_keys_stay_ordered_while_unrelated_keys_make_progress()
    {
        var fixture = new Fixture(capacity: 4, concurrency: 2);
        var assignment = fixture.Scheduler.Assign(1);
        var first = new Delivery(7);
        var second = new Delivery(7);
        var unrelated = new Delivery(9);
        fixture.Enqueue(assignment, first, "first");
        fixture.Enqueue(assignment, second, "second");
        fixture.Enqueue(assignment, unrelated, "third");
        fixture.Scheduler.Pump();
        await Assert.That(first.Started).IsTrue();
        await Assert.That(second.Started).IsFalse();
        await Assert.That(unrelated.Started).IsTrue();
        await Assert.That(fixture.Scheduler.ActiveCount).IsEqualTo(2);

        unrelated.Complete();
        fixture.Scheduler.Pump();
        await Assert.That(fixture.Source.Commits.Count).IsEqualTo(0);
        await Assert.That(second.Started).IsFalse();
        // A checkpoint may borrow the delivery: retain even completed out-of-order storage.
        await Assert.That(unrelated.Releases).IsEqualTo(0);
        first.Complete();
        fixture.Scheduler.Pump();
        await Assert.That(second.Started).IsTrue();
        await Assert.That(fixture.Source.Commits.Last().Position).IsEqualTo("first");
        second.Complete();
        fixture.Scheduler.Pump();
        await Assert.That(fixture.Source.Commits.Last().Position).IsEqualTo("third");
        await Assert.That(fixture.Scheduler.BufferedCount).IsEqualTo(0);
        await Assert.That(first.Releases + second.Releases + unrelated.Releases).IsEqualTo(3);
    }

    [Test]
    public async Task Queued_equal_key_cannot_be_overtaken_when_another_key_uses_capacity()
    {
        var fixture = new Fixture(capacity: 5, concurrency: 2);
        var assignment = fixture.Scheduler.Assign(1);
        Delivery[] deliveries = [new(1), new(2), new(2), new(3), new(2)];
        foreach (var delivery in deliveries) fixture.Enqueue(assignment, delivery, "opaque");
        fixture.Scheduler.Pump();
        deliveries[0].Complete();
        fixture.Scheduler.Pump();
        await Assert.That(deliveries[3].Started).IsTrue();
        await Assert.That(deliveries[2].Started).IsFalse();
        await Assert.That(deliveries[4].Started).IsFalse();
        deliveries[1].Complete();
        fixture.Scheduler.Pump();
        await Assert.That(deliveries[2].Started).IsTrue();
        await Assert.That(deliveries[4].Started).IsFalse();
        foreach (var delivery in deliveries) delivery.Complete();
        fixture.Scheduler.Pump();
        fixture.Scheduler.Pump();
        await Assert.That(fixture.Scheduler.BufferedCount).IsEqualTo(0);
    }

    [Test]
    public async Task Full_window_pauses_until_oldest_gap_closes_then_resumes_once()
    {
        var fixture = new Fixture(capacity: 2, concurrency: 2);
        var assignment = fixture.Scheduler.Assign(1);
        var first = new Delivery(1);
        var later = new Delivery(2);
        fixture.Enqueue(assignment, first, "z");
        fixture.Enqueue(assignment, later, "a");
        var refused = new Delivery(3);
        await Assert.That(fixture.Scheduler.TryEnqueue(assignment, refused, "refused")).IsFalse();
        fixture.Scheduler.Pump();
        later.Complete();
        fixture.Scheduler.Pump();
        fixture.Scheduler.Pump();
        await Assert.That(fixture.Source.Pauses).IsEquivalentTo(new[] { 1 });
        await Assert.That(fixture.Source.Resumes.Count).IsEqualTo(1);
        first.Complete();
        fixture.Scheduler.Pump();
        fixture.Scheduler.Pump();
        await Assert.That(fixture.Source.Resumes).IsEquivalentTo(new[] { 1, 1 });
        await Assert.That(fixture.Source.Commits.Last().Position).IsEqualTo("a");
        await Assert.That(refused.Releases).IsEqualTo(0);
    }

    [Test]
    public async Task Global_limits_include_old_ownership_and_pause_other_assignments()
    {
        var fixture = new Fixture(capacity: 2, concurrency: 1, totalCapacity: 2, totalConcurrency: 1);
        var first = fixture.Scheduler.Assign(1);
        var second = fixture.Scheduler.Assign(2);
        var old = new Delivery(1);
        fixture.Enqueue(first, old, "old");
        fixture.Scheduler.Pump();
        fixture.Scheduler.Lose(first);
        var current = new Delivery(2);
        fixture.Enqueue(second, current, "current");
        fixture.Scheduler.Pump();
        await Assert.That(current.Started).IsFalse();
        await Assert.That(fixture.Source.Pauses).IsEquivalentTo(new[] { 2 });
        await Assert.That(fixture.Scheduler.TryEnqueue(second, new(3), "overflow")).IsFalse();
        old.Complete();
        fixture.Scheduler.Pump();
        await Assert.That(current.Started).IsTrue();
        await Assert.That(fixture.Source.Resumes).IsEquivalentTo(new[] { 1, 2, 2 });
        await Assert.That(fixture.Source.Commits.Count).IsEqualTo(0);
        current.Complete();
        fixture.Scheduler.Pump();
    }

    [Test]
    public async Task Ready_assignments_share_global_concurrency_without_starvation()
    {
        var fixture = new Fixture(capacity: 4, concurrency: 2, totalConcurrency: 2);
        var first = fixture.Scheduler.Assign(1);
        var second = fixture.Scheduler.Assign(2);
        var busy = new Delivery(1);
        var busyNext = new Delivery(2);
        var peer = new Delivery(3);
        fixture.Enqueue(first, busy, "busy");
        fixture.Enqueue(first, busyNext, "busy-next");
        fixture.Enqueue(second, peer, "peer");
        fixture.Scheduler.Pump();
        await Assert.That(busy.Started).IsTrue();
        await Assert.That(peer.Started).IsTrue();
        await Assert.That(busyNext.Started).IsFalse();
        busy.Complete();
        fixture.Scheduler.Pump();
        await Assert.That(busyNext.Started).IsTrue();
        busyNext.Complete();
        peer.Complete();
        fixture.Scheduler.Pump();
        await Assert.That(fixture.Scheduler.BufferedCount).IsEqualTo(0);
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task Failure_or_cancellation_stops_dispatch_and_never_acknowledges_past_gap(bool cancel)
    {
        var fixture = new Fixture(capacity: 4, concurrency: 2);
        var assignment = fixture.Scheduler.Assign(1);
        var first = new Delivery(1);
        var gap = new Delivery(2);
        var queued = new Delivery(3);
        fixture.Enqueue(assignment, first, "first");
        fixture.Enqueue(assignment, gap, "gap");
        fixture.Enqueue(assignment, queued, "queued");
        fixture.Scheduler.Pump();
        if (cancel) gap.Completion.SetCanceled();
        else gap.Completion.SetException(new FormatException("durable write failed"));
        first.Complete();
        fixture.Scheduler.Pump();
        await Assert.That(fixture.Scheduler.GetFailure(assignment)).IsNotNull();
        await Assert.That(queued.Started).IsFalse();
        await Assert.That(fixture.Source.Commits.Select(c => c.Position)).IsEquivalentTo(new[] { "first" });
        await Assert.That(fixture.Source.Pauses).IsEquivalentTo(new[] { 1 });
        await Assert.That(fixture.Scheduler.BufferedCount).IsEqualTo(2);
        fixture.Scheduler.Lose(assignment);
        fixture.Scheduler.Pump();
        await Assert.That(gap.Releases + queued.Releases).IsEqualTo(2);
    }

    [Test]
    [Arguments(DispositionKind.Redeliver)]
    [Arguments(DispositionKind.DeadLetter)]
    public async Task Unsupported_native_settlement_does_not_become_a_durable_completion(DispositionKind kind)
    {
        var fixture = new Fixture();
        var assignment = fixture.Scheduler.Assign(1);
        var delivery = new Delivery(1);
        delivery.Completion.SetResult(kind == DispositionKind.Redeliver
            ? Disposition.Redeliver(TimeSpan.Zero) : Disposition.DeadLettered(new("bad")));
        fixture.Enqueue(assignment, delivery, "gap");
        fixture.Scheduler.Pump();
        await Assert.That(fixture.Scheduler.GetFailure(assignment)).IsTypeOf<InvalidOperationException>();
        await Assert.That(fixture.Source.Commits.Count).IsEqualTo(0);
        fixture.Scheduler.Lose(assignment);
        fixture.Scheduler.Pump();
    }

    [Test]
    public async Task Revoke_drains_active_prefix_but_never_starts_queued_work_or_resumes_old_pause()
    {
        var fixture = new Fixture(capacity: 2, concurrency: 1);
        var assignment = fixture.Scheduler.Assign(1);
        var active = new Delivery(1);
        var queued = new Delivery(1);
        fixture.Enqueue(assignment, active, "safe");
        fixture.Enqueue(assignment, queued, "queued");
        fixture.Scheduler.Pump();
        fixture.Scheduler.Revoke(assignment, TimeSpan.FromSeconds(5));
        active.Complete();
        fixture.Scheduler.Pump();
        await Assert.That(fixture.Scheduler.IsDraining).IsFalse();
        await Assert.That(queued.Started).IsFalse();
        await Assert.That(queued.Releases).IsEqualTo(1);
        await Assert.That(fixture.Source.Commits.Last().Position).IsEqualTo("safe");
        await Assert.That(fixture.Source.Resumes.Count).IsEqualTo(1);
        var reassigned = fixture.Scheduler.Assign(1);
        await Assert.That(fixture.Source.Resumes).IsEquivalentTo(new[] { 1, 1 });
        var replay = new Delivery(1);
        replay.Complete();
        fixture.Enqueue(reassigned, replay, "replayed");
        fixture.Scheduler.Pump();
        await Assert.That(fixture.Source.Commits.Last().Position).IsEqualTo("replayed");
        await Assert.That(fixture.Scheduler.TryEnqueue(assignment, new(1), "stale")).IsFalse();
    }

    [Test]
    public async Task Deadline_fences_late_completion_keeps_borrowed_memory_and_preserves_retained_partition()
    {
        var clock = new InMemoryTestClock();
        var fixture = new Fixture(capacity: 2, concurrency: 1, clock: clock);
        var revoked = fixture.Scheduler.Assign(1);
        var retained = fixture.Scheduler.Assign(2);
        var late = new Delivery(1);
        fixture.Enqueue(revoked, late, "old epoch");
        fixture.Scheduler.Pump();
        fixture.Scheduler.Revoke(revoked, TimeSpan.FromSeconds(5));
        clock.Advance(TimeSpan.FromSeconds(5));
        fixture.Scheduler.Pump();
        await Assert.That(fixture.Scheduler.IsDraining).IsFalse();
        await Assert.That(late.Cancellation.IsCancellationRequested).IsTrue();
        await Assert.That(late.Releases).IsEqualTo(0);
        var reassigned = fixture.Scheduler.Assign(1);
        var replay = new Delivery(1);
        var independent = new Delivery(2);
        replay.Complete();
        independent.Complete();
        fixture.Enqueue(reassigned, replay, "new epoch");
        fixture.Enqueue(retained, independent, "retained");
        fixture.Scheduler.Pump();
        late.Complete();
        fixture.Scheduler.Pump();
        await Assert.That(late.Releases).IsEqualTo(1);
        await Assert.That(fixture.Source.Commits.Select(c => c.Position)).IsEquivalentTo(new[] { "retained", "new epoch" });
        await Assert.That(fixture.Source.Resumes).IsEquivalentTo(new[] { 1, 2, 1 });
    }

    [Test]
    public async Task Cooperative_retained_partition_keeps_dispatching_during_another_partitions_drain()
    {
        var fixture = new Fixture(concurrency: 1);
        var revoked = fixture.Scheduler.Assign(1);
        var retained = fixture.Scheduler.Assign(2);
        var draining = new Delivery(1);
        var continuing = new Delivery(2);
        fixture.Enqueue(revoked, draining, "draining");
        fixture.Enqueue(retained, continuing, "retained-first");
        fixture.Scheduler.Pump();
        fixture.Scheduler.Revoke(revoked, TimeSpan.FromSeconds(5));
        continuing.Complete();
        fixture.Scheduler.Pump();
        var next = new Delivery(2);
        fixture.Enqueue(retained, next, "retained-next");
        fixture.Scheduler.Pump();
        await Assert.That(next.Started).IsTrue();
        await Assert.That(fixture.Scheduler.IsDraining).IsTrue();
        await Assert.That(fixture.Source.Commits.Select(c => c.Position)).IsEquivalentTo(new[] { "retained-first" });
        next.Complete();
        draining.Complete();
        fixture.Scheduler.Pump();
        await Assert.That(fixture.Scheduler.IsDraining).IsFalse();
        await Assert.That(fixture.Scheduler.BufferedCount).IsEqualTo(0);
    }

    [Test]
    public async Task Loss_fences_already_completed_but_unobserved_work_without_commit()
    {
        var fixture = new Fixture();
        var assignment = fixture.Scheduler.Assign(1);
        var delivery = new Delivery(1);
        fixture.Enqueue(assignment, delivery, "must not commit");
        fixture.Scheduler.Pump();
        delivery.Complete();
        fixture.Scheduler.Lose(assignment);
        fixture.Scheduler.Pump();
        await Assert.That(fixture.Source.Commits.Count).IsEqualTo(0);
        await Assert.That(delivery.Releases).IsEqualTo(1);
    }

    [Test]
    public async Task Shutdown_is_bounded_rejects_admission_and_observes_late_faults()
    {
        var fixture = new Fixture();
        var assignment = fixture.Scheduler.Assign(1);
        var delivery = new Delivery(1);
        fixture.Enqueue(assignment, delivery, "unfinished");
        fixture.Scheduler.Pump();
        fixture.Scheduler.Stop(TimeSpan.Zero);
        fixture.Scheduler.Pump();
        await Assert.That(fixture.Scheduler.IsDraining).IsFalse();
        await Assert.That(fixture.Scheduler.TryEnqueue(assignment, new(2), "rejected")).IsFalse();
        await Assert.That(() => fixture.Scheduler.Assign(2)).Throws<InvalidOperationException>();
        await Assert.That(delivery.Releases).IsEqualTo(0);
        delivery.Completion.SetException(new ApplicationException("late fault"));
        fixture.Scheduler.Pump();
        fixture.Scheduler.Pump();
        await Assert.That(delivery.Releases).IsEqualTo(1);
        await Assert.That(fixture.Scheduler.ActiveCount).IsEqualTo(0);
        await Assert.That(fixture.Scheduler.BufferedCount).IsEqualTo(0);
        await Assert.That(fixture.Source.Commits.Count).IsEqualTo(0);
    }

    [Test]
    [Arguments(300, 10, 0, 0)]
    [Arguments(300, 10, 5, 5)]
    [Arguments(5, 3, 300, 2)]
    [Arguments(5, 10, 300, 0)]
    public async Task Shutdown_uses_earliest_deadline_for_already_revoking_assignment(
        int revokeSeconds, int elapsedSeconds, int stopSeconds, int remainingSeconds)
    {
        var clock = new InMemoryTestClock();
        var fixture = new Fixture(clock: clock);
        var assignment = fixture.Scheduler.Assign(1);
        var delivery = new Delivery(1);
        fixture.Enqueue(assignment, delivery, "unfinished");
        fixture.Scheduler.Pump();
        fixture.Scheduler.Revoke(assignment, TimeSpan.FromSeconds(revokeSeconds));
        clock.Advance(TimeSpan.FromSeconds(elapsedSeconds));
        fixture.Scheduler.Stop(TimeSpan.FromSeconds(stopSeconds));
        if (remainingSeconds > 0)
        {
            clock.Advance(TimeSpan.FromSeconds(remainingSeconds) - TimeSpan.FromTicks(1));
            fixture.Scheduler.Pump();
            await Assert.That(fixture.Scheduler.IsDraining).IsTrue();
            await Assert.That(delivery.Cancellation.IsCancellationRequested).IsFalse();
            clock.Advance(TimeSpan.FromTicks(1));
        }
        fixture.Scheduler.Pump();
        await Assert.That(fixture.Scheduler.IsDraining).IsFalse();
        await Assert.That(delivery.Cancellation.IsCancellationRequested).IsTrue();
        await Assert.That(delivery.Releases).IsEqualTo(0);
        delivery.Complete();
        fixture.Scheduler.Pump();
        await Assert.That(delivery.Releases).IsEqualTo(1);
        await Assert.That(fixture.Scheduler.BufferedCount).IsEqualTo(0);
        await Assert.That(fixture.Source.Commits.Count).IsEqualTo(0);
    }

    [Test]
    public async Task Throwing_checkpoint_capture_retries_before_releasing_native_storage()
    {
        var fixture = new Fixture();
        var assignment = fixture.Scheduler.Assign(1);
        var delivery = new Delivery(1);
        delivery.Complete();
        fixture.Enqueue(assignment, delivery, "checkpoint");
        fixture.Source.ThrowCommit = true;
        await Assert.That(() => fixture.Scheduler.Pump()).Throws<ApplicationException>();
        await Assert.That(delivery.Releases).IsEqualTo(0);
        fixture.Source.ThrowCommit = false;
        fixture.Scheduler.Pump();
        await Assert.That(delivery.Releases).IsEqualTo(1);
        await Assert.That(fixture.Source.Commits.Last().Position).IsEqualTo("checkpoint");
    }

    [Test]
    public async Task Default_foreign_and_duplicate_assignment_tokens_cannot_change_ownership()
    {
        var first = new Fixture();
        var second = new Fixture();
        var assignment = first.Scheduler.Assign(1);
        await Assert.That(second.Scheduler.TryEnqueue(assignment, new(1), "foreign")).IsFalse();
        await Assert.That(first.Scheduler.TryEnqueue(default, new(1), "default")).IsFalse();
        second.Scheduler.Lose(assignment);
        await Assert.That(() => first.Scheduler.Assign(1)).Throws<InvalidOperationException>();
        first.Scheduler.Lose(assignment);
        first.Scheduler.Lose(assignment);
        first.Scheduler.Pump();
        first.Scheduler.Assign(1);
    }

    [Test]
    public async Task Durable_completion_updates_existing_liveness_inputs_but_failed_gap_is_not_idle()
    {
        var clock = new InMemoryTestClock();
        var fixture = new Fixture(clock: clock);
        var assignment = fixture.Scheduler.Assign(1);
        var delivery = new Delivery(1);
        fixture.Enqueue(assignment, delivery, "success");
        fixture.Scheduler.Pump();
        var before = fixture.Scheduler.LastProgressAt;
        clock.Advance(TimeSpan.FromSeconds(1));
        delivery.Complete();
        fixture.Scheduler.Pump();
        await Assert.That(fixture.Scheduler.LastProgressAt).IsEqualTo(before.AddSeconds(1));
        await Assert.That(fixture.Scheduler.BufferedCount).IsEqualTo(0);
    }

    [Test]
    public async Task Synchronous_warmed_scheduler_adds_no_managed_allocations()
    {
        var source = new CountingSource();
        var scheduler = new PartitionScheduler<int, ScalarBinding, int, long>(source,
            static (_, _) => new(Disposition.Complete));
        var assignment = scheduler.Assign(1);
        Exercise(scheduler, assignment, 10_000);
        var before = GC.GetAllocatedBytesForCurrentThread();
        Exercise(scheduler, assignment, 10_000);
        var allocated = GC.GetAllocatedBytesForCurrentThread() - before;
        await Assert.That(allocated).IsEqualTo(0L);
        await Assert.That(source.Commits).IsEqualTo(20_000);
    }

    [Test]
    public async Task Failed_oldest_gap_keeps_later_durable_success_uncommitted()
    {
        var fixture = new Fixture();
        var assignment = fixture.Scheduler.Assign(1);
        var first = new Delivery(1);
        var later = new Delivery(2);
        fixture.Enqueue(assignment, first, "gap");
        fixture.Enqueue(assignment, later, "later");
        fixture.Scheduler.Pump();
        first.Completion.SetException(new ApplicationException("not durable"));
        later.Complete();
        fixture.Scheduler.Pump();
        await Assert.That(fixture.Source.Commits.Count).IsEqualTo(0);
        await Assert.That(later.Releases).IsEqualTo(0);
        await Assert.That(fixture.Scheduler.BufferedCount).IsEqualTo(2);
        fixture.Scheduler.Lose(assignment);
        fixture.Scheduler.Pump();
        await Assert.That(first.Releases + later.Releases).IsEqualTo(2);
    }

    [Test]
    public async Task Reusable_async_ValueTasks_are_consumed_once_without_scheduler_allocations()
    {
        var source = new CountingSource();
        var delivery = new CompletionSource();
        var scheduler = new PartitionScheduler<CompletionSource, CompletionBinding, int, long>(source,
            static (message, _) => message.Start());
        var assignment = scheduler.Assign(1);
        ExerciseAsync(scheduler, assignment, delivery, 10_000);
        var before = GC.GetAllocatedBytesForCurrentThread();
        ExerciseAsync(scheduler, assignment, delivery, 10_000);
        var allocated = GC.GetAllocatedBytesForCurrentThread() - before;
        await Assert.That(allocated).IsEqualTo(0L);
        await Assert.That(delivery.Results).IsEqualTo(20_000);
        await Assert.That(source.Commits).IsEqualTo(20_000);
    }

    [Test]
    public async Task Key_queues_survive_repeated_ring_wrap_and_reverse_active_completion()
    {
        var fixture = new Fixture(capacity: 8, concurrency: 3);
        var assignment = fixture.Scheduler.Assign(1);
        int[] keys = [1, 2, 1, 3, 2, 1, 4, 3];
        for (var round = 0; round < 50; round++)
        {
            var deliveries = new Delivery[keys.Length];
            for (var i = 0; i < keys.Length; i++)
            {
                deliveries[i] = new(keys[i]);
                fixture.Enqueue(assignment, deliveries[i], $"{round}:{i}");
            }
            // Complete the latest runnable delivery first, forcing gaps and repeated key-head promotion.
            for (var completed = 0; completed < keys.Length; completed++)
            {
                fixture.Scheduler.Pump();
                var selected = -1;
                for (var i = 0; i < deliveries.Length; i++)
                {
                    if (!deliveries[i].Started || deliveries[i].Completion.Task.IsCompleted) continue;
                    selected = i;
                    for (var earlier = 0; earlier < i; earlier++)
                        if (keys[earlier] == keys[i])
                            await Assert.That(deliveries[earlier].Completion.Task.IsCompleted).IsTrue();
                }
                await Assert.That(selected).IsGreaterThanOrEqualTo(0);
                deliveries[selected].Complete();
            }
            fixture.Scheduler.Pump();
            await Assert.That(fixture.Scheduler.BufferedCount).IsEqualTo(0);
            await Assert.That(fixture.Source.Commits.Last().Position).IsEqualTo($"{round}:7");
            foreach (var delivery in deliveries) await Assert.That(delivery.Releases).IsEqualTo(1);
        }
    }

    [Test]
    public async Task Same_key_queue_links_survive_admission_into_reused_ring_slots()
    {
        var fixture = new Fixture(capacity: 3, concurrency: 2);
        var assignment = fixture.Scheduler.Assign(1);
        var first = new Delivery(1);
        var independent = new Delivery(2);
        var next = new Delivery(1);
        fixture.Enqueue(assignment, first, "first");
        fixture.Enqueue(assignment, independent, "independent");
        fixture.Enqueue(assignment, next, "next");
        fixture.Scheduler.Pump();
        first.Complete();
        fixture.Scheduler.Pump();
        var wrapped = new Delivery(1);
        fixture.Enqueue(assignment, wrapped, "wrapped");
        fixture.Scheduler.Pump();
        await Assert.That(wrapped.Started).IsFalse();
        independent.Complete();
        fixture.Scheduler.Pump();
        next.Complete();
        fixture.Scheduler.Pump();
        await Assert.That(wrapped.Started).IsTrue();
        for (var i = 0; i < 20; i++)
        {
            var following = new Delivery(1);
            fixture.Enqueue(assignment, following, "following");
            fixture.Scheduler.Pump();
            await Assert.That(following.Started).IsFalse();
            wrapped.Complete();
            fixture.Scheduler.Pump();
            await Assert.That(following.Started).IsTrue();
            await Assert.That(wrapped.Releases).IsEqualTo(1);
            wrapped = following;
        }
        wrapped.Complete();
        fixture.Scheduler.Pump();
        await Assert.That(fixture.Scheduler.BufferedCount).IsEqualTo(0);
    }

    [Test]
    [Arguments(0, 4, 2, 1)]
    [Arguments(4, 0, 2, 1)]
    [Arguments(4, 4, 0, 1)]
    [Arguments(4, 4, 2, 0)]
    [Arguments(8, 4, 2, 1)]
    [Arguments(4, 4, 8, 1)]
    [Arguments(2, 4, 4, 3)]
    [Arguments(4, 4, 1, 2)]
    public async Task Invalid_limits_fail_before_native_calls(int perPartition, int total, int concurrency, int perPartitionConcurrency)
    {
        var source = new Source();
        await Assert.That(() => new PartitionScheduler<Delivery, Binding, int, string>(source,
            static (_, _) => new(Disposition.Complete), new()
            {
                MaxBufferedPerPartition = perPartition, MaxBufferedDeliveries = total,
                MaxConcurrency = concurrency, ConcurrencyPerPartition = perPartitionConcurrency,
            })).Throws<ArgumentOutOfRangeException>();
        await Assert.That(source.Pauses.Count + source.Resumes.Count + source.Commits.Count).IsEqualTo(0);
    }

    private static void ExerciseAsync(PartitionScheduler<CompletionSource, CompletionBinding, int, long> scheduler,
        PartitionAssignmentToken assignment, CompletionSource delivery, int iterations)
    {
        for (var i = 0; i < iterations; i++)
        {
            scheduler.TryEnqueue(assignment, delivery, i);
            scheduler.Pump();
            scheduler.Pump(); // Pending: no GetResult, no continuation registration.
            delivery.Complete();
            scheduler.Pump();
            scheduler.Pump(); // Completed: no second GetResult.
        }
    }

    private sealed class CompletionSource : IValueTaskSource<Disposition>
    {
        private ManualResetValueTaskSourceCore<Disposition> _completion;
        internal int Results;
        internal ValueTask<Disposition> Start() { _completion.Reset(); return new(this, _completion.Version); }
        internal void Complete() => _completion.SetResult(Disposition.Complete);
        public Disposition GetResult(short token) { Results++; return _completion.GetResult(token); }
        public ValueTaskSourceStatus GetStatus(short token) => _completion.GetStatus(token);
        public void OnCompleted(Action<object?> continuation, object? state, short token, ValueTaskSourceOnCompletedFlags flags) =>
            throw new InvalidOperationException("The scheduler must poll without allocating handler continuations.");
    }

    private readonly struct CompletionBinding : IPartitionMessageBinding<CompletionSource>
    {
        public static bool KeysEqual(CompletionSource first, CompletionSource second) => ReferenceEquals(first, second);
        public static ReadOnlySequence<byte> GetBody(CompletionSource message) => default;
        public static int? GetDeliveryCount(CompletionSource message) => null;
        public static bool TryGetHeader(CompletionSource message, string name, out WireString value) { value = default; return false; }
    }

    private static void Exercise(PartitionScheduler<int, ScalarBinding, int, long> scheduler,
        PartitionAssignmentToken assignment, int iterations)
    {
        for (var i = 0; i < iterations; i++)
        {
            if (!scheduler.TryEnqueue(assignment, i, i)) throw new InvalidOperationException("Unexpected backpressure.");
            scheduler.Pump();
        }
    }

    private sealed class Fixture
    {
        internal readonly Source Source = new();
        internal readonly PartitionScheduler<Delivery, Binding, int, string> Scheduler;
        internal Fixture(int capacity = 4, int concurrency = 2, int totalCapacity = 16, int totalConcurrency = 4, TimeProvider? clock = null)
        {
            Scheduler = new(Source, static (delivery, cancellation) =>
            {
                delivery.Started = true;
                delivery.Cancellation = cancellation;
                return new(delivery.Completion.Task);
            }, new()
            {
                MaxBufferedPerPartition = capacity, MaxBufferedDeliveries = totalCapacity,
                ConcurrencyPerPartition = concurrency, MaxConcurrency = totalConcurrency,
            }, clock);
        }
        internal void Enqueue(PartitionAssignmentToken assignment, Delivery delivery, string position)
        {
            if (!Scheduler.TryEnqueue(assignment, delivery, position)) throw new InvalidOperationException("Unexpected backpressure.");
        }
    }

    private sealed class Delivery(int key)
    {
        internal readonly int Key = key;
        internal readonly TaskCompletionSource<Disposition> Completion = new();
        internal bool Started;
        internal int Releases;
        internal CancellationToken Cancellation;
        internal void Complete() => Completion.TrySetResult(Disposition.Complete);
    }

    private readonly struct Binding : IPartitionMessageBinding<Delivery>
    {
        public static bool KeysEqual(Delivery first, Delivery second) => first.Key == second.Key;
        public static void Release(Delivery message) => message.Releases++;
        public static ReadOnlySequence<byte> GetBody(Delivery message) => default;
        public static int? GetDeliveryCount(Delivery message) => null;
        public static bool TryGetHeader(Delivery message, string name, out WireString value) { value = default; return false; }
    }

    private readonly struct ScalarBinding : IPartitionMessageBinding<int>
    {
        public static bool KeysEqual(int first, int second) => first == second;
        public static ReadOnlySequence<byte> GetBody(int message) => default;
        public static int? GetDeliveryCount(int message) => null;
        public static bool TryGetHeader(int message, string name, out WireString value) { value = default; return false; }
    }

    private sealed class Source : IPartitionSource<int, string>
    {
        internal readonly List<(int Partition, string Position)> Commits = [];
        internal readonly List<int> Pauses = [];
        internal readonly List<int> Resumes = [];
        internal bool ThrowCommit;
        public void Commit(int partition, string position)
        {
            if (ThrowCommit) throw new ApplicationException("native checkpoint capture failed");
            Commits.Add((partition, position));
        }
        public void Pause(int partition) => Pauses.Add(partition);
        public void Resume(int partition) => Resumes.Add(partition);
    }

    private sealed class CountingSource : IPartitionSource<int, long>
    {
        internal int Commits;
        public void Commit(int partition, long position) => Commits++;
        public void Pause(int partition) { }
        public void Resume(int partition) { }
    }
}
