using Relayer.Diagnostics;

namespace Relayer.Tests;

public class FrozenInvocationAllocationTests
{
    [Test]
    public async Task Counts_exact_synchronous_allocations_and_preserves_pending_value_tasks()
    {
        var handler = new AllocatingHandler();
        Func<int, int, CancellationToken, ValueTask> invoke = handler.Invoke;
        for (var i = 0; i < 100; i++) _ = invoke(0, 0, default);
        var before = GC.GetAllocatedBytesForCurrentThread();
        _ = invoke(0, 0, default);
        var expected = GC.GetAllocatedBytesForCurrentThread() - before;
        var recorder = new InvocationAllocations(3);
        // Warm the instrumented generic method before enabling the measured region.
        _ = recorder.Invoke(invoke, 0, 0, default);
        recorder.Enabled = true;
        var first = recorder.Invoke(invoke, 0, 0, default);
        var second = recorder.Invoke(invoke, 0, 0, default);
        var third = recorder.Invoke(invoke, 0, 0, default);
        recorder.Enabled = false;
        var report = recorder.Report();
        await Assert.That(first.IsCompleted).IsFalse();
        await Assert.That(second.IsCompleted).IsFalse();
        await Assert.That(third.IsCompleted).IsFalse();
        await Assert.That(expected).IsGreaterThan(0);
        await Assert.That(report.Invocations).IsEqualTo(3);
        await Assert.That(report.Bytes).IsEqualTo(expected * 3);
        await Assert.That(report.Histogram.Count).IsEqualTo(1);
        await Assert.That(report.Histogram[0].Bytes).IsEqualTo(expected);
        await Assert.That(report.Histogram[0].Invocations).IsEqualTo(3);
        handler.Complete();
        await first;
        await second;
        await third;
    }

    [Test]
    public async Task Disabled_recording_excludes_warmup_and_never_awaits_the_handler()
    {
        var handler = new AllocatingHandler();
        var recorder = new InvocationAllocations(0);
        var pending = recorder.Invoke<int, int>(handler.Invoke, 0, 0, default);
        var report = recorder.Report();
        await Assert.That(pending.IsCompleted).IsFalse();
        await Assert.That(report.Invocations).IsEqualTo(0);
        await Assert.That(report.Bytes).IsEqualTo(0);
        handler.Complete();
        await pending;
    }

    [Test]
    public async Task Refuses_incomplete_or_still_active_measurements()
    {
        var recorder = new InvocationAllocations(1) { Enabled = true };
        Func<int, int, CancellationToken, ValueTask> invoke = static (_, _, _) => ValueTask.CompletedTask;
        _ = recorder.Invoke(invoke, 0, 0, default);
        await Assert.That(() => recorder.Report()).Throws<InvalidOperationException>();
        await Assert.That(() => { _ = recorder.Invoke(invoke, 0, 0, default); }).Throws<InvalidOperationException>();
        recorder.Enabled = false;
        var report = recorder.Report();
        await Assert.That(report.Invocations).IsEqualTo(1);
        await Assert.That(report.Bytes).IsEqualTo(0);
    }

    [Test]
    public async Task Result_calls_count_only_synchronous_work_and_preserve_the_pending_result()
    {
        var completion = new TaskCompletionSource<int>(TaskCreationOptions.RunContinuationsAsynchronously);
        byte[]? retained = null;
        Func<ValueTask<int>> invoke = () =>
        {
            retained = new byte[64];
            return new ValueTask<int>(completion.Task);
        };
        var recorder = new InvocationAllocations(1);
        _ = recorder.Invoke(invoke); // Warmup is excluded.
        var before = GC.GetAllocatedBytesForCurrentThread();
        _ = invoke();
        var expected = GC.GetAllocatedBytesForCurrentThread() - before;
        recorder.Enabled = true;
        var pending = recorder.Invoke(invoke);
        recorder.Enabled = false;
        var report = recorder.Report();
        await Assert.That(pending.IsCompleted).IsFalse();
        await Assert.That(report.Invocations).IsEqualTo(1);
        await Assert.That(report.Bytes).IsEqualTo(expected);
        await Assert.That(expected).IsGreaterThan(0);
        completion.SetResult(42);
        await Assert.That(await pending).IsEqualTo(42);
        GC.KeepAlive(retained);
    }

    [Test]
    public async Task Generic_driver_regions_preserve_pending_result_and_count_only_synchronous_callback()
    {
        var completion = new TaskCompletionSource<int>(TaskCreationOptions.RunContinuationsAsynchronously);
        var state = (Completion: completion, Keep: new byte[1][]);
        Func<(TaskCompletionSource<int> Completion, byte[][] Keep), ValueTask<int>> invoke = static value =>
        {
            value.Keep[0] = new byte[64];
            return new ValueTask<int>(value.Completion.Task);
        };
        var recorder = new InvocationAllocations(2);
        for (var i = 0; i < 100; i++) _ = recorder.Invoke(invoke, state);
        var before = GC.GetAllocatedBytesForCurrentThread();
        _ = invoke(state);
        var expected = GC.GetAllocatedBytesForCurrentThread() - before;
        recorder.Enabled = true;
        var pending = recorder.Invoke(invoke, state);
        _ = recorder.Invoke(invoke, state);
        recorder.Enabled = false;
        var report = recorder.Report();
        await Assert.That(pending.IsCompleted).IsFalse();
        await Assert.That(expected).IsGreaterThan(0);
        await Assert.That(report.Invocations).IsEqualTo(2);
        await Assert.That(report.Bytes).IsEqualTo(expected * 2);
        await Assert.That(report.Histogram[0].Invocations).IsEqualTo(2);
        completion.SetResult(42);
        await Assert.That(await pending).IsEqualTo(42);
        GC.KeepAlive(state.Keep);
    }

    private sealed class AllocatingHandler
    {
        private readonly TaskCompletionSource _completion = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private byte[]? _lastAllocation;

        public ValueTask Invoke(int message, int context, CancellationToken cancellationToken)
        {
            _lastAllocation = new byte[64];
            return new ValueTask(_completion.Task);
        }

        public void Complete()
        {
            GC.KeepAlive(_lastAllocation);
            _completion.SetResult();
        }
    }
}
