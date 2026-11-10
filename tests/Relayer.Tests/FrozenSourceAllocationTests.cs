using Relayer.Diagnostics;
using System.Threading.Channels;

namespace Relayer.Tests;

public class FrozenSourceAllocationTests
{
    [Test]
    public async Task Tracks_reference_reuse_across_warmup_and_preserves_pending_completion()
    {
        var completion = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        Func<int, int, CancellationToken, ValueTask> invoke = (_, _, _) => new ValueTask(completion.Task);
        var recorder = new SourceAllocations(2);
        _ = recorder.Invoke(invoke, 0, 0, default);
        recorder.Enabled = true;
        var pending = recorder.Invoke(invoke, 0, 0, default);
        var second = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        _ = recorder.Invoke<int, int>((_, _, _) => new ValueTask(second.Task), 0, 0, default);
        await Assert.That(() => recorder.Report()).Throws<InvalidOperationException>();
        await Assert.That(() => { _ = recorder.Invoke(invoke, 0, 0, default); }).Throws<InvalidOperationException>();
        recorder.Enabled = false;
        var report = recorder.Report();
        await Assert.That(report.Invocations).IsEqualTo(2);
        await Assert.That(report.DistinctSourcesIncludingWarmup).IsEqualTo(2);
        await Assert.That(report.Histogram.Count).IsEqualTo(2);
        await Assert.That(report.Histogram[0].FirstObservedSource).IsFalse();
        await Assert.That(report.Histogram[1].FirstObservedSource).IsTrue();
        await Assert.That(pending.IsCompleted).IsFalse();
        completion.SetResult();
        second.SetResult();
        await pending;
    }

    [Test]
    public async Task Counts_pending_native_reads_without_consuming_their_results()
    {
        var channel = Channel.CreateBounded<byte>(8);
        using var cancellation = new CancellationTokenSource();
        var recorder = new SourceAllocations(2) { Enabled = true };
        var first = recorder.Read(channel.Reader, cancellation.Token);
        var second = recorder.Read(channel.Reader, cancellation.Token);
        recorder.Enabled = false;
        var report = recorder.Report();
        await Assert.That(first.IsCompleted).IsFalse();
        await Assert.That(second.IsCompleted).IsFalse();
        await Assert.That(report.Invocations).IsEqualTo(2);
        await Assert.That(report.DistinctSourcesIncludingWarmup).IsEqualTo(2);
        await Assert.That(report.Histogram.All(bin => bin.FirstObservedSource && bin.Bytes > 0)).IsTrue();
        await channel.Writer.WriteAsync(41);
        await channel.Writer.WriteAsync(42);
        await Assert.That(await first).IsEqualTo((byte)41);
        await Assert.That(await second).IsEqualTo((byte)42);
    }

    [Test]
    public async Task Rejects_synchronous_handlers_instead_of_reporting_a_missing_source_as_reuse()
    {
        var recorder = new SourceAllocations(1) { Enabled = true };
        await Assert.That(() => { _ = recorder.Invoke<int, int>(static (_, _, _) => ValueTask.CompletedTask, 0, 0, default); })
            .Throws<InvalidOperationException>();
    }
}
