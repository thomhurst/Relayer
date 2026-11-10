using System.Text.Json;
using System.Threading.Channels;
using Relayer.Diagnostics;

namespace Relayer.Tests;

public class FrozenWorkerReadTests
{
    [Test]
    public async Task Measures_native_read_and_original_callback_separately_and_tracks_target_reuse()
    {
        var original = Channel.CreateBounded<byte>(new BoundedChannelOptions(8) { AllowSynchronousContinuations = true });
        var wrapper = new WorkerReadChannel(original, 3);
        using var cancellation = new CancellationTokenSource();
        var consumer = new Consumer();
        Action resume = consumer.Resume;
        for (var i = 0; i < 3; i++)
        {
            consumer.Pending = wrapper.Reader.ReadAsync(cancellation.Token);
            var wasPending = !consumer.Pending.IsCompleted;
            consumer.Retained = new byte[128]; // Between native read return and await registration.
            consumer.Pending.GetAwaiter().UnsafeOnCompleted(resume);
            await Assert.That(wasPending).IsTrue();
            await Assert.That(wrapper.Writer.TryWrite(42)).IsTrue();
            wrapper.Drain(i + 1);
            await Assert.That(consumer.Value).IsEqualTo((byte)42);
        }
        var report = JsonSerializer.SerializeToElement(wrapper.Report(1));
        await Assert.That(report.GetProperty("calls").GetInt32()).IsEqualTo(2);
        await Assert.That(report.GetProperty("continuationTargetsIncludingWarmup").GetInt32()).IsEqualTo(1);
        var bins = report.GetProperty("histogram").EnumerateArray().ToArray();
        await Assert.That(bins.Sum(bin => bin.GetProperty("calls").GetInt32())).IsEqualTo(2);
        await Assert.That(bins.All(bin => bin.GetProperty("registrationBytes").GetInt64() >= 152)).IsTrue();
        await Assert.That(bins.All(bin => bin.GetProperty("readBytes").GetInt64() > 0)).IsTrue();
        await Assert.That(bins.All(bin => bin.GetProperty("continuationBytes").GetInt64() >= 88)).IsTrue();
        await Assert.That(bins.All(bin => !bin.GetProperty("firstContinuationTargetInReadOrder").GetBoolean())).IsTrue();
        await Assert.That(() => wrapper.Report(1)).Throws<InvalidOperationException>();
        GC.KeepAlive(consumer.Retained);
    }

    [Test]
    public async Task Preserves_native_cancellation_and_drains_callback_after_notification()
    {
        var wrapper = new WorkerReadChannel(Channel.CreateBounded<byte>(8), 1);
        using var cancellation = new CancellationTokenSource();
        var pending = wrapper.Reader.ReadAsync(cancellation.Token);
        var notification = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var observedCancellation = false;
        pending.GetAwaiter().UnsafeOnCompleted(() =>
        {
            try { _ = pending.GetAwaiter().GetResult(); }
            catch (OperationCanceledException) { observedCancellation = true; }
            notification.SetResult();
        });
        cancellation.Cancel();
        await notification.Task;
        wrapper.Drain(1);
        await Assert.That(observedCancellation).IsTrue();
        var report = JsonSerializer.SerializeToElement(wrapper.Report(0));
        await Assert.That(report.GetProperty("calls").GetInt32()).IsEqualTo(1);
    }

    [Test]
    public async Task Rejects_synchronous_reads_and_buffer_overflow()
    {
        var original = Channel.CreateBounded<byte>(8);
        original.Writer.TryWrite(1);
        var wrapper = new WorkerReadChannel(original, 1);
        await Assert.That(() => { _ = wrapper.Reader.ReadAsync(); }).Throws<InvalidOperationException>();
        await Assert.That(() => { _ = wrapper.Reader.ReadAsync(); }).Throws<InvalidOperationException>();
    }

    private sealed class Consumer
    {
        public ValueTask<byte> Pending;
        public byte Value;
        public byte[]? Retained;
        public void Resume()
        {
            Value = Pending.GetAwaiter().GetResult();
            Retained = new byte[64];
        }
    }
}
