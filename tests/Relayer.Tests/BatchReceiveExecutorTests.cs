using Microsoft.Extensions.DependencyInjection;

namespace Relayer.Tests;

public class BatchReceiveExecutorTests
{
    [Test]
    [Arguments(9, 10, 9)]
    [Arguments(10, 10, 10)]
    [Arguments(11, 10, 10)]
    public async Task Native_selection_caps_exact_count_boundary_and_uses_configured_wait(int configured, int nativeLimit, int expected)
    {
        await using var provider = BatchPipelineTests.Provider(configured, nativeLimit);
        var calls = 0;
        var executor = new BatchReceiveExecutor<int>(provider, "test", BatchPipelineTests.Definition(provider), "audit",
            (destination, wait, _) =>
            {
                calls++;
                if (destination.Length != expected || wait != TimeSpan.FromSeconds(10)) throw new Exception("Configuration not honored");
                destination.Span.Fill(42);
                return new(expected - 1); // a native byte limit may produce fewer messages
            }, (_, _, _) => throw new Exception("Native batching must take precedence"));
        var output = new int[expected + 1];
        var count = await executor.ReceiveAsync(output);
        await Assert.That(executor.UsesNativeBatching).IsTrue();
        await Assert.That(executor.MaximumSize).IsEqualTo(expected);
        await Assert.That(count).IsEqualTo(expected - 1);
        await Assert.That(output[^1]).IsEqualTo(0);
        await executor.ReceiveAsync(output);
        await Assert.That(calls).IsEqualTo(2);
        await Assert.That(provider.GetRequiredService<BatchPipelineTests.Transport>().Reads).IsEqualTo(1);
    }

    [Test]
    [Arguments(1, true)]
    [Arguments(10, false)]
    public async Task Fallback_requires_single_executor_when_native_capability_or_executor_is_absent(int nativeLimit, bool offerNative)
    {
        await using var provider = BatchPipelineTests.Provider(3, nativeLimit);
        var calls = 0;
        var executor = new BatchReceiveExecutor<int>(provider, "test", BatchPipelineTests.Definition(provider), "audit",
            offerNative ? (_, _, _) => throw new Exception("Capability alone cannot select native batching") : null,
            (destination, _, _) =>
            {
                if (destination.Length != 1) throw new Exception("Single receiver must receive one slot");
                destination.Span[0] = ++calls;
                return new(1);
            });
        var output = new int[4];
        await Assert.That(await executor.ReceiveAsync(output)).IsEqualTo(3);
        await Assert.That(executor.UsesNativeBatching).IsFalse();
        await Assert.That(string.Join(",", output)).IsEqualTo("1,2,3,0");
    }

    [Test]
    public async Task Partial_batch_wait_starts_at_first_delivery_and_deadline_is_not_reset_per_item()
    {
        await using var provider = BatchPipelineTests.Provider(4, 1);
        var clock = new ManualTime();
        var waits = new List<TimeSpan>();
        var executor = new BatchReceiveExecutor<int>(provider, "test", BatchPipelineTests.Definition(provider), "audit", singleReceive:
            (destination, wait, _) =>
            {
                waits.Add(wait);
                destination.Span[0] = waits.Count;
                clock.Advance(waits.Count == 1 ? TimeSpan.FromSeconds(50) : TimeSpan.FromSeconds(Math.Min(6, wait.TotalSeconds)));
                return new(1);
            }, timeProvider: clock);
        var output = new int[4];
        await Assert.That(await executor.ReceiveAsync(output)).IsEqualTo(3);
        await Assert.That(string.Join(",", waits.Select(wait => wait.TotalSeconds))).IsEqualTo("10,10,4");
        await Assert.That(output[^1]).IsEqualTo(0);
    }

    [Test]
    public async Task Receive_timeout_returns_partial_or_empty_batch_without_pending_background_reads()
    {
        await using var provider = BatchPipelineTests.Provider(4, 1);
        var calls = 0;
        var executor = new BatchReceiveExecutor<int>(provider, "test", BatchPipelineTests.Definition(provider), "audit", singleReceive:
            (destination, _, _) => { calls++; if (calls == 1) { destination.Span[0] = 42; return new(1); } return new(0); });
        var output = new int[4];
        await Assert.That(await executor.ReceiveAsync(output)).IsEqualTo(1);
        await Assert.That(calls).IsEqualTo(2);
        await Assert.That(await executor.ReceiveAsync(output)).IsEqualTo(0);
        await Assert.That(calls).IsEqualTo(3);
    }

    [Test]
    [Arguments(1)]
    [Arguments(10)]
    public async Task Startup_rejects_declaration_without_executable_path_with_consumer_context(int nativeLimit)
    {
        await using var provider = BatchPipelineTests.Provider(4, nativeLimit);
        await Assert.That(() => new BatchReceiveExecutor<int>(provider, "test", BatchPipelineTests.Definition(provider), "audit"))
            .Throws<InvalidOperationException>().WithMessageContaining("Consumer 'audit' from 'source' on transport 'test'");
        if (nativeLimit == 1)
            await Assert.That(() => new BatchReceiveExecutor<int>(provider, "test", BatchPipelineTests.Definition(provider), "audit", (_, _, _) => new(1)))
                .Throws<InvalidOperationException>().WithMessageContaining("has no executor");
    }

    [Test]
    [Arguments(-1)]
    [Arguments(2)]
    public async Task Invalid_executor_counts_propagate_without_returning_success(int count)
    {
        await using var provider = BatchPipelineTests.Provider(3, 1);
        var executor = new BatchReceiveExecutor<int>(provider, "test", BatchPipelineTests.Definition(provider), "audit", singleReceive: (_, _, _) => new(count));
        await Assert.That(async () => await executor.ReceiveAsync(new int[3])).Throws<InvalidOperationException>();
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task Receive_cancellation_is_observed_before_and_after_native_work(bool native)
    {
        await using var provider = BatchPipelineTests.Provider(3);
        using var cancellation = new CancellationTokenSource();
        var calls = 0;
        BatchReceiveDelegate<int> receive = (_, _, _) => { calls++; cancellation.Cancel(); return new(1); };
        var executor = new BatchReceiveExecutor<int>(provider, "test", BatchPipelineTests.Definition(provider), "audit",
            native ? receive : null, native ? null : receive);
        await Assert.That(async () => await executor.ReceiveAsync(new int[3], cancellation.Token)).Throws<OperationCanceledException>();
        await Assert.That(calls).IsEqualTo(1);
        await Assert.That(async () => await executor.ReceiveAsync(new int[3], cancellation.Token)).Throws<OperationCanceledException>();
        await Assert.That(calls).IsEqualTo(1);
    }

    private sealed class ManualTime : TimeProvider
    {
        private long _timestamp;
        public override long TimestampFrequency => TimeSpan.TicksPerSecond;
        public override long GetTimestamp() => _timestamp;
        internal void Advance(TimeSpan elapsed) => _timestamp += elapsed.Ticks;
    }
}
