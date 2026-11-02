using System.Diagnostics.Metrics;

namespace Relayer.TestFixtures;

// Providers own native transactions and deterministic contention observation.
public abstract class InboxContractTests
{
    protected abstract Task<InboxFixture> CreateFixtureAsync();

    protected abstract class InboxFixture : IAsyncDisposable
    {
        public abstract InboxConsumer Consumer(string name = "orders");
        public abstract Task<DurableState> ReadStateAsync();
        public abstract Task WaitForContentionAsync();
        public abstract ValueTask DisposeAsync();
    }

    protected abstract class InboxConsumer : IAsyncDisposable
    {
        // Runs after business writes and the outgoing send, inside the native transaction.
        public Func<MessageContext, ValueTask>? AfterSave { get; set; }
        public Func<ValueTask>? Settlement { get; set; }
        public abstract RelayerInstrumentation Instrumentation { get; }
        public abstract int Invocations { get; }
        public abstract int DirectSends { get; }
        public abstract Task DeliverAsync(Guid messageId, CancellationToken cancellation = default);
        public abstract ValueTask DisposeAsync();
    }

    protected readonly record struct DurableState(long Business, long Inbox, long Outbox);

    [Test]
    public async Task Redelivery_skips_handler_and_records_metric_after_atomic_commit()
    {
        await using var fixture = await CreateFixtureAsync();
        await using var consumer = fixture.Consumer();
        consumer.AfterSave = async context =>
        {
            await Assert.That(await fixture.ReadStateAsync()).IsEqualTo(new DurableState(0, 0, 0));
            context.Features.GetRequired<ISettlementFeature>().Complete();
        };
        long duplicates = 0;
        using var listener = new MeterListener();
        listener.InstrumentPublished = (instrument, meter) =>
        {
            if (instrument.Name == "relayer.inbox.duplicates" && ReferenceEquals(instrument.Meter.Scope, consumer.Instrumentation))
                meter.EnableMeasurementEvents(instrument);
        };
        listener.SetMeasurementEventCallback<long>((_, count, tags, _) =>
        {
            foreach (var tag in tags)
                if (tag.Key == "relayer.consumer" && Equals(tag.Value, "orders")) Interlocked.Add(ref duplicates, count);
        });
        listener.Start();
        var settlements = 0;
        consumer.Settlement = async () =>
        {
            await Assert.That(await fixture.ReadStateAsync()).IsEqualTo(new DurableState(1, 1, 1));
            settlements++;
        };
        var id = Guid.CreateVersion7();
        await consumer.DeliverAsync(id);
        await consumer.DeliverAsync(id);
        await Assert.That(consumer.Invocations).IsEqualTo(1);
        await Assert.That(duplicates).IsEqualTo(1L);
        await Assert.That(settlements).IsEqualTo(2);
        await Assert.That(consumer.DirectSends).IsEqualTo(0);
    }

    [Test]
    public async Task Identity_is_scoped_by_consumer()
    {
        await using var fixture = await CreateFixtureAsync();
        await using var first = fixture.Consumer("first");
        await using var second = fixture.Consumer("second");
        var id = Guid.CreateVersion7();
        await first.DeliverAsync(id);
        await second.DeliverAsync(id);
        await first.DeliverAsync(id);
        await second.DeliverAsync(id);
        await Assert.That(await fixture.ReadStateAsync()).IsEqualTo(new DurableState(2, 2, 2));
        await Assert.That(first.Invocations).IsEqualTo(1);
        await Assert.That(second.Invocations).IsEqualTo(1);
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task Concurrent_redelivery_waits_for_commit_or_runs_after_rollback(bool rollback)
    {
        await using var fixture = await CreateFixtureAsync();
        await using var firstConsumer = fixture.Consumer();
        await using var secondConsumer = fixture.Consumer();
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        firstConsumer.AfterSave = async _ =>
        {
            entered.SetResult();
            await release.Task.WaitAsync(deadline.Token);
            if (rollback) throw new InvalidOperationException("Roll back first delivery.");
        };
        var id = Guid.CreateVersion7();
        var first = firstConsumer.DeliverAsync(id, deadline.Token);
        Task? second = null;
        try
        {
            await entered.Task.WaitAsync(deadline.Token);
            second = secondConsumer.DeliverAsync(id, deadline.Token);
            await fixture.WaitForContentionAsync();
            await Assert.That(secondConsumer.Invocations).IsEqualTo(0);
        }
        finally
        {
            release.TrySetResult();
            // Drain both operations before their providers and database are disposed, even on assertion failure.
            try { await first; }
            catch (InvalidOperationException error) when (rollback && error.Message == "Roll back first delivery.") { }
            finally
            {
                if (second is not null) await second;
            }
        }
        if (rollback) await Assert.That(async () => await first).Throws<InvalidOperationException>();
        await Assert.That(secondConsumer.Invocations).IsEqualTo(rollback ? 1 : 0);
        await Assert.That(await fixture.ReadStateAsync()).IsEqualTo(new DurableState(1, 1, 1));
    }

    [Test]
    [Arguments("handler")]
    [Arguments("cancel")]
    [Arguments("retry")]
    [Arguments("deadletter")]
    public async Task Failed_or_noncomplete_work_rolls_back_and_redelivery_runs_handler(string failure)
    {
        await using var fixture = await CreateFixtureAsync();
        await using var consumer = fixture.Consumer();
        using var cancellation = new CancellationTokenSource();
        consumer.AfterSave = context =>
        {
            switch (failure)
            {
                case "handler": throw new InvalidOperationException("Handler failed after save.");
                case "cancel": cancellation.Cancel(); break;
                case "retry": context.Features.GetRequired<ISettlementFeature>().Retry(TimeSpan.Zero); break;
                case "deadletter": context.Features.GetRequired<ISettlementFeature>().DeadLetter("Rejected"); break;
            }
            return default;
        };
        var settled = 0;
        consumer.Settlement = () => { settled++; return default; };
        var id = Guid.CreateVersion7();
        if (failure is "retry" or "deadletter") await consumer.DeliverAsync(id);
        else if (failure == "cancel")
            await Assert.That(async () => await consumer.DeliverAsync(id, cancellation.Token)).Throws<OperationCanceledException>();
        else await Assert.That(async () => await consumer.DeliverAsync(id)).Throws<InvalidOperationException>();
        await Assert.That(settled).IsEqualTo(failure is "retry" or "deadletter" ? 1 : 0);
        await Assert.That(await fixture.ReadStateAsync()).IsEqualTo(new DurableState(0, 0, 0));
        consumer.AfterSave = null;
        await consumer.DeliverAsync(id);
        await Assert.That(consumer.Invocations).IsEqualTo(2);
        await Assert.That(await fixture.ReadStateAsync()).IsEqualTo(new DurableState(1, 1, 1));
    }

    [Test]
    public async Task Settlement_failure_after_commit_redelivers_without_repeating_work()
    {
        await using var fixture = await CreateFixtureAsync();
        await using var consumer = fixture.Consumer();
        var calls = 0;
        consumer.Settlement = () => ++calls == 1
            ? ValueTask.FromException(new InvalidOperationException("Lost acknowledgement.")) : default;
        var id = Guid.CreateVersion7();
        await Assert.That(async () => await consumer.DeliverAsync(id)).Throws<InvalidOperationException>();
        await Assert.That(await fixture.ReadStateAsync()).IsEqualTo(new DurableState(1, 1, 1));
        await consumer.DeliverAsync(id);
        await Assert.That(consumer.Invocations).IsEqualTo(1);
        await Assert.That(calls).IsEqualTo(2);
        await Assert.That(await fixture.ReadStateAsync()).IsEqualTo(new DurableState(1, 1, 1));
    }
}
