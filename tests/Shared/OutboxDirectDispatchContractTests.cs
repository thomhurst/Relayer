using System.Collections.Concurrent;
using Relayer.Transports;

namespace Relayer.TestFixtures;

public abstract class OutboxDirectDispatchContractTests
{
    protected virtual bool IsolatesFailedNativeBatches => false;

    protected abstract Task<DispatchFixture> CreateFixtureAsync(RecordingTransport transport, int batchSize = 100,
        int queueCapacity = 256);

    protected abstract class DispatchFixture : IAsyncDisposable
    {
        public abstract Task<DispatchSession> OpenSessionAsync();
        public abstract Task<Row[]> ReadRowsAsync();
        // Drain committed work before returning. Tests release blocked native sends first.
        public abstract Task StopAsync();
        public abstract ValueTask DisposeAsync();

        public async Task CommitAsync(params OutboxRecord[] records)
        {
            await using var session = await OpenSessionAsync();
            await session.BeginTransactionAsync();
            foreach (var record in records) await session.EnlistAsync(record);
            await session.SaveAsync();
            await session.CommitAsync();
        }
    }

    // One application scope can reuse its buffers across multiple native transactions.
    protected abstract class DispatchSession : IAsyncDisposable
    {
        public abstract Task BeginTransactionAsync();
        public abstract ValueTask EnlistAsync(OutboxRecord record);
        public abstract Task SaveAsync();
        public abstract Task CommitAsync();
        public abstract Task RollbackAsync();
        public abstract ValueTask DisposeAsync();
    }

    protected readonly record struct Row(Guid Id, int Attempts, string? Error, bool HasClaim, bool HasClaimExpiry);

    [Test]
    public async Task Explicit_transactions_dispatch_only_after_commit_and_rollback_never_sends()
    {
        var transport = new RecordingTransport { Block = true };
        await using var fixture = await CreateFixtureAsync(transport);
        var committed = Record(2);
        await using (var session = await fixture.OpenSessionAsync())
        {
            await session.BeginTransactionAsync();
            await session.EnlistAsync(Record(1));
            await session.SaveAsync();
            await session.RollbackAsync();
            await session.BeginTransactionAsync();
            await session.EnlistAsync(committed);
            await session.SaveAsync();
            await Assert.That(transport.Entered.Task.IsCompleted).IsFalse();
            await Assert.That((await fixture.ReadRowsAsync()).Length).IsEqualTo(0);
            await session.CommitAsync();
            await transport.Entered.Task.WaitAsync(TimeSpan.FromSeconds(30));
            await Assert.That(await fixture.ReadRowsAsync()).IsEquivalentTo(new[] { new Row(committed.Id, 0, null, true, true) });
        }
        // The native send outlives the application scope and its transaction.
        transport.Release.TrySetResult();
        await fixture.StopAsync();
        await Assert.That(transport.Received.Select(sent => sent.Body[0])).IsEquivalentTo(new byte[] { 2 });
        await Assert.That(Identity(transport.Received.Single())).IsEqualTo(committed.Id);
        await Assert.That((await fixture.ReadRowsAsync()).Length).IsEqualTo(0);
    }

    [Test]
    public async Task Pending_native_batch_keeps_original_bytes_while_later_transactions_reuse_scoped_buffers()
    {
        var transport = new RecordingTransport { Block = true };
        await using var fixture = await CreateFixtureAsync(transport);
        var body = new byte[] { 1, 255, 0 };
        var header = new byte[] { 0xff, 0, 0xfe };
        var first = Record(1, headers: [new("same", new(header)), new("same", new("second"))], body: body);
        await using (var session = await fixture.OpenSessionAsync())
        {
            await session.BeginTransactionAsync();
            await session.EnlistAsync(first);
            await session.EnlistAsync(Record(2));
            Array.Fill(body, (byte)42);
            Array.Fill(header, (byte)42);
            await session.SaveAsync();
            await session.CommitAsync();
            await transport.Entered.Task.WaitAsync(TimeSpan.FromSeconds(30));
            await session.BeginTransactionAsync();
            await session.EnlistAsync(Record(3));
            await session.EnlistAsync(Record(4));
            await session.SaveAsync();
            await session.CommitAsync();
        }
        transport.Release.TrySetResult();
        await fixture.StopAsync();
        var sent = transport.Received.Single(message => Identity(message) == first.Id);
        await Assert.That(sent.Body).IsEquivalentTo(new byte[] { 1, 255, 0 });
        await Assert.That(sent.Headers.Select(value => value.Key)).IsEquivalentTo(new[] { "ce_id", "same", "same" });
        await Assert.That(sent.Headers[1].Value).IsEquivalentTo(new byte[] { 0xff, 0, 0xfe });
        await Assert.That(sent.Headers[2].Value).IsEquivalentTo("second"u8.ToArray());
        await Assert.That(transport.Received.Select(message => message.Body[0])).IsEquivalentTo(new byte[] { 1, 2, 3, 4 });
        await Assert.That((await fixture.ReadRowsAsync()).Length).IsEqualTo(0);
    }

    [Test]
    public async Task Native_batch_limits_and_destinations_bound_sends()
    {
        var transport = new RecordingTransport();
        await using var fixture = await CreateFixtureAsync(transport, batchSize: 3);
        await fixture.CommitAsync(Record(1, "alpha"), Record(2, "beta"), Record(3, "alpha"), Record(4, "single"),
            Record(5, "alpha"), Record(6, "beta"), Record(7, "single"));
        await fixture.StopAsync();
        await Assert.That(transport.Calls.Select(call => $"{call.Destination}:{call.Count}")).IsEquivalentTo(new[]
            { "alpha:2", "alpha:1", "beta:2", "single:1", "single:1" });
        await Assert.That(transport.Received.Select(sent => sent.Body[0])).IsEquivalentTo(new byte[] { 1, 2, 3, 4, 5, 6, 7 });
        await Assert.That((await fixture.ReadRowsAsync()).Length).IsEqualTo(0);
    }

    [Test]
    public async Task Ordered_and_future_rows_remain_durable_while_due_unordered_rows_dispatch()
    {
        var transport = new RecordingTransport();
        await using var fixture = await CreateFixtureAsync(transport);
        var ordered = Record(1, orderingKey: new WireString(""));
        var future = Record(2, deliverAt: DateTimeOffset.UtcNow.AddDays(1));
        var due = Record(3, deliverAt: DateTimeOffset.UtcNow.AddDays(-1));
        await fixture.CommitAsync(ordered, future, due);
        await fixture.StopAsync();
        await Assert.That(transport.Received.Select(sent => sent.Body[0])).IsEquivalentTo(new byte[] { 3 });
        await Assert.That(Identity(transport.Received.Single())).IsEqualTo(due.Id);
        await Assert.That(await fixture.ReadRowsAsync()).IsEquivalentTo(new[]
            { new Row(ordered.Id, 0, null, false, false), new Row(future.Id, 0, null, false, false) });
    }

    [Test]
    public async Task Native_failure_is_recorded_once_and_does_not_prevent_next_commit_dispatch()
    {
        var transport = new RecordingTransport { FailFirst = 1 };
        await using var fixture = await CreateFixtureAsync(transport);
        var failed = Record(1);
        await fixture.CommitAsync(failed);
        await transport.Entered.Task.WaitAsync(TimeSpan.FromSeconds(30));
        await fixture.CommitAsync(Record(2));
        await fixture.StopAsync();
        await Assert.That(transport.Calls.Count).IsEqualTo(2);
        await Assert.That(transport.Received.Select(sent => sent.Body[0])).IsEquivalentTo(new byte[] { 2 });
        await Assert.That(await fixture.ReadRowsAsync()).IsEquivalentTo(new[]
            { new Row(failed.Id, 1, "native send failed", false, false) });
    }

    [Test]
    public async Task Partial_native_batch_failure_preserves_or_completes_every_row_for_at_least_once_delivery()
    {
        var transport = new RecordingTransport { PartialFirst = 1 };
        await using var fixture = await CreateFixtureAsync(transport);
        var first = Record(1);
        var second = Record(2);
        await fixture.CommitAsync(first, second);
        await transport.Entered.Task.WaitAsync(TimeSpan.FromSeconds(30));
        await fixture.CommitAsync(Record(3));
        await fixture.StopAsync();
        if (IsolatesFailedNativeBatches)
        {
            // The failed batch accepted one row before throwing. Single-send isolation
            // completes both rows, duplicating the accepted row under at-least-once delivery.
            await Assert.That(transport.Calls.Select(call => call.Count)).IsEquivalentTo(new[] { 2, 1, 1, 1 });
            await Assert.That(transport.Received.Select(sent => sent.Body[0])).IsEquivalentTo(new byte[] { 1, 1, 2, 3 });
            await Assert.That((await fixture.ReadRowsAsync()).Length).IsEqualTo(0);
            return;
        }
        await Assert.That(transport.Calls.Select(call => call.Count)).IsEquivalentTo(new[] { 2, 1 });
        await Assert.That(transport.Received.Count).IsEqualTo(2);
        await Assert.That(transport.Received.Count(sent => sent.Body[0] == 3)).IsEqualTo(1);
        await Assert.That(transport.Received.Count(sent => sent.Body[0] is 1 or 2)).IsEqualTo(1);
        await Assert.That(await fixture.ReadRowsAsync()).IsEquivalentTo(new[]
        {
            new Row(first.Id, 1, "native send failed", false, false),
            new Row(second.Id, 1, "native send failed", false, false)
        });
    }

    [Test]
    public async Task Full_queue_keeps_commit_successful_and_leaves_overflow_durable()
    {
        var transport = new RecordingTransport { Block = true };
        await using var fixture = await CreateFixtureAsync(transport, queueCapacity: 1);
        var overflow = Record(3);
        await fixture.CommitAsync(Record(1));
        await transport.Entered.Task.WaitAsync(TimeSpan.FromSeconds(30));
        await fixture.CommitAsync(Record(2));
        await fixture.CommitAsync(overflow);
        await Assert.That((await fixture.ReadRowsAsync()).Length).IsEqualTo(3);
        transport.Release.TrySetResult();
        await fixture.StopAsync();
        await Assert.That(transport.Received.Select(sent => sent.Body[0])).IsEquivalentTo(new byte[] { 1, 2 });
        await Assert.That(await fixture.ReadRowsAsync()).IsEquivalentTo(new[] { new Row(overflow.Id, 0, null, false, false) });
    }

    private static OutboxRecord Record(byte value, string destination = "destination", WireString? orderingKey = null,
        DateTimeOffset? deliverAt = null, KeyValuePair<string, WireString>[]? headers = null, byte[]? body = null)
    {
        var id = Guid.CreateVersion7();
        KeyValuePair<string, WireString>[] wireHeaders = [new("ce_id", new(id.ToString())), .. headers ?? []];
        return new(id, "events", "dispatch.probe;v=1",
            new(destination, body ?? [value], wireHeaders, orderingKey, deliverAt),
            DateTimeOffset.UtcNow);
    }

    private static Guid Identity(Sent message) => Guid.Parse(System.Text.Encoding.UTF8.GetString(
        message.Headers.Single(header => header.Key == "ce_id").Value));

    public sealed record Sent(byte[] Body, KeyValuePair<string, byte[]>[] Headers);
    public sealed record NativeCall(string Destination, int Count);

    public sealed class RecordingTransport : ITransport
    {
        public bool Block { get; init; }
        public string? CapabilityFailure { get; init; }
        public int FailFirst;
        public int PartialFirst;
        public TaskCompletionSource Entered { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public TaskCompletionSource Release { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public ConcurrentQueue<Sent> Received { get; } = new();
        public ConcurrentQueue<NativeCall> Calls { get; } = new();
        public string Name => "events";

        public TransportCapabilities GetCapabilities(string endpoint)
        {
            if (endpoint == "alpha")
            {
                if (CapabilityFailure == "throw") throw new InvalidOperationException("invalid destination");
                if (CapabilityFailure is "zero" or "negative")
                    return new() { MaxBatchSize = CapabilityFailure == "zero" ? 0 : -1 };
            }
            return new() { OrderedByKey = true, MaxBatchSize = endpoint == "single" ? 1 : 2 };
        }

        public async ValueTask SendAsync(OutgoingMessage message, CancellationToken cancellationToken)
        {
            Calls.Enqueue(new(message.Destination, 1));
            Entered.TrySetResult();
            if (Block) await Release.Task.WaitAsync(cancellationToken);
            if (Interlocked.Exchange(ref FailFirst, 0) != 0) throw new InvalidOperationException("native send failed");
            Record(message);
        }

        public async ValueTask SendBatchAsync(ReadOnlyMemory<OutgoingMessage> messages, CancellationToken cancellationToken)
        {
            Calls.Enqueue(new(messages.Span[0].Destination, messages.Length));
            Entered.TrySetResult();
            if (Block) await Release.Task.WaitAsync(cancellationToken);
            if (Interlocked.Exchange(ref PartialFirst, 0) != 0)
            {
                Record(messages.Span[0]);
                throw new InvalidOperationException("native send failed");
            }
            if (Interlocked.Exchange(ref FailFirst, 0) != 0) throw new InvalidOperationException("native send failed");
            for (var i = 0; i < messages.Length; i++) Record(messages.Span[i]);
        }

        private void Record(OutgoingMessage message)
        {
            // Due scheduling is handled by the outbox, not this native endpoint.
            if (message.DeliverAt is not null) throw new NotSupportedException("Native scheduling is unavailable.");
            var headers = new KeyValuePair<string, byte[]>[message.Headers.Length];
            for (var i = 0; i < headers.Length; i++)
            {
                var header = message.Headers.Span[i];
                headers[i] = new(header.Key, header.Value.Text is { } text
                    ? System.Text.Encoding.UTF8.GetBytes(text) : header.Value.Utf8.ToArray());
            }
            Received.Enqueue(new(message.Body.ToArray(), headers));
        }

        public ValueTask<TopologyReport> ApplyTopologyAsync(TopologyMode mode, CancellationToken cancellationToken) => new(new TopologyReport([]));
        public Task StartAsync(CancellationToken cancellationToken) => Task.CompletedTask;
        public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
        public ValueTask DisposeAsync() => default;
    }
}
