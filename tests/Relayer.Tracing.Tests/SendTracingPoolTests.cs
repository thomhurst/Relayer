using System.Diagnostics;
using System.Runtime.InteropServices;
using Microsoft.Extensions.Options;
using Relayer.TestFixtures;

namespace Relayer.Tests;

public class SendTracingPoolTests
{
    [Test]
    [Arguments(false, false, false)]
    [Arguments(false, false, true)]
    [Arguments(false, true, false)]
    [Arguments(false, true, true)]
    [Arguments(true, false, false)]
    [Arguments(true, false, true)]
    [Arguments(true, true, false)]
    [Arguments(true, true, true)]
    public async Task Cross_thread_sends_retain_continuations_after_warmup(bool batch, bool listener, bool asynchronousCaller)
    {
        using var fixture = new SendTracingCompletionFixture(batch, listener);
        for (var i = 0; i < 16; i++) fixture.Run(traced: true, asynchronousCaller);
        var native = fixture.Run(traced: false, asynchronousCaller);
        long allocated = 0;
        for (var i = 0; i < 8; i++) allocated += fixture.Run(traced: true, asynchronousCaller);
        await Assert.That(native).IsEqualTo(0L);
        await Assert.That(allocated).IsEqualTo(0L);
    }

    [Test]
    [Arguments(false, false)]
    [Arguments(false, true)]
    [Arguments(true, false)]
    [Arguments(true, true)]
    public async Task Native_caller_warmup_is_sufficient_for_first_awaited_traced_completion(bool batch, bool listener)
    {
        using var fixture = new SendTracingCompletionFixture(batch, listener);
        // Warm Relayer storage without an async caller, then initialize only the native caller's dispatcher.
        for (var i = 0; i < 16; i++) fixture.Run(traced: true);
        fixture.Run(traced: false, asynchronousCaller: true);
        var allocated = fixture.Run(traced: true, asynchronousCaller: true);
        await Assert.That(allocated).IsEqualTo(0L);
    }

    [Test]
    [Arguments(false, false)]
    [Arguments(true, false)]
    [Arguments(true, true)]
    public async Task Overlapping_sends_reuse_preparation_storage_after_warmup(bool batch, bool createSpans)
    {
        using var tracing = new MessageTracing(Options.Create(new TracingOptions { CreateSpansForBatches = createSpans }), []);
        using var listener = new ActivityListener
        {
            ShouldListenTo = source => ReferenceEquals(source, tracing.Source),
            Sample = static (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.None,
        };
        ActivitySource.AddActivityListener(listener);
        // Exclude ActivitySource's root trace ID allocation. Dropped spans still prepare headers and batch links.
        using var ambient = new Activity("request").Start();
        var sink = new AllocationSink();
        var send = new SendTracing(tracing, new() { System = "test.native", Destination = "orders" }, sink.Send, sink.SendBatch);
        var messages = new[] { new OutgoingMessage("orders", new byte[] { 1 }), new("orders", new byte[] { 2 }) };
        var pending = new ValueTask[128];
        long allocated = 0;
        for (var round = 0; round < 4; round++)
        {
            sink.Completion = new();
            allocated = 0;
            for (var i = 0; i < pending.Length; i++)
            {
                var before = GC.GetAllocatedBytesForCurrentThread();
                pending[i] = batch ? send.SendBatchAsync(messages) : send.SendAsync(messages[0]);
                // Measure through native entry, before async builder suspension. This isolates preparation storage
                // from framework async state-machine pooling, which has its own bounded retention policy.
                allocated += sink.EntryAllocated - before;
            }
            sink.Completion.SetResult();
            foreach (var operation in pending) await operation;
        }
        await Assert.That(allocated).IsEqualTo(0L);
    }

    [Test]
    [Arguments(false, false)]
    [Arguments(true, false)]
    [Arguments(true, true)]
    public async Task Concurrent_providers_keep_borrowed_storage_exclusive_through_completion(bool batch, bool createSpans)
    {
        using var first = new ConcurrentFixture(batch, createSpans, "first");
        using var second = new ConcurrentFixture(batch, createSpans, "second");
        var fixtures = new[] { first, second };
        await Task.WhenAll(fixtures.SelectMany(fixture => Enumerable.Range(0, 128)
            .Select(index => Task.Run(() => fixture.Start(index)))));
        await VerifyBorrowedStorage(fixtures, batch, createSpans);

        // Return half the states concurrently, including faulted and cancelled sends. The other half stay borrowed.
        await Task.WhenAll(fixtures.SelectMany(fixture => Enumerable.Range(0, 64).Select(index => Task.Run(() =>
        {
            var slot = fixture.Slots[index];
            if (index % 3 == 0) slot.Completion.SetException(slot.Error);
            else if (index % 3 == 1)
            {
                slot.Cancellation.Cancel();
                slot.Completion.SetCanceled(slot.Cancellation.Token);
            }
            else slot.Completion.SetResult();
        }))));
        foreach (var fixture in fixtures)
        {
            for (var i = 0; i < 64; i++)
            {
                var slot = fixture.Slots[i];
                Exception? error = null;
                try { await slot.Pending; }
                catch (Exception caught) { error = caught; }
                if (i % 3 == 0) await Assert.That(error).IsSameReferenceAs(slot.Error);
                else if (i % 3 == 1)
                    await Assert.That(((OperationCanceledException)error!).CancellationToken).IsEqualTo(slot.Cancellation.Token);
                else await Assert.That(error).IsNull();
                await Assert.That(slot.NativeCalls).IsEqualTo(1);
                await Assert.That(slot.AcceptedBody).IsEqualTo(slot.Messages[0].Body);
                slot.Reset();
            }
        }
        await Task.WhenAll(fixtures.SelectMany(fixture => Enumerable.Range(0, 64)
            .Select(index => Task.Run(() => fixture.Start(index)))));
        await VerifyBorrowedStorage(fixtures, batch, createSpans);
        await Task.WhenAll(fixtures.SelectMany(fixture => fixture.Slots.Select(slot => Task.Run(() => slot.Completion.SetResult()))));
        foreach (var fixture in fixtures)
            foreach (var slot in fixture.Slots) await slot.Pending;
    }

    private static async Task VerifyBorrowedStorage(ConcurrentFixture[] fixtures, bool batch, bool createSpans)
    {
        var headerArrays = new HashSet<object>(ReferenceEqualityComparer.Instance);
        var batchArrays = new HashSet<object>(ReferenceEqualityComparer.Instance);
        foreach (var fixture in fixtures)
        {
            foreach (var slot in fixture.Slots)
            {
                await Assert.That(slot.Pending.IsCompleted).IsFalse();
                await Assert.That(slot.NativeToken).IsEqualTo(slot.Cancellation.Token);
                await Assert.That(slot.Activity!.Source).IsSameReferenceAs(fixture.Tracing.Source);
                await Assert.That(slot.Activity.Duration).IsEqualTo(TimeSpan.Zero);
                var message = batch ? slot.Batch.Span[0] : slot.Single;
                await Assert.That(message.Body).IsEqualTo(slot.Messages[0].Body);
                await Assert.That(Header(message, "owner").ToString()).IsEqualTo(fixture.Owner);
                await Assert.That(Header(message, "traceparent").ToString()).IsEqualTo(slot.TraceParent);
                MemoryMarshal.TryGetArray(message.Headers, out var headers);
                await Assert.That(headerArrays.Add(headers.Array!)).IsTrue();
                if (!batch) continue;
                MemoryMarshal.TryGetArray(slot.Batch, out var messages);
                await Assert.That(batchArrays.Add(messages.Array!)).IsTrue();
                await Assert.That(slot.Batch.Length).IsEqualTo(2);
                await Assert.That(slot.Batch.Span[1].Body).IsEqualTo(slot.Messages[1].Body);
                await Assert.That(slot.Batch.Span[1].Headers).IsEqualTo(slot.Messages[1].Headers);
                await Assert.That(slot.Activity.Links.Count()).IsEqualTo(createSpans ? 2 : 1);
                await Assert.That(slot.Activity.Links.Last().Context.SpanId.ToString()).IsEqualTo("0123456789abcdef");
                if (createSpans)
                {
                    ActivityContext.TryParse(slot.TraceParent, null, true, out var context);
                    await Assert.That(slot.Activity.Links.First().Context.SpanId).IsEqualTo(context.SpanId);
                }
            }
        }
    }

    private static WireString Header(OutgoingMessage message, string name)
    {
        foreach (var header in message.Headers.Span)
            if (header.Key == name) return header.Value;
        throw new InvalidOperationException($"Missing header {name}.");
    }

    private sealed class ConcurrentFixture : IDisposable
    {
        private readonly ActivityListener _listener;
        private readonly SendTracing _send;
        private readonly bool _batch;
        internal readonly MessageTracing Tracing;
        internal readonly Slot[] Slots;
        internal readonly string Owner;

        internal ConcurrentFixture(bool batch, bool createSpans, string owner)
        {
            _batch = batch;
            Owner = owner;
            Tracing = new(Options.Create(new TracingOptions { CreateSpansForBatches = createSpans }), []);
            _listener = new()
            {
                ShouldListenTo = source => ReferenceEquals(source, Tracing.Source),
                Sample = static (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
            };
            ActivitySource.AddActivityListener(_listener);
            Slots = Enumerable.Range(0, 128).Select(index => new Slot(index, owner)).ToArray();
            _send = new(Tracing, new() { System = "test.native", Destination = "orders" }, Send, SendBatch);
        }

        internal void Start(int index)
        {
            var slot = Slots[index];
            slot.Pending = _batch ? _send.SendBatchAsync(slot.Messages, slot.Cancellation.Token)
                : _send.SendAsync(slot.Messages[0], slot.Cancellation.Token);
        }

        private ValueTask Send(OutgoingMessage message, CancellationToken token)
        {
            var slot = Slots[message.Body.Span[0]];
            slot.Single = message;
            return Capture(slot, message, token);
        }

        private ValueTask SendBatch(ReadOnlyMemory<OutgoingMessage> messages, CancellationToken token)
        {
            var slot = Slots[messages.Span[0].Body.Span[0]];
            slot.Batch = messages;
            return Capture(slot, messages.Span[0], token);
        }

        private static ValueTask Capture(Slot slot, OutgoingMessage message, CancellationToken token)
        {
            slot.Activity = Activity.Current;
            slot.TraceParent = Header(message, "traceparent").ToString();
            slot.NativeToken = token;
            // The native sink accepts the first message before a possible later batch failure.
            slot.AcceptedBody = message.Body;
            slot.NativeCalls++;
            return new(slot.Completion.Task);
        }

        public void Dispose()
        {
            foreach (var slot in Slots) slot.Cancellation.Dispose();
            _listener.Dispose();
            Tracing.Dispose();
        }
    }

    private sealed class Slot
    {
        internal readonly OutgoingMessage[] Messages;
        internal readonly Exception Error = new InvalidOperationException("native partial failure");
        internal CancellationTokenSource Cancellation = new();
        internal TaskCompletionSource Completion = new();
        internal ValueTask Pending;
        internal OutgoingMessage Single;
        internal ReadOnlyMemory<OutgoingMessage> Batch;
        internal Activity? Activity;
        internal string TraceParent = "";
        internal CancellationToken NativeToken;
        internal ReadOnlyMemory<byte> AcceptedBody;
        internal int NativeCalls;

        internal Slot(int index, string owner)
        {
            KeyValuePair<string, WireString>[] headers = [new("owner", new(owner))];
            KeyValuePair<string, WireString>[] external =
                [new("traceparent", new("00-0123456789abcdef0123456789abcdef-0123456789abcdef-01"u8.ToArray()))];
            Messages = [new("orders", new byte[] { (byte)index, 0 }, headers), new("orders", new byte[] { (byte)index, 1 }, external)];
        }

        internal void Reset()
        {
            Cancellation.Dispose();
            Cancellation = new();
            Completion = new();
            NativeCalls = 0;
        }
    }

    private sealed class AllocationSink
    {
        internal TaskCompletionSource Completion = null!;
        internal long EntryAllocated;
        internal ValueTask Send(OutgoingMessage message, CancellationToken cancellationToken)
        {
            EntryAllocated = GC.GetAllocatedBytesForCurrentThread();
            return new(Completion.Task);
        }
        internal ValueTask SendBatch(ReadOnlyMemory<OutgoingMessage> messages, CancellationToken cancellationToken)
        {
            EntryAllocated = GC.GetAllocatedBytesForCurrentThread();
            return new(Completion.Task);
        }
    }
}
