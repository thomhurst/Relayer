using System.Runtime.InteropServices;
using Relayer.Transports;

namespace Relayer.Tests;

public class AttemptSendBufferTests
{
    private static readonly TimeSpan s_sendTimeout = TimeSpan.FromSeconds(30);

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task Large_payload_burst_does_not_increase_storage_for_the_next_attempt(bool flush)
    {
        var buffer = new AttemptSendBuffer();
        var sink = new Sink();
        var large = new OutgoingMessage("orders", new byte[512 * 1024]);
        var small = new OutgoingMessage("orders", new byte[1]);

        for (var i = 0; i < 24; i++)
        {
            buffer.Add(sink, large, s_sendTimeout);
        }

        if (flush)
        {
            await buffer.FlushAsync(default);
            await Assert.That(sink.Bodies).Count().IsEqualTo(24);
        }

        buffer.Clear();
        sink.Bodies.Clear();

        for (var i = 0; i < 24; i++)
        {
            buffer.Add(sink, small, s_sendTimeout);
        }

        await buffer.FlushAsync(default);
        buffer.Clear();
        await Assert.That(sink.Bodies).Count().IsEqualTo(24);
        await Assert.That(sink.Bodies.All(body => body.Length == 1 && body.Capacity <= 64 * 1024)).IsTrue();
    }

    [Test]
    public async Task Reuse_stops_at_the_retained_slot_limit()
    {
        var buffer = new AttemptSendBuffer();
        var sink = new Sink();
        var message = new OutgoingMessage("orders", new byte[1]);
        const int BurstSize = 145;

        for (var i = 0; i < BurstSize; i++)
        {
            buffer.Add(sink, message, s_sendTimeout);
        }

        await buffer.FlushAsync(default);
        buffer.Clear();
        (int Length, int Capacity, WeakReference<byte[]> Array)[] firstAttempt = [.. sink.Bodies];
        sink.Bodies.Clear();

        for (var i = 0; i < BurstSize; i++)
        {
            buffer.Add(sink, message, s_sendTimeout);
        }

        await buffer.FlushAsync(default);
        buffer.Clear();
        await Assert.That(sink.Bodies).Count().IsEqualTo(BurstSize);

        for (var i = 0; i < BurstSize; i++)
        {
            bool reused = firstAttempt[i].Array.TryGetTarget(out byte[]? first)
                && sink.Bodies[i].Array.TryGetTarget(out byte[]? second)
                && ReferenceEquals(first, second);
            await Assert.That(reused).IsEqualTo(i < 128);
        }
    }

    [Test]
    public async Task Oversized_header_storage_is_discarded_after_a_failed_flush()
    {
        var buffer = new AttemptSendBuffer();
        var sink = new Sink();
        var largeValues = new byte[40 * 1024];
        KeyValuePair<string, WireString>[] manyHeaders = new KeyValuePair<string, WireString>[100];
        Array.Fill(manyHeaders, new("header", new WireString("value")));
        var value = new WireString(largeValues);
        KeyValuePair<string, WireString>[] largeHeader = [new("header", value)];
        buffer.Add(sink, new("orders", new byte[1], manyHeaders), s_sendTimeout);
        buffer.Add(sink, new("orders", new byte[1], largeHeader, value), s_sendTimeout);
        sink.FailSend = true;

        await Assert.That(async () => await buffer.FlushAsync(default)).Throws<InvalidOperationException>();
        buffer.Clear();
        sink.FailSend = false;
        sink.Bodies.Clear();
        sink.HeaderCapacities.Clear();
        KeyValuePair<string, WireString>[] smallHeader = [new("header", new WireString("value"))];

        for (var i = 0; i < 2; i++)
        {
            buffer.Add(sink, new("orders", new byte[1], smallHeader), s_sendTimeout);
        }

        await buffer.FlushAsync(default);
        buffer.Clear();
        await Assert.That(sink.HeaderCapacities).IsEquivalentTo([1, 1]);
        await Assert.That(sink.Bodies.All(body => body.Capacity <= 64 * 1024)).IsTrue();
    }

    private sealed class Sink : ITransport
    {
        public string Name => "sink";

        internal List<(int Length, int Capacity, WeakReference<byte[]> Array)> Bodies
        {
            get;
        } = [];

        internal List<int> HeaderCapacities
        {
            get;
        } = [];

        internal bool FailSend
        {
            get; set;
        }

        public ValueTask SendAsync(OutgoingMessage message, CancellationToken cancellationToken)
        {
            if (!MemoryMarshal.TryGetArray(message.Body, out var body) ||
                !MemoryMarshal.TryGetArray(message.Headers, out var headers))
            {
                throw new InvalidOperationException("Expected array-backed send storage.");
            }

            Bodies.Add((body.Count, body.Array!.Length, new(body.Array)));
            HeaderCapacities.Add(headers.Array!.Length);
            return FailSend ? ValueTask.FromException(new InvalidOperationException("Native send failed.")) : default;
        }

        public TransportCapabilities GetCapabilities(string endpoint)
        {
            throw new NotSupportedException();
        }

        public ValueTask<TopologyReport> ApplyTopologyAsync(TopologyMode mode, CancellationToken cancellationToken)
        {
            throw new NotSupportedException();
        }

        public ValueTask SendBatchAsync(ReadOnlyMemory<OutgoingMessage> messages, CancellationToken cancellationToken)
        {
            throw new NotSupportedException();
        }

        public Task StartAsync(CancellationToken cancellationToken)
        {
            throw new NotSupportedException();
        }

        public Task StopAsync(CancellationToken cancellationToken)
        {
            throw new NotSupportedException();
        }

        public ValueTask DisposeAsync()
        {
            return default;
        }
    }
}
