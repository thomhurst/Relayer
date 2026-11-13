using System.Buffers;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Relayer.Serialization;
using Relayer.Transports;

namespace Relayer.Tests;

public class SendRetentionTests
{
    [Test]
    public async Task Completed_burst_retains_at_most_64_operations_and_preserves_borrowed_messages()
    {
        var sink = new Sink();
        var route = Route(sink);
        var pending = new ValueTask[256];
        for (var i = 0; i < pending.Length; i++) pending[i] = route.SendAsync(i + 1, null, null, default, null);
        await Assert.That(Stored(route).Count).IsEqualTo(0);
        await Assert.That(sink.Messages.Select(message => message.Body.Length).Distinct().Count()).IsEqualTo(256);
        sink.Completion.SetResult();
        // Native completion must not return an operation before the caller consumes its ValueTask.
        await Assert.That(Stored(route).Count).IsEqualTo(0);
        foreach (var send in pending) await send;
        await Assert.That(Stored(route).Count).IsEqualTo(64);
        sink.Messages.Clear();
        for (var i = 0; i < 256; i++) await route.SendAsync(1, null, null, default, null);
        await Assert.That(Stored(route).Count).IsEqualTo(64);
    }

    [Test]
    [Arguments("body")]
    [Arguments("key")]
    [Arguments("headers")]
    [Arguments("value")]
    [Arguments("options")]
    [Arguments("callbacks")]
    public async Task Oversized_storage_is_discarded_after_success_or_failure(string storage)
    {
        var sink = new Sink();
        var route = Route(sink, storage == "key" ? new LargeKey() : null);
        Action<SendOptions>? configure = storage switch
        {
            "headers" => options => { for (var i = 0; i < 100; i++) options.Headers.Add(i.ToString(), "v"); },
            "options" => options => options.Headers.EnsureCapacity(1000),
            "callbacks" => options => { for (var i = 0; i < 32; i++) options.ConfigureNative<object>(static _ => { }); },
            _ => null,
        };
        // Exercise retained UTF-8 envelope values, not caller-owned header strings.
        SendRoute<int> actual = storage == "value" ? Route(sink, source: new string('s', 8192)) : route;
        var send = actual.SendAsync(storage == "body" ? 128 * 1024 : 1, configure, null, default, null);
        sink.Completion.SetResult();
        await send;
        await Assert.That(Stored(actual).Count).IsEqualTo(0);
        sink.Completion = new();
        send = actual.SendAsync(storage == "body" ? 128 * 1024 : 1, configure, null, default, null);
        sink.Completion.SetException(new InvalidOperationException("native failure"));
        await Assert.That(async () => await send).Throws<InvalidOperationException>();
        await Assert.That(Stored(actual).Count).IsEqualTo(0);
    }

    [Test]
    public async Task Large_body_is_collectible_while_route_stays_alive()
    {
        var sink = new Sink();
        sink.Completion.SetResult();
        var route = Route(sink);
        var body = SendLargeBody(route, sink);
        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();
        await Assert.That(body.TryGetTarget(out _)).IsFalse();
        await route.SendAsync(1, null, null, default, null);
        await Assert.That(Stored(route).Count).IsEqualTo(1);
        GC.KeepAlive(route);
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static WeakReference<byte[]> SendLargeBody(SendRoute<int> route, Sink sink)
    {
        // The native operation is already complete; this consumes the synchronous send without blocking.
        route.SendAsync(128 * 1024, null, null, default, null).GetAwaiter().GetResult();
        MemoryMarshal.TryGetArray(sink.Messages[0].Body, out var body);
        var reference = new WeakReference<byte[]>(body.Array!);
        sink.Messages.Clear();
        return reference;
    }

    [Test]
    public async Task Sender_burst_drops_overflow_and_clears_attempt_state()
    {
        var pool = new SenderPool(null!);
        pool.Enable();
        var rented = Enumerable.Range(0, 256).Select(_ => pool.Rent(null, new TenantContext(null!))).ToArray();
        foreach (var sender in rented) sender.EndAttempt();
        var reused = Enumerable.Range(0, 256).Select(_ => pool.Rent(null)).ToArray();
        await Assert.That(reused.Count(sender => rented.Contains(sender))).IsEqualTo(64);
        await Assert.That(reused.All(sender => sender.Tenant is null && sender.Outbox is null && sender.AttemptBuffer is null)).IsTrue();
        foreach (var sender in reused) sender.EndAttempt();
    }

    private static List<object> Stored(SendRoute<int> route) =>
        ((Array)typeof(SendRoute<int>).GetField("_operations", BindingFlags.Instance | BindingFlags.NonPublic)!
            .GetValue(route)!).Cast<object?>().Where(item => item is not null).Cast<object>().ToList();

    private static SendRoute<int> Route(Sink sink, IOrderingKeySelector<int>? key = null, string source = "/tests") =>
        new(sink, "orders", new("test.retention", 1), new() { Source = source }, key, new SizedSerializer(),
            static context => ((ISendOperation)context).DispatchAsync(), new() { OrderedByKey = true });

    private sealed class LargeKey : IOrderingKeySelector<int>
    {
        public WireString? Select(int message, ArrayBufferWriter<byte> buffer)
        {
            buffer.GetSpan(8192).Slice(0, 8192).Clear();
            buffer.Advance(8192);
            return new WireString(buffer.WrittenMemory);
        }
    }

    private sealed class SizedSerializer : IMessageSerializer
    {
        public string ContentType => "application/test";
        public void Serialize<TMessage>(TMessage message, IBufferWriter<byte> destination)
        {
            var size = (int)(object)message!;
            destination.GetSpan(size).Slice(0, size).Clear();
            destination.Advance(size);
        }
        public TMessage Deserialize<TMessage>(ReadOnlySequence<byte> source) => throw new NotSupportedException();
    }

    private sealed class Sink : ITransport
    {
        public TaskCompletionSource Completion = new();
        public readonly List<OutgoingMessage> Messages = [];
        public string Name => "sink";
        public TransportCapabilities GetCapabilities(string endpoint) => new();
        public ValueTask SendAsync(OutgoingMessage message, CancellationToken cancellationToken)
        {
            Messages.Add(message);
            return new(Completion.Task);
        }
        public ValueTask SendBatchAsync(ReadOnlyMemory<OutgoingMessage> messages, CancellationToken cancellationToken) => throw new NotSupportedException();
        public ValueTask<TopologyReport> ApplyTopologyAsync(TopologyMode mode, CancellationToken cancellationToken) => ValueTask.FromResult(new TopologyReport([]));
        public Task StartAsync(CancellationToken cancellationToken) => Task.CompletedTask;
        public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }
}
