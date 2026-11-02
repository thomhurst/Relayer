using System.Diagnostics.Metrics;
using System.Threading.Tasks.Sources;
using Dekaf.Producer;
using Dekaf;
using Dekaf.Serialization;
using Microsoft.Extensions.Options;
using Relayer.Tests;

namespace Relayer.Kafka.Tests;

public class KafkaSendDurationTests
{
    [Test]
    public async Task Acknowledged_singles_batches_mixed_destinations_and_tombstones_record_native_durations()
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(2));
        var token = timeout.Token;
        using var instrumentation = new RelayerInstrumentation();
        using var capture = new SendDurationCapture(instrumentation);
        using var tracing = new MessageTracing(Options.Create(new TracingOptions()), [], instrumentation: instrumentation);
        await using var fixture = (KafkaFixture)await KafkaFixture.CreateAsync(tracing, token);
        await fixture.Transport.StartAsync(token);
        var first = new OutgoingMessage(fixture.Destination, new byte[] { 1 });
        var other = new OutgoingMessage(fixture.Destination + ".dead-letter", new byte[] { 2 });
        await fixture.Transport.SendAsync(first, token);
        await fixture.Transport.SendBatchAsync(new[] { first, first }, token);
        await fixture.Transport.SendBatchAsync(new[] { first, other }, token);
        await fixture.Transport.SendBatchAsync(new[] { new OutgoingMessage(fixture.Destination, default,
            orderingKey: new WireString("deleted"), isTombstone: true), first }, token);
        await fixture.Transport.SendBatchAsync(Array.Empty<OutgoingMessage>(), token);
        await Assert.That(capture.Values.Count).IsEqualTo(6);
        await Assert.That(capture.Values.Count(value => Equals(value.Tags["messaging.destination.name"], fixture.Destination))).IsEqualTo(5);
        await Assert.That(capture.Values.All(value => value.Seconds > 0 &&
            Equals(value.Tags["messaging.system"], "kafka") && Equals(value.Tags["messaging.operation.name"], "send") &&
            !value.Tags.ContainsKey("error.type"))).IsTrue();
    }

    [Test]
    [Arguments("single")]
    [Arguments("batch")]
    [Arguments("mixed")]
    [Arguments("tombstone")]
    [Arguments("tombstone-batch")]
    public async Task Native_single_and_batch_failures_record_bounded_error(string mode)
    {
        using var instrumentation = new RelayerInstrumentation();
        using var capture = new SendDurationCapture(instrumentation);
        using var tracing = new MessageTracing(Options.Create(new TracingOptions()), [], instrumentation: instrumentation);
        var producer = new ProducerBuilder<WireString?, (ReadOnlyMemory<byte> Body, Headers Headers)?>()
            .WithBootstrapServers("localhost:1").WithKeySerializer(new KafkaKeySerializer()).WithValueSerializer(new KafkaValueSerializer()).Build();
        await using var transport = new KafkaProducerTransport("test", producer, ["orders", "other"], tracing);
        await producer.DisposeAsync();
        var message = new OutgoingMessage("orders", new byte[] { 1 }, orderingKey: new WireString("key"), isTombstone: mode.StartsWith("tombstone", StringComparison.Ordinal));
        await Assert.That(async () =>
        {
            if (mode == "mixed") await transport.SendBatchAsync(new[] { message, new OutgoingMessage("other", default) }, default);
            else if (mode is "batch" or "tombstone-batch") await transport.SendBatchAsync(new[] { message, message }, default);
            else await transport.SendAsync(message, default);
        }).Throws<ObjectDisposedException>();
        var duration = capture.Values.Single();
        await Assert.That(duration.Seconds > 0).IsTrue();
        await Assert.That(duration.Tags["messaging.system"]).IsEqualTo("kafka");
        await Assert.That(duration.Tags["messaging.operation.name"]).IsEqualTo("send");
        await Assert.That(duration.Tags["messaging.destination.name"]).IsEqualTo("orders");
        await Assert.That(duration.Tags["error.type"]).IsEqualTo("_OTHER");
    }
    [Test]
    public async Task Concurrent_measured_native_completions_allocate_zero_after_warmup()
    {
        using var instrumentation = new RelayerInstrumentation();
        using var tracing = new MessageTracing(Options.Create(new TracingOptions()), [], instrumentation: instrumentation);
        using var listener = new MeterListener
        {
            InstrumentPublished = (instrument, observer) =>
            {
                if (ReferenceEquals(instrument.Meter.Scope, instrumentation)) observer.EnableMeasurementEvents(instrument);
            },
        };
        listener.SetMeasurementEventCallback<double>(static (_, _, _, _) => { });
        listener.Start();
        var send = new SendTracing(tracing, new() { System = "kafka", Destination = "orders" }, static (_, _) => default, static (_, _) => default);
        var states = Enumerable.Range(0, 128).Select(_ => new KafkaSendState()).ToArray();
        var native = Enumerable.Range(0, 128).Select(_ => new NativeCompletion()).ToArray();
        var pending = new ValueTask<RecordMetadata>[states.Length];
        void Batch()
        {
            for (var i = 0; i < states.Length; i++) pending[i] = states[i].TrackSend(native[i].Next(), send.StartNativeSend());
            for (var i = 0; i < states.Length; i++) native[i].Complete();
            for (var i = 0; i < states.Length; i++) pending[i].GetAwaiter().GetResult();
        }
        for (var i = 0; i < 100; i++) Batch();
        var before = GC.GetAllocatedBytesForCurrentThread();
        for (var i = 0; i < 100; i++) Batch();
        var allocated = GC.GetAllocatedBytesForCurrentThread() - before;
        await Assert.That(allocated).IsEqualTo(0L);
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task Measured_completion_preserves_pending_and_already_completed_native_failures(bool completed)
    {
        using var instrumentation = new RelayerInstrumentation();
        using var capture = new SendDurationCapture(instrumentation);
        using var tracing = new MessageTracing(Options.Create(new TracingOptions()), [], instrumentation: instrumentation);
        var send = new SendTracing(tracing, new() { System = "kafka", Destination = "orders" }, static (_, _) => default, static (_, _) => default);
        var native = new TaskCompletionSource<RecordMetadata>(TaskCreationOptions.RunContinuationsAsynchronously);
        var failure = new InvalidOperationException("native failure");
        if (completed) native.SetException(failure);
        var pending = new KafkaSendState().TrackSend(new(native.Task), send.StartNativeSend());
        if (!completed)
        {
            await Assert.That(pending.IsCompleted).IsFalse();
            await Assert.That(capture.Values.Count).IsEqualTo(0);
            native.SetException(failure);
        }
        await Assert.That(async () => await pending).Throws<InvalidOperationException>();
        await Assert.That(capture.Values.Single().Tags["error.type"]).IsEqualTo("_OTHER");
    }

    [Test]
    public async Task Awaiting_batch_can_reuse_completion_state_from_an_inline_continuation()
    {
        using var instrumentation = new RelayerInstrumentation();
        using var capture = new SendDurationCapture(instrumentation);
        using var tracing = new MessageTracing(Options.Create(new TracingOptions()), [], instrumentation: instrumentation);
        var send = new SendTracing(tracing, new() { System = "kafka", Destination = "orders" }, static (_, _) => default, static (_, _) => default);
        var native = new NativeCompletion();
        var state = new KafkaSendState();
        var pending = state.TrackSend(native.Next(), send.StartNativeSend());
        async Task ReuseAsync()
        {
            await pending.ConfigureAwait(false);
            await state.TrackSend(new ValueTask<RecordMetadata>(default(RecordMetadata)), send.StartNativeSend()).ConfigureAwait(false);
        }
        var reuse = ReuseAsync();
        await Assert.That(reuse.IsCompleted).IsFalse();
        native.Complete();
        await reuse;
        await Assert.That(capture.Values.Count).IsEqualTo(2);
    }

    private sealed class NativeCompletion : IValueTaskSource<RecordMetadata>
    {
        private ManualResetValueTaskSourceCore<RecordMetadata> _source;
        internal ValueTask<RecordMetadata> Next() { _source.Reset(); return new(this, _source.Version); }
        internal void Complete() => _source.SetResult(default);
        RecordMetadata IValueTaskSource<RecordMetadata>.GetResult(short token) => _source.GetResult(token);
        ValueTaskSourceStatus IValueTaskSource<RecordMetadata>.GetStatus(short token) => _source.GetStatus(token);
        void IValueTaskSource<RecordMetadata>.OnCompleted(Action<object?> continuation, object? state, short token, ValueTaskSourceOnCompletedFlags flags) =>
            _source.OnCompleted(continuation, state, token, flags);
    }

}
