using Relayer.InMemory;
using Relayer.Testing;
using Relayer.Transports;

namespace Relayer.Conformance.Tests;

public sealed class OutageConformanceTests
{
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task Outage_accepts_acknowledgements_and_ambiguous_cancelled_delivery(bool cancel)
    {
        var suite = Suite(cancel ? "cancel" : "success");
        await Assert.That((await suite.RunAsync(TransportConformanceCase.SendDuringOutage)).Skipped).IsFalse();
    }

    [Test]
    public async Task Outage_accepts_warmup_redelivery_before_recovery_marker()
    {
        await Assert.That((await Suite("warmup-redelivery").RunAsync(TransportConformanceCase.SendDuringOutage)).Skipped).IsFalse();
    }

    [Test]
    [Arguments("unexpected", "outage sends")]
    [Arguments("lost", "receive completion")]
    public async Task Outage_rejects_unexpected_failures_and_lost_acknowledged_messages(string mode, string phase)
    {
        await Assert.That(async () => await Suite(mode).RunAsync(TransportConformanceCase.SendDuringOutage))
            .Throws<InvalidOperationException>().WithMessageContaining(phase);
    }

    [Test]
    public async Task Fixture_without_real_fault_injection_explicitly_skips_outage()
    {
        var suite = new TransportConformanceSuite<InMemoryDelivery, InMemoryMessageBinding>(
            (tracing, _) => new ValueTask<ITransportConformanceFixture<InMemoryDelivery>>(new InMemoryFixture(tracing)));
        var result = await suite.RunAsync(TransportConformanceCase.SendDuringOutage);
        await Assert.That(result.SkipReason).IsEqualTo("Fixture has no bounded broker-outage send contract.");
    }

    private static TransportConformanceSuite<InMemoryDelivery, InMemoryMessageBinding> Suite(string mode) =>
        new((tracing, _) => new ValueTask<ITransportConformanceFixture<InMemoryDelivery>>(new OutageFixture(tracing, mode)),
            TimeSpan.FromSeconds(2));

    // A controlled fixture tests the contract's assertions, not a broker's reconnect implementation.
    private sealed class OutageFixture(MessageTracing tracing, string mode) : ITransportConformanceFixture<InMemoryDelivery>, ITransport
    {
        private readonly InMemoryFixture _inner = new(tracing);
        private bool _offline;
        private int _outageSends;
        private int _warmupSettlements;
        private readonly TaskCompletionSource _warmupRedelivered = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public ITransport Transport => this;
        public string Name => _inner.Transport.Name;
        public string Destination => _inner.Destination;
        public string MessagingSystem => _inner.MessagingSystem;
        public string SettlementOperation => _inner.SettlementOperation;
        public HeaderNames HeaderNames => _inner.HeaderNames;
        public TimeSpan? OutageSendTimeout => TimeSpan.FromMilliseconds(25);
        public bool IsExpectedOutageFailure(Exception error) => error is IOException;
        public TransportCapabilities GetCapabilities(string endpoint) => _inner.Transport.GetCapabilities(endpoint);
        public void RegisterConsumer(Func<InMemoryDelivery, CancellationToken, ValueTask<Disposition>> receive) => _inner.RegisterConsumer(receive);
        public ValueTask SettleAsync(InMemoryDelivery incoming, Disposition disposition, CancellationToken token)
        {
            // Requeue exactly the post-restart warm-up. Handler observation precedes
            // settlement, so its next delivery may already be queued when outage recovery starts.
            if (mode == "warmup-redelivery" && incoming.Message.Body.Span.SequenceEqual("{\"Value\":0}"u8))
            {
                var settlement = Interlocked.Increment(ref _warmupSettlements);
                if (settlement == 2)
                {
                    incoming.Redeliver(TimeSpan.Zero);
                    return default;
                }
                if (settlement == 3) _warmupRedelivered.TrySetResult();
            }
            return _inner.SettleAsync(incoming, disposition, token);
        }
        public ValueTask<ConformanceDeadLetter> ReadDeadLetterAsync(CancellationToken token) => _inner.ReadDeadLetterAsync(token);
        public Task StartAsync(CancellationToken token) => _inner.Transport.StartAsync(token);
        public Task StopAsync(CancellationToken token) => _inner.Transport.StopAsync(token);
        public async Task DisconnectAsync(CancellationToken token) { await _inner.DisconnectAsync(token); _offline = true; }
        public Task ReconnectAsync(CancellationToken token) { token.ThrowIfCancellationRequested(); _offline = false; return Task.CompletedTask; }
        public async ValueTask SendAsync(OutgoingMessage message, CancellationToken token)
        {
            if (_offline)
            {
                var attempt = Interlocked.Increment(ref _outageSends);
                if (mode == "unexpected") throw new InvalidOperationException("Unexpected native bug.");
                if (mode == "lost") return;
                if (mode == "cancel")
                {
                    if (attempt == 1) { await Task.Delay(Timeout.InfiniteTimeSpan, token); return; }
                    if (attempt == 2) throw new IOException("Expected disconnected native operation.");
                    // Cancellation does not prove a message was not accepted by the broker.
                    await _inner.Transport.SendAsync(message, token);
                    await Task.Delay(Timeout.InfiniteTimeSpan, token);
                    return;
                }
            }
            // Queue the redelivered warm-up before the recovery marker, regardless
            // of the in-memory endpoint's partition scheduling.
            if (mode == "warmup-redelivery" && !_offline && Volatile.Read(ref _outageSends) != 0)
                await _warmupRedelivered.Task.WaitAsync(token);
            await _inner.Transport.SendAsync(message, token);
        }
        public ValueTask SendBatchAsync(ReadOnlyMemory<OutgoingMessage> messages, CancellationToken token) => _inner.Transport.SendBatchAsync(messages, token);
        public ValueTask<TopologyReport> ApplyTopologyAsync(TopologyMode mode, CancellationToken token) => _inner.Transport.ApplyTopologyAsync(mode, token);
        public ValueTask DisposeAsync() => _inner.DisposeAsync();
    }
}
