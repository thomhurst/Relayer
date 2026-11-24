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
    public async Task Reconnect_waits_for_the_restarted_receiver_and_accepts_warmup_redelivery()
    {
        var result = await Suite("reconnect").RunAsync(TransportConformanceCase.Reconnect);
        await Assert.That(result.Skipped).IsFalse();
    }

    [Test]
    public async Task Reconnect_requires_a_recovery_marker_even_after_warmup_redelivery()
    {
        var error = await Assert.That(async () => await Suite("reconnect-missing-marker").RunAsync(TransportConformanceCase.Reconnect))
            .Throws<InvalidOperationException>().WithMessageContaining("reconnect recovery receive");
        await Assert.That(error!.InnerException is OperationCanceledException).IsTrue();
    }

    [Test]
    [Arguments("unexpected", "outage sends")]
    [Arguments("lost", "outage recovery receive; outstanding values: 1, 2, 3")]
    public async Task Outage_rejects_unexpected_failures_and_lost_acknowledged_messages(string mode, string phase)
    {
        await Assert.That(async () => await Suite(mode).RunAsync(TransportConformanceCase.SendDuringOutage))
            .Throws<InvalidOperationException>().WithMessageContaining(phase);
    }

    [Test]
    public async Task Outage_reconnects_after_recognized_native_shutdown_failure()
    {
        await Assert.That((await Suite("shutdown-failure").RunAsync(TransportConformanceCase.SendDuringOutage)).Skipped).IsFalse();
    }

    [Test]
    public async Task Outage_finishes_interrupted_shutdown_after_reconnect_before_restart()
    {
        await Assert.That((await Suite("interrupted-shutdown").RunAsync(TransportConformanceCase.SendDuringOutage)).Skipped).IsFalse();
    }

    [Test]
    public async Task Outage_rejects_unrecognized_shutdown_failure()
    {
        await Assert.That(async () => await Suite("unexpected-shutdown").RunAsync(TransportConformanceCase.SendDuringOutage))
            .Throws<AggregateException>().WithMessageContaining("offline shutdown");
    }

    [Test]
    public async Task Fixture_without_real_fault_injection_explicitly_skips_outage()
    {
        var suite = new TransportConformanceSuite<InMemoryDelivery, InMemoryMessageBinding>(
            (tracing, _) => new ValueTask<ITransportConformanceFixture<InMemoryDelivery>>(new InMemoryFixture(tracing)));
        var result = await suite.RunAsync(TransportConformanceCase.SendDuringOutage);
        await Assert.That(result.SkipReason).IsEqualTo("Fixture has no bounded broker-outage send contract.");
    }

    [Test]
    [Arguments(TransportConformanceCase.Reconnect, "broker disconnect")]
    [Arguments(TransportConformanceCase.Reconnect, "broker reconnect")]
    [Arguments(TransportConformanceCase.SendDuringOutage, "broker disconnect")]
    [Arguments(TransportConformanceCase.SendDuringOutage, "broker reconnect")]
    public async Task Cancellation_abandons_pending_broker_hook_and_attempts_cleanup(TransportConformanceCase scenario, string phase)
    {
        var hook = new PendingHook(phase);
        using var caller = new CancellationTokenSource();
        var suite = new TransportConformanceSuite<InMemoryDelivery, InMemoryMessageBinding>(
            (tracing, _) => new ValueTask<ITransportConformanceFixture<InMemoryDelivery>>(new OutageFixture(tracing, "success", hook)));
        var run = suite.RunAsync(scenario, caller.Token);

        try
        {
            await hook.Entered.Task.WaitAsync(TimeSpan.FromSeconds(5));
            caller.Cancel();

            var failure = await Assert.That(async () => await run.WaitAsync(TimeSpan.FromSeconds(5)))
                .Throws<InvalidOperationException>().WithMessageContaining(phase);
            await Assert.That(failure!.InnerException is OperationCanceledException).IsTrue();
            await Assert.That(hook.StoppedWhilePending).IsTrue();
            await Assert.That(hook.DisposedWhilePending).IsTrue();
        }
        finally
        {
            hook.Completion.TrySetCanceled();

            try
            {
                await run.WaitAsync(TimeSpan.FromSeconds(5));
            }
            catch (InvalidOperationException)
            {
            }
        }
    }

    [Test]
    public async Task Cancelled_outage_wait_cleans_up_before_pending_send_faults()
    {
        var send = new PendingHook("outage sends");
        using var caller = new CancellationTokenSource();
        var suite = new TransportConformanceSuite<InMemoryDelivery, InMemoryMessageBinding>(
            (tracing, _) => new ValueTask<ITransportConformanceFixture<InMemoryDelivery>>(new OutageFixture(tracing, "success", send)));
        var run = suite.RunAsync(TransportConformanceCase.SendDuringOutage, caller.Token);

        try
        {
            await send.Entered.Task.WaitAsync(TimeSpan.FromSeconds(5));
            var body = send.Message.Body.ToArray();
            var headers = send.Message.Headers.ToArray();
            caller.Cancel();

            var failure = await Assert.That(async () => await run.WaitAsync(TimeSpan.FromSeconds(5)))
                .Throws<InvalidOperationException>().WithMessageContaining("outage sends");
            await Assert.That(failure!.InnerException is OperationCanceledException).IsTrue();
            await Assert.That(send.StoppedWhilePending).IsTrue();
            await Assert.That(send.DisposedWhilePending).IsTrue();
            await Assert.That(send.IsPending).IsTrue();
            await Assert.That(send.Message.Body.Span.SequenceEqual(body)).IsTrue();
            await Assert.That(send.Message.Headers.Span.SequenceEqual(headers)).IsTrue();

            send.Completion.SetException(new InvalidOperationException("Unexpected late native send failure."));
        }
        finally
        {
            send.Completion.TrySetResult();
        }
    }

    private sealed class PendingHook(string phase)
    {
        public string Phase
        {
            get;
        } = phase;

        public TaskCompletionSource Entered
        {
            get;
        } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public TaskCompletionSource Completion
        {
            get;
        } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public bool IsPending => Entered.Task.IsCompleted && !Completion.Task.IsCompleted;

        public OutgoingMessage Message
        {
            get; private set;
        }

        public bool StoppedWhilePending
        {
            get; set;
        }

        public bool DisposedWhilePending
        {
            get; set;
        }

        public Task WaitAsync(OutgoingMessage message = default)
        {
            Message = message;
            Entered.SetResult();
            return Completion.Task;
        }
    }

    private static TransportConformanceSuite<InMemoryDelivery, InMemoryMessageBinding> Suite(string mode)
    {
        return new((tracing, _) => new ValueTask<ITransportConformanceFixture<InMemoryDelivery>>(new OutageFixture(tracing, mode)),
                    TimeSpan.FromSeconds(2));
    }

    // A controlled fixture tests the contract's assertions, not a broker's reconnect implementation.
    private sealed class OutageFixture(MessageTracing tracing, string mode, PendingHook? hook = null) : ITransportConformanceFixture<InMemoryDelivery>, ITransport
    {
        private readonly InMemoryFixture _inner = new(tracing);
        private bool _offline;
        private bool _shutdownFailed;
        private bool _recovered;
        private bool _shutdownIncomplete;
        private int _outageSends;
        private int _warmupSettlements;
        private readonly TaskCompletionSource _warmupRedelivered = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private bool _started;
        private bool _restartPending;
        private int _reconnectWarmupReceived;

        private bool TestReconnect => mode is "reconnect" or "reconnect-missing-marker";

        public ITransport Transport => this;

        public string Name => _inner.Transport.Name;

        public string Destination => _inner.Destination;

        public string MessagingSystem => _inner.MessagingSystem;

        public string SettlementOperation => _inner.SettlementOperation;

        public HeaderNames HeaderNames => _inner.HeaderNames;

        public TimeSpan? OutageSendTimeout => TimeSpan.FromMilliseconds(25);

        public bool IsExpectedOutageFailure(Exception error)
        {
            return error is IOException;
        }

        public TransportCapabilities GetCapabilities(string endpoint)
        {
            return _inner.Transport.GetCapabilities(endpoint);
        }

        public void RegisterConsumer(Func<InMemoryDelivery, CancellationToken, ValueTask<Disposition>> receive)
        {
            _inner.RegisterConsumer((incoming, token) =>
            {
                if (TestReconnect && incoming.Message.Body.Span.SequenceEqual("{\"Value\":2}"u8))
                {
                    Volatile.Write(ref _reconnectWarmupReceived, 1);
                }

                return receive(incoming, token);
            });
        }

        public ValueTask SettleAsync(InMemoryDelivery incoming, Disposition disposition, CancellationToken token)
        {
            // Requeue exactly the post-restart warm-up. Handler observation precedes
            // settlement, so its next delivery may already be queued when outage recovery starts.
            if (TestReconnect && incoming.Message.Body.Span.SequenceEqual("{\"Value\":2}"u8))
            {
                return SettleWarmup(incoming, disposition, token, replayOnSettlement: 1);
            }

            if (mode == "warmup-redelivery" && incoming.Message.Body.Span.SequenceEqual("{\"Value\":0}"u8))
            {
                return SettleWarmup(incoming, disposition, token, replayOnSettlement: 2);
            }

            return _inner.SettleAsync(incoming, disposition, token);
        }

        private ValueTask SettleWarmup(InMemoryDelivery incoming, Disposition disposition, CancellationToken token, int replayOnSettlement)
        {
            var settlement = Interlocked.Increment(ref _warmupSettlements);

            if (settlement == replayOnSettlement)
            {
                incoming.Redeliver(TimeSpan.Zero);
                return default;
            }

            if (settlement == replayOnSettlement + 1)
            {
                _warmupRedelivered.TrySetResult();
            }

            return _inner.SettleAsync(incoming, disposition, token);
        }

        public ValueTask<ConformanceDeadLetter> ReadDeadLetterAsync(CancellationToken token)
        {
            return _inner.ReadDeadLetterAsync(token);
        }

        public Task StartAsync(CancellationToken token)
        {
            if (_shutdownIncomplete)
            {
                throw new InvalidOperationException("Interrupted shutdown must finish before restart.");
            }

            if (_shutdownFailed && !_recovered)
            {
                throw new InvalidOperationException("Faulted application was not recovered.");
            }

            if (TestReconnect && _started)
            {
                // Model StartAsync returning before the restarted receiver connects.
                _restartPending = true;
                return Task.CompletedTask;
            }

            _started = true;
            return _inner.Transport.StartAsync(token);
        }

        public async Task StopAsync(CancellationToken token)
        {
            if (hook is { IsPending: true })
            {
                hook.StoppedWhilePending = true;
            }

            if (_offline && mode == "interrupted-shutdown")
            {
                _shutdownIncomplete = true;
                // Model a disconnected native drain that outlives the caller's stop budget.
                throw new IOException("Disconnected drain is still pending.");
            }

            await _inner.Transport.StopAsync(token);
            _shutdownIncomplete = false;

            if (_offline && mode == "unexpected-shutdown")
            {
                throw new InvalidOperationException("Unexpected shutdown defect.");
            }

            if (_offline && mode == "shutdown-failure")
            {
                _shutdownFailed = true;
                throw new IOException("Expected disconnected native shutdown.");
            }
        }

        public async Task DisconnectAsync(CancellationToken token)
        {
            if (TestReconnect && Volatile.Read(ref _reconnectWarmupReceived) == 0)
            {
                throw new InvalidOperationException("Disconnected before the restarted receiver received the warm-up.");
            }

            if (hook is { Phase: "broker disconnect" })
            {
                await hook.WaitAsync();
            }

            await _inner.DisconnectAsync(token);
            _offline = true;
        }

        public Task ReconnectAsync(CancellationToken token)
        {
            if (hook is { Phase: "broker reconnect" })
            {
                return hook.WaitAsync();
            }

            token.ThrowIfCancellationRequested();
            _offline = false;
            _recovered = true;
            return TestReconnect ? _inner.ReconnectAsync(token) : Task.CompletedTask;
        }

        public async ValueTask SendAsync(OutgoingMessage message, CancellationToken token)
        {
            if (_restartPending)
            {
                await _inner.Transport.StartAsync(token);
                _restartPending = false;
            }

            if (_offline)
            {
                var attempt = Interlocked.Increment(ref _outageSends);

                if (hook is { Phase: "outage sends" } && attempt == 1)
                {
                    await hook.WaitAsync(message);
                    return;
                }

                if (mode == "unexpected")
                {
                    throw new InvalidOperationException("Unexpected native bug.");
                }

                if (mode == "lost")
                {
                    return;
                }

                if (mode == "cancel")
                {
                    if (attempt == 1)
                    {
                        await Task.Delay(Timeout.InfiniteTimeSpan, token);
                        return;
                    }

                    if (attempt == 2)
                    {
                        throw new IOException("Expected disconnected native operation.");
                    }
                    // Cancellation does not prove a message was not accepted by the broker.
                    await _inner.Transport.SendAsync(message, token);
                    await Task.Delay(Timeout.InfiniteTimeSpan, token);
                    return;
                }
            }
            // Queue the redelivered warm-up before the recovery marker, regardless
            // of the in-memory endpoint's partition scheduling.
            if (TestReconnect && _recovered || mode == "warmup-redelivery" && !_offline && Volatile.Read(ref _outageSends) != 0)
            {
                await _warmupRedelivered.Task.WaitAsync(token);

                if (mode == "reconnect-missing-marker")
                {
                    return;
                }
            }

            await _inner.Transport.SendAsync(message, token);
        }

        public ValueTask SendBatchAsync(ReadOnlyMemory<OutgoingMessage> messages, CancellationToken token)
        {
            return _inner.Transport.SendBatchAsync(messages, token);
        }

        public ValueTask<TopologyReport> ApplyTopologyAsync(TopologyMode mode, CancellationToken token)
        {
            return _inner.Transport.ApplyTopologyAsync(mode, token);
        }

        public ValueTask DisposeAsync()
        {
            if (hook is { IsPending: true })
            {
                hook.DisposedWhilePending = true;
            }

            return _inner.DisposeAsync();
        }
    }
}
