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
    public async Task Outage_requires_recovery_marker_after_all_acknowledged_sends_arrive()
    {
        var suite = Suite("outage-missing-marker");
        var error = await Assert.That(async () => await suite.RunAsync(TransportConformanceCase.SendDuringOutage))
            .Throws<InvalidOperationException>().WithMessageContaining("outage recovery receive; outstanding values: 4.");
        await Assert.That(error!.InnerException is OperationCanceledException).IsTrue();
    }

    [Test]
    public async Task Outage_waits_for_post_restart_warmup_after_initial_replay()
    {
        var result = await Suite("initial-replay").RunAsync(TransportConformanceCase.SendDuringOutage);
        await Assert.That(result.Skipped).IsFalse();
    }

    [Test]
    public async Task Outage_initial_replay_cannot_satisfy_missing_post_restart_warmup()
    {
        var suite = Suite("initial-replay-missing-warmup");
        var error = await Assert.That(async () => await suite.RunAsync(TransportConformanceCase.SendDuringOutage))
            .Throws<InvalidOperationException>().WithMessageContaining("outage warm-up receive");
        await Assert.That(error!.InnerException is OperationCanceledException).IsTrue();
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
    [Arguments("warm-up")]
    [Arguments("recovery")]
    public async Task Reconnect_accepts_initial_replay_before_expected_marker(string replayBefore)
    {
        var result = await Suite("reconnect", replayBefore).RunAsync(TransportConformanceCase.Reconnect);
        await Assert.That(result.Skipped).IsFalse();
    }

    [Test]
    [Arguments("warm-up")]
    [Arguments("recovery")]
    public async Task Reconnect_requires_expected_marker_after_initial_replay(string replayBefore)
    {
        var suite = Suite("reconnect", replayBefore, omitMarker: true);
        var error = await Assert.That(async () => await suite.RunAsync(TransportConformanceCase.Reconnect))
            .Throws<InvalidOperationException>().WithMessageContaining($"reconnect {replayBefore} receive");
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
    [Arguments(0L)]
    [Arguments(-TimeSpan.TicksPerMillisecond)]
    [Arguments(1L)]
    [Arguments(TimeSpan.TicksPerMillisecond - 1L)]
    public async Task Outage_rejects_timeout_below_one_millisecond_before_lifecycle_calls(long ticks)
    {
        var calls = new List<string>();
        var suite = new TransportConformanceSuite<InMemoryDelivery, InMemoryMessageBinding>(
            (tracing, _) => new ValueTask<ITransportConformanceFixture<InMemoryDelivery>>(
                new OutageFixture(tracing, "success", calls: calls) { OutageSendTimeout = TimeSpan.FromTicks(ticks) }));

        var failure = await Assert.That(async () => await suite.RunAsync(TransportConformanceCase.SendDuringOutage))
            .Throws<InvalidOperationException>();
        await Assert.That(calls.ToArray()).IsEquivalentTo([nameof(IAsyncDisposable.DisposeAsync)]);
        await Assert.That(failure!.Message).Contains(nameof(ITransportConformanceFixture<InMemoryDelivery>.OutageSendTimeout));
    }

    [Test]
    public async Task Fixture_without_real_fault_injection_explicitly_skips_outage()
    {
        var calls = new List<string>();
        var suite = new TransportConformanceSuite<InMemoryDelivery, InMemoryMessageBinding>(
            (tracing, _) => new ValueTask<ITransportConformanceFixture<InMemoryDelivery>>(
                new OutageFixture(tracing, "success", calls: calls) { OutageSendTimeout = null }));
        var result = await suite.RunAsync(TransportConformanceCase.SendDuringOutage);
        await Assert.That(result.SkipReason).IsEqualTo("Fixture has no bounded broker-outage send contract.");
        await Assert.That(calls.ToArray()).IsEquivalentTo([nameof(IAsyncDisposable.DisposeAsync)]);
    }

    [Test]
    [Arguments(4_294_962_295L)]
    [Arguments(4_294_967_295L)]
    public async Task Outage_rejects_oversized_timeout_before_lifecycle_calls(long milliseconds)
    {
        var calls = new List<string>();
        var suite = new TransportConformanceSuite<InMemoryDelivery, InMemoryMessageBinding>(
            (tracing, _) => new ValueTask<ITransportConformanceFixture<InMemoryDelivery>>(
                new OutageFixture(tracing, "success", calls: calls) { OutageSendTimeout = TimeSpan.FromMilliseconds(milliseconds) }));

        var failure = await Assert.That(async () => await suite.RunAsync(TransportConformanceCase.SendDuringOutage))
            .Throws<InvalidOperationException>();

        await Assert.That(calls.ToArray()).IsEquivalentTo([nameof(IAsyncDisposable.DisposeAsync)]);
        await Assert.That(failure!.Message).Contains("OutageSendTimeout must be less than 4,294,962,295 milliseconds");
    }

    [Test]
    [Arguments(TransportConformanceCase.Reconnect, "broker disconnect")]
    [Arguments(TransportConformanceCase.Reconnect, "broker reconnect")]
    [Arguments(TransportConformanceCase.SendDuringOutage, "broker disconnect")]
    [Arguments(TransportConformanceCase.SendDuringOutage, "broker reconnect")]
    [Arguments(TransportConformanceCase.SendDuringOutage, "offline shutdown")]
    public async Task Cancellation_reports_outage_phase_and_cleans_up_before_late_fault(TransportConformanceCase scenario, string phase)
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

            hook.Completion.SetException(new IOException($"Late {phase} failure."));
        }
        finally
        {
            hook.Completion.TrySetResult();

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
    [Arguments(1L)]
    [Arguments(25L)]
    [Arguments(4_294_962_294L)]
    public async Task Cancelled_outage_wait_cleans_up_before_pending_send_faults(long milliseconds)
    {
        var send = new PendingHook("outage sends");
        using var caller = new CancellationTokenSource();
        var suite = new TransportConformanceSuite<InMemoryDelivery, InMemoryMessageBinding>(
            (tracing, _) => new ValueTask<ITransportConformanceFixture<InMemoryDelivery>>(
                new OutageFixture(tracing, "success", send) { OutageSendTimeout = TimeSpan.FromMilliseconds(milliseconds) }));
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

    private static TransportConformanceSuite<InMemoryDelivery, InMemoryMessageBinding> Suite(string mode,
        string? replayBefore = null, bool omitMarker = false)
    {
        return new((tracing, _) => new ValueTask<ITransportConformanceFixture<InMemoryDelivery>>(
            new OutageFixture(tracing, mode, replayBefore: replayBefore, omitMarker: omitMarker)), TimeSpan.FromSeconds(2));
    }

    // A controlled fixture tests the contract's assertions, not a broker's reconnect implementation.
    private sealed class OutageFixture(MessageTracing tracing, string mode, PendingHook? hook = null, List<string>? calls = null,
        string? replayBefore = null, bool omitMarker = false) : ITransportConformanceFixture<InMemoryDelivery>, ITransport
    {
        private readonly InMemoryFixture _inner = new(tracing);
        private OutgoingMessage _initial;
        private bool _offline;
        private bool _shutdownFailed;
        private bool _recovered;
        private bool _shutdownIncomplete;
        private int _outageSends;
        private int _replaySettlements;
        private readonly TaskCompletionSource _markerReplayed = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private bool _started;
        private bool _restartPending;
        private int _restartWarmupReceived;

        private bool TestReconnect => mode is "reconnect" or "reconnect-missing-marker";

        private bool ReplayInitial => mode is "initial-replay" or "initial-replay-missing-warmup";

        public ITransport Transport => this;

        public string Name => _inner.Transport.Name;

        public string Destination => _inner.Destination;

        public string MessagingSystem => _inner.MessagingSystem;

        public string SettlementOperation => _inner.SettlementOperation;

        public HeaderNames HeaderNames => _inner.HeaderNames;

        // Put reconnect replays and their markers on the same ordered partition.
        public WireString? DefaultOrderingKey => replayBefore is null ? null : new WireString("reconnect");

        public TimeSpan? OutageSendTimeout
        {
            get; init;
        } = TimeSpan.FromMilliseconds(25);

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
            calls?.Add(nameof(RegisterConsumer));

            _inner.RegisterConsumer((incoming, token) =>
            {
                var body = incoming.Message.Body.Span;

                if (TestReconnect && body.SequenceEqual("{\"Value\":2}"u8) ||
                    ReplayInitial && body.SequenceEqual("{\"Value\":5}"u8))
                {
                    Volatile.Write(ref _restartWarmupReceived, 1);
                }

                return receive(incoming, token);
            });
        }

        public ValueTask SettleAsync(InMemoryDelivery incoming, Disposition disposition, CancellationToken token)
        {
            var body = incoming.Message.Body.Span;

            if (TestReconnect && body.SequenceEqual("{\"Value\":2}"u8) ||
                mode == "warmup-redelivery" && body.SequenceEqual("{\"Value\":5}"u8) ||
                ReplayInitial && body.SequenceEqual("{\"Value\":0}"u8))
            {
                return SettleWithReplay(incoming, disposition, token);
            }

            return _inner.SettleAsync(incoming, disposition, token);
        }

        private ValueTask SettleWithReplay(InMemoryDelivery incoming, Disposition disposition, CancellationToken token)
        {
            var settlement = Interlocked.Increment(ref _replaySettlements);

            if (settlement == 1)
            {
                incoming.Redeliver(TimeSpan.Zero);
                return default;
            }

            if (settlement == 2)
            {
                _markerReplayed.TrySetResult();
            }

            return _inner.SettleAsync(incoming, disposition, token);
        }

        public ValueTask<ConformanceDeadLetter> ReadDeadLetterAsync(CancellationToken token)
        {
            return _inner.ReadDeadLetterAsync(token);
        }

        public Task StartAsync(CancellationToken token)
        {
            calls?.Add(nameof(StartAsync));

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
            calls?.Add(nameof(StopAsync));

            if (_offline && hook is { Phase: "offline shutdown" } && !hook.Entered.Task.IsCompleted)
            {
                await hook.WaitAsync();
                return;
            }

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
            calls?.Add(nameof(DisconnectAsync));

            if ((TestReconnect || ReplayInitial) && Volatile.Read(ref _restartWarmupReceived) == 0)
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
            calls?.Add(nameof(ReconnectAsync));

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
            calls?.Add(nameof(SendAsync));

            if (_restartPending)
            {
                await _inner.Transport.StartAsync(token);
                _restartPending = false;
            }

            if (ReplayInitial && !_offline && !_recovered)
            {
                if (!_markerReplayed.Task.IsCompleted)
                {
                    // Queue the initial delivery and its replay before the suite can stop the receiver.
                    await _inner.Transport.SendAsync(message, token);
                    await _markerReplayed.Task.WaitAsync(token);
                    return;
                }

                if (mode == "initial-replay-missing-warmup")
                {
                    return;
                }
            }

            if (mode == "outage-missing-marker" && _recovered && message.Body.Span.SequenceEqual("{\"Value\":4}"u8))
            {
                return;
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
                await _markerReplayed.Task.WaitAsync(token);

                if (mode == "reconnect-missing-marker")
                {
                    return;
                }
            }

            if (replayBefore is not null)
            {
                if (message.Body.Span.SequenceEqual("{\"Value\":1}"u8))
                {
                    if (replayBefore == "warm-up")
                    {
                        // Enqueue a second delivery on the same partition before the first stop.
                        await _inner.Transport.SendAsync(message, token);
                    }
                    else
                    {
                        // Keep an owned copy for the replay after reconnect.
                        _initial = new(message.Destination, message.Body.ToArray(), message.Headers.ToArray(), message.OrderingKey);
                    }
                }
                else if (replayBefore == (_recovered ? "recovery" : "warm-up"))
                {
                    if (_recovered)
                    {
                        await _inner.Transport.SendAsync(_initial, token);
                    }

                    if (omitMarker)
                    {
                        return;
                    }
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
            calls?.Add(nameof(DisposeAsync));

            if (hook is { IsPending: true })
            {
                hook.DisposedWhilePending = true;
            }

            return _inner.DisposeAsync();
        }
    }
}
