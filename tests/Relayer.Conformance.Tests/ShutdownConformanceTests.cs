using Microsoft.Extensions.Logging;
using Relayer.InMemory;
using Relayer.Testing;
using Relayer.Transports;

namespace Relayer.Conformance.Tests;

public class ShutdownConformanceTests
{
    [Test]
    [Arguments(LogLevel.Error, false)]
    [Arguments(LogLevel.Critical, false)]
    [Arguments(LogLevel.Warning, true)]
    public async Task Capture_rejects_failure_logs(LogLevel level, bool cancellation)
    {
        using var capture = new ConformanceLogCapture();
        capture.CreateLogger("native").Log(level, new EventId(1), "stopping",
            cancellation ? new OperationCanceledException() : null, static (state, _) => state);
        await Assert.That(() => capture.AssertClean()).Throws<InvalidOperationException>();
    }

    [Test]
    public void Capture_is_fixture_local_and_allows_normal_cancellation_information()
    {
        using var first = new ConformanceLogCapture();
        using var second = new ConformanceLogCapture();
        first.CreateLogger("native").LogError("failed");
        second.CreateLogger("native").LogInformation(new OperationCanceledException(), "stopped");
        second.AssertClean();
    }

    [Test]
    [Arguments(true, false, false)]
    [Arguments(false, true, false)]
    [Arguments(false, false, true)]
    public async Task Broken_shutdown_fails(bool earlyStop, bool disposalError, bool nativeViolation)
    {
        var suite = new TransportConformanceSuite<InMemoryDelivery, InMemoryMessageBinding>((tracing, _) =>
            new ValueTask<ITransportConformanceFixture<InMemoryDelivery>>(new BrokenFixture(tracing, earlyStop, disposalError, nativeViolation)));
        await Assert.That(async () => await suite.RunAsync(TransportConformanceCase.CleanShutdown))
            .Throws<InvalidOperationException>();
    }

    [Test]
    public async Task Asynchronous_early_stop_fails_before_native_verification()
    {
        var suite = new TransportConformanceSuite<InMemoryDelivery, InMemoryMessageBinding>((tracing, _) =>
            new ValueTask<ITransportConformanceFixture<InMemoryDelivery>>(new BrokenFixture(tracing, false, false, false, asynchronousEarlyStop: true)));
        var error = await Assert.That(async () => await suite.RunAsync(TransportConformanceCase.CleanShutdown))
            .Throws<InvalidOperationException>();
        await Assert.That(error!.InnerException!.Message).Contains("StopAsync completed before the in-flight consumer finished");
    }

    [Test]
    [Arguments(TransportConformanceCase.SendReceiveSettle, "transport start", 1, 0, 1)]
    [Arguments(TransportConformanceCase.SendReceiveSettle, "transport drain", 0, 1, 2)]
    [Arguments(TransportConformanceCase.SendReceiveSettle, "transport restart", 2, 0, 2)]
    [Arguments(TransportConformanceCase.CleanShutdown, "in-flight transport shutdown", 0, 1, 2)]
    public async Task Cancellation_reports_lifecycle_phase_and_cleans_up_before_late_fault(
        TransportConformanceCase scenario, string phase, int pendingStart, int pendingStop, int stopCalls)
    {
        using var cancellation = new CancellationTokenSource();
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var pending = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var watchdog = TimeSpan.FromSeconds(5);
        BrokenFixture? fixture = null;

        Task? PauseOperation(int call, int pendingCall)
        {
            if (call != pendingCall)
            {
                return null;
            }

            entered.SetResult();
            return pending.Task;
        }

        var suite = new TransportConformanceSuite<InMemoryDelivery, InMemoryMessageBinding>((tracing, _) =>
            new ValueTask<ITransportConformanceFixture<InMemoryDelivery>>(
                fixture = new BrokenFixture(tracing, false, false, false,
                    start: call => PauseOperation(call, pendingStart),
                    stop: call => PauseOperation(call, pendingStop))));
        var run = suite.RunAsync(scenario, cancellation.Token);

        try
        {
            await entered.Task.WaitAsync(watchdog);

            // The handler also uses the scenario token. Drain the real transport before
            // canceling the scenario while its simulated StopAsync is still pending.
            if (scenario == TransportConformanceCase.CleanShutdown)
            {
                await fixture!.DisconnectAsync(CancellationToken.None).WaitAsync(watchdog);
                await fixture.VerifyShutdownAsync(CancellationToken.None);
            }

            cancellation.Cancel();

            var error = await Assert.That(async () => await run.WaitAsync(watchdog))
                .Throws<InvalidOperationException>();
            await Assert.That(error!.Message).Contains(phase);
            await Assert.That(error.InnerException is OperationCanceledException).IsTrue();
            await Assert.That(fixture!.StopCalls).IsEqualTo(stopCalls);
            await Assert.That(fixture.LastStopToken.CanBeCanceled && !fixture.LastStopToken.IsCancellationRequested).IsTrue();
            await Assert.That(fixture.Disposed).IsTrue();
            await Assert.That(pending.Task.IsCompleted).IsFalse();

            pending.SetException(new IOException($"Late {phase} failure."));
        }
        finally
        {
            pending.TrySetResult();
        }
    }

    [Test]
    [Arguments(TransportConformanceCase.CleanShutdown, "native shutdown verification", 2)]
    [Arguments(TransportConformanceCase.SendReceiveSettle, "native settlement verification", 3)]
    public async Task Pending_native_verification_fails_on_cancellation_and_runs_cleanup(
        TransportConformanceCase scenario, string phase, int stopCalls)
    {
        using var cancellation = new CancellationTokenSource();
        var verificationStarted = new TaskCompletionSource<CancellationToken>(TaskCreationOptions.RunContinuationsAsynchronously);
        var verification = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        BrokenFixture? fixture = null;
        var suite = new TransportConformanceSuite<InMemoryDelivery, InMemoryMessageBinding>((tracing, _) =>
            new ValueTask<ITransportConformanceFixture<InMemoryDelivery>>(
                fixture = new BrokenFixture(tracing, false, false, false, verifyNative: token =>
                {
                    verificationStarted.SetResult(token);
                    return new ValueTask(verification.Task);
                })));

        var run = suite.RunAsync(scenario, cancellation.Token);

        try
        {
            var scenarioToken = await verificationStarted.Task.WaitAsync(TimeSpan.FromSeconds(5));
            cancellation.Cancel();

            var error = await Assert.That(async () => await run.WaitAsync(TimeSpan.FromSeconds(5)))
                .Throws<InvalidOperationException>();
            await Assert.That(error!.Message).Contains(phase);
            await Assert.That(error.InnerException is OperationCanceledException).IsTrue();
            await Assert.That(scenarioToken.IsCancellationRequested).IsTrue();
            await Assert.That(fixture!.StopCalls).IsEqualTo(stopCalls);
            await Assert.That(fixture.Disposed).IsTrue();
            await Assert.That(verification.Task.IsCompleted).IsFalse();

            verification.SetException(new IOException($"Late {phase} failure."));
        }
        finally
        {
            verification.TrySetResult();
            await ((Task)run).ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);
        }
    }

    [Test]
    public async Task Final_shutdown_watchdog_allows_disposal_before_late_fault()
    {
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var pending = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        BrokenFixture? fixture = null;

        Task? PauseFinalStop(int call)
        {
            if (call != 3)
            {
                return null;
            }

            entered.SetResult();
            return pending.Task;
        }

        var suite = new TransportConformanceSuite<InMemoryDelivery, InMemoryMessageBinding>((tracing, _) =>
            new ValueTask<ITransportConformanceFixture<InMemoryDelivery>>(
                fixture = new BrokenFixture(tracing, false, false, false, stop: PauseFinalStop)));
        var run = suite.RunAsync(TransportConformanceCase.SendReceiveSettle);

        try
        {
            await entered.Task.WaitAsync(TimeSpan.FromSeconds(5));

            await Assert.That(async () => await run.WaitAsync(TimeSpan.FromSeconds(30))).Throws<TimeoutException>();
            await Assert.That(fixture!.LastStopToken.IsCancellationRequested).IsTrue();
            await Assert.That(fixture.Disposed).IsTrue();
            await Assert.That(pending.Task.IsCompleted).IsFalse();

            pending.SetException(new IOException("Late final shutdown failure."));
        }
        finally
        {
            pending.TrySetResult();
        }
    }

    private sealed class BrokenFixture(MessageTracing tracing, bool earlyStop, bool disposalError, bool nativeViolation,
        bool asynchronousEarlyStop = false, Func<CancellationToken, ValueTask>? verifyNative = null,
        Func<int, Task?>? start = null, Func<int, Task?>? stop = null)
        : ITransportConformanceFixture<InMemoryDelivery>, ITransportShutdownConformanceFixture, ITransport
    {
        private readonly InMemoryFixture _inner = new(tracing);
        private readonly TaskCompletionSource _settlementRelease = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private int _startCalls;
        private int _stopCalls;

        public int StopCalls => Volatile.Read(ref _stopCalls);

        public CancellationToken LastStopToken
        {
            get; private set;
        }

        public bool Disposed
        {
            get; private set;
        }

        public ConformanceLogCapture Logs => _inner.Logs;

        public ITransport Transport => this;

        public string Name => _inner.Transport.Name;

        public string Destination => _inner.Destination;

        public string MessagingSystem => _inner.MessagingSystem;

        public string SettlementOperation => _inner.SettlementOperation;

        public HeaderNames HeaderNames => _inner.HeaderNames;

        public void RegisterConsumer(Func<InMemoryDelivery, CancellationToken, ValueTask<Disposition>> receive)
        {
            _inner.RegisterConsumer(receive);
        }

        public async ValueTask SettleAsync(InMemoryDelivery incoming, Disposition disposition, CancellationToken token)
        {
            // Hold native completion until cleanup, so scheduling cannot hide the premature stop.
            if (asynchronousEarlyStop)
            {
                await _settlementRelease.Task.WaitAsync(token);
            }

            await _inner.SettleAsync(incoming, disposition, token);
        }

        public ValueTask<ConformanceDeadLetter> ReadDeadLetterAsync(CancellationToken token)
        {
            return _inner.ReadDeadLetterAsync(token);
        }

        public Task DisconnectAsync(CancellationToken token)
        {
            return _inner.DisconnectAsync(token);
        }

        public Task ReconnectAsync(CancellationToken token)
        {
            return _inner.ReconnectAsync(token);
        }

        public TransportCapabilities GetCapabilities(string endpoint)
        {
            return _inner.Transport.GetCapabilities(endpoint);
        }

        public ValueTask SendAsync(OutgoingMessage message, CancellationToken token)
        {
            return _inner.Transport.SendAsync(message, token);
        }

        public ValueTask SendBatchAsync(ReadOnlyMemory<OutgoingMessage> messages, CancellationToken token)
        {
            return _inner.Transport.SendBatchAsync(messages, token);
        }

        public ValueTask<TopologyReport> ApplyTopologyAsync(TopologyMode mode, CancellationToken token)
        {
            return _inner.Transport.ApplyTopologyAsync(mode, token);
        }

        public Task StartAsync(CancellationToken token)
        {
            return start?.Invoke(++_startCalls) ?? _inner.Transport.StartAsync(token);
        }

        public async Task StopAsync(CancellationToken token)
        {
            var call = Interlocked.Increment(ref _stopCalls);
            LastStopToken = token;

            if (stop?.Invoke(call) is { } pending)
            {
                await pending;
                return;
            }

            if (call == 1 && asynchronousEarlyStop)
            {
                await Task.Yield();
                return;
            }

            _settlementRelease.TrySetResult();

            if (!earlyStop)
            {
                await _inner.Transport.StopAsync(token);
            }
        }

        public ValueTask VerifySettlementAsync(CancellationToken token)
        {
            return verifyNative?.Invoke(token) ?? _inner.VerifySettlementAsync(token);
        }

        public ValueTask VerifyShutdownAsync(CancellationToken token)
        {
            return nativeViolation
                        ? ValueTask.FromException(new InvalidOperationException("settlement after native close"))
                        : verifyNative?.Invoke(token) ?? _inner.VerifyShutdownAsync(token);
        }

        public async ValueTask DisposeAsync()
        {
            await _inner.DisposeAsync();
            Disposed = true;

            if (disposalError)
            {
                Logs.CreateLogger("native").LogError("receiver disposal failed");
            }
        }
    }
}
