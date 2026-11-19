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
    public async Task Pending_stop_fails_when_scenario_is_cancelled_and_cleanup_releases_it()
    {
        using var cancellation = new CancellationTokenSource();
        BrokenFixture? fixture = null;
        var suite = new TransportConformanceSuite<InMemoryDelivery, InMemoryMessageBinding>((tracing, _) =>
            new ValueTask<ITransportConformanceFixture<InMemoryDelivery>>(
                fixture = new BrokenFixture(tracing, false, false, false, cancelScenarioAfterReceive: cancellation.Cancel)));

        var error = await Assert.That(async () =>
                await suite.RunAsync(TransportConformanceCase.CleanShutdown, cancellation.Token)
                    .WaitAsync(TimeSpan.FromSeconds(5)))
            .Throws<InvalidOperationException>();
        await Assert.That(error!.Message).Contains("in-flight transport shutdown");
        await Assert.That(error.InnerException is OperationCanceledException).IsTrue();
        await Assert.That(fixture!.PendingStopReleased).IsTrue();
    }

    [Test]
    [Arguments(false, "transport drain")]
    [Arguments(true, "transport restart")]
    public async Task Pending_lifecycle_operation_fails_when_scenario_is_cancelled(bool stallRestart, string phase)
    {
        using var cancellation = new CancellationTokenSource();
        BrokenFixture? fixture = null;
        var suite = new TransportConformanceSuite<InMemoryDelivery, InMemoryMessageBinding>((tracing, _) =>
            new ValueTask<ITransportConformanceFixture<InMemoryDelivery>>(
                fixture = new BrokenFixture(tracing, false, false, false,
                    cancelPendingLifecycle: cancellation.Cancel, stallRestart: stallRestart)));

        try
        {
            var error = await Assert.That(async () =>
                    await suite.RunAsync(TransportConformanceCase.SendReceiveSettle, cancellation.Token)
                        .WaitAsync(TimeSpan.FromSeconds(5)))
                .Throws<InvalidOperationException>();
            await Assert.That(error!.Message).Contains(phase);
            await Assert.That(error.InnerException is OperationCanceledException).IsTrue();
            await Assert.That(fixture!.PendingLifecycleReleasedByCleanup).IsTrue();
        }
        finally
        {
            fixture?.ReleasePendingLifecycle();
        }
    }

    [Test]
    public async Task Pending_native_verification_fails_on_cancellation_and_runs_cleanup()
    {
        using var cancellation = new CancellationTokenSource();
        var verificationStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var verification = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        BrokenFixture? fixture = null;
        var suite = new TransportConformanceSuite<InMemoryDelivery, InMemoryMessageBinding>((tracing, _) =>
            new ValueTask<ITransportConformanceFixture<InMemoryDelivery>>(
                fixture = new BrokenFixture(tracing, false, false, false, verifyShutdown: () =>
                {
                    verificationStarted.SetResult();
                    return new ValueTask(verification.Task);
                })));

        var run = suite.RunAsync(TransportConformanceCase.CleanShutdown, cancellation.Token);
        try
        {
            await verificationStarted.Task.WaitAsync(TimeSpan.FromSeconds(5));
            cancellation.Cancel();

            var error = await Assert.That(async () => await run.WaitAsync(TimeSpan.FromSeconds(5)))
                .Throws<InvalidOperationException>();
            await Assert.That(error!.Message).Contains("native shutdown verification");
            await Assert.That(error.InnerException is OperationCanceledException).IsTrue();
            await Assert.That(fixture!.StopCalls).IsEqualTo(2);
            await Assert.That(fixture.Disposed).IsTrue();
            await Assert.That(verification.Task.IsCompleted).IsFalse();
        }
        finally
        {
            verification.SetResult();
            await ((Task)run).ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);
        }
    }

    private sealed class BrokenFixture(MessageTracing tracing, bool earlyStop, bool disposalError, bool nativeViolation,
        bool asynchronousEarlyStop = false, Action? cancelScenarioAfterReceive = null,
        Action? cancelPendingLifecycle = null, bool stallRestart = false, Func<ValueTask>? verifyShutdown = null)
        : ITransportConformanceFixture<InMemoryDelivery>, ITransportShutdownConformanceFixture, ITransport
    {
        private readonly InMemoryFixture _inner = new(tracing);
        private readonly TaskCompletionSource _settlementRelease = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly TaskCompletionSource _stopRelease = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly TaskCompletionSource _pendingLifecycle = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private int _startCalls;
        private int _stopCalls;
        public bool PendingLifecycleReleasedByCleanup { get; private set; }
        public void ReleasePendingLifecycle() => _pendingLifecycle.TrySetResult();
        public int StopCalls => Volatile.Read(ref _stopCalls);
        public bool Disposed { get; private set; }
        public bool PendingStopReleased => _stopRelease.Task.IsCompletedSuccessfully;
        public ConformanceLogCapture Logs => _inner.Logs;
        public ITransport Transport => this;
        public string Name => _inner.Transport.Name;
        public string Destination => _inner.Destination;
        public string MessagingSystem => _inner.MessagingSystem;
        public string SettlementOperation => _inner.SettlementOperation;
        public HeaderNames HeaderNames => _inner.HeaderNames;
        public void RegisterConsumer(Func<InMemoryDelivery, CancellationToken, ValueTask<Disposition>> receive) =>
            _inner.RegisterConsumer(async (delivery, token) =>
            {
                var disposition = await receive(delivery, token);
                cancelScenarioAfterReceive?.Invoke();
                return disposition;
            });
        public async ValueTask SettleAsync(InMemoryDelivery incoming, Disposition disposition, CancellationToken token)
        {
            // Hold native completion until cleanup, so scheduling cannot hide the premature stop.
            if (asynchronousEarlyStop) await _settlementRelease.Task.WaitAsync(token);
            await _inner.SettleAsync(incoming, disposition, token);
        }
        public ValueTask<ConformanceDeadLetter> ReadDeadLetterAsync(CancellationToken token) => _inner.ReadDeadLetterAsync(token);
        public Task DisconnectAsync(CancellationToken token) => _inner.DisconnectAsync(token);
        public Task ReconnectAsync(CancellationToken token) => _inner.ReconnectAsync(token);
        public TransportCapabilities GetCapabilities(string endpoint) => _inner.Transport.GetCapabilities(endpoint);
        public ValueTask SendAsync(OutgoingMessage message, CancellationToken token) => _inner.Transport.SendAsync(message, token);
        public ValueTask SendBatchAsync(ReadOnlyMemory<OutgoingMessage> messages, CancellationToken token) => _inner.Transport.SendBatchAsync(messages, token);
        public ValueTask<TopologyReport> ApplyTopologyAsync(TopologyMode mode, CancellationToken token) => _inner.Transport.ApplyTopologyAsync(mode, token);
        public Task StartAsync(CancellationToken token)
        {
            if (stallRestart && ++_startCalls == 2)
                return StallLifecycle();
            return _inner.Transport.StartAsync(token);
        }

        private Task StallLifecycle()
        {
            cancelPendingLifecycle!.Invoke();
            return _pendingLifecycle.Task;
        }
        public async Task StopAsync(CancellationToken token)
        {
            var firstStop = Interlocked.Increment(ref _stopCalls) == 1;
            if (firstStop && cancelScenarioAfterReceive is not null)
            {
                await _stopRelease.Task;
                return;
            }
            if (firstStop && cancelPendingLifecycle is not null && !stallRestart)
            {
                await StallLifecycle();
                return;
            }
            if (!firstStop && cancelPendingLifecycle is not null)
            {
                PendingLifecycleReleasedByCleanup = token.CanBeCanceled && !token.IsCancellationRequested;
                ReleasePendingLifecycle();
            }
            _stopRelease.TrySetResult();
            if (firstStop && asynchronousEarlyStop)
            {
                await Task.Yield();
                return;
            }
            _settlementRelease.TrySetResult();
            if (!earlyStop) await _inner.Transport.StopAsync(token);
        }
        public ValueTask VerifyShutdownAsync(CancellationToken token) => nativeViolation
            ? ValueTask.FromException(new InvalidOperationException("settlement after native close"))
            : verifyShutdown?.Invoke() ?? _inner.VerifyShutdownAsync(token);
        public async ValueTask DisposeAsync()
        {
            await _inner.DisposeAsync();
            Disposed = true;
            if (disposalError) Logs.CreateLogger("native").LogError("receiver disposal failed");
        }
    }
}
