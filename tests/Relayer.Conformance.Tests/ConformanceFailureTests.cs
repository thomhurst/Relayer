using System.Threading.Tasks.Sources;
using Relayer.InMemory;
using Relayer.Testing;
using Relayer.Transports;

namespace Relayer.Conformance.Tests;

public class ConformanceFailureTests
{
    [Test]
    [Arguments(0L)]
    [Arguments(-TimeSpan.TicksPerMillisecond)]
    [Arguments(1L)]
    [Arguments(TimeSpan.TicksPerMillisecond - 1L)]
    [Arguments(4_294_967_295L * TimeSpan.TicksPerMillisecond)]
    public async Task Constructor_rejects_unsupported_scenario_timeout(long timeoutTicks)
    {
        Action construct = () => _ = new TransportConformanceSuite<InMemoryDelivery, InMemoryMessageBinding>(
            static (tracing, _) => new(new InMemoryFixture(tracing)),
            timeout: TimeSpan.FromTicks(timeoutTicks));

        var error = await Assert.That(construct).Throws<ArgumentOutOfRangeException>();
        await Assert.That(error!.ParamName).IsEqualTo("timeout");
    }

    [Test]
    public void Constructor_accepts_one_millisecond_scenario_timeout()
    {
        _ = new TransportConformanceSuite<InMemoryDelivery, InMemoryMessageBinding>(
            static (tracing, _) => new(new InMemoryFixture(tracing)),
            timeout: TimeSpan.FromMilliseconds(1));
    }

    [Test]
    [Arguments(4_294_967_294d)]
    [Arguments(4_294_967_294.5d)]
    public async Task Scenario_runs_at_the_timer_timeout_boundary(double timeoutMilliseconds)
    {
        var suite = new TransportConformanceSuite<InMemoryDelivery, InMemoryMessageBinding>(
            static (tracing, _) => new(new InMemoryFixture(tracing, capabilities: new())),
            timeout: TimeSpan.FromMilliseconds(timeoutMilliseconds));

        var result = await suite.RunAsync(TransportConformanceCase.Delay);

        await Assert.That(result.Skipped).IsTrue();
    }

    [Test]
    public async Task Pre_canceled_run_fails_before_invoking_fixture_factory()
    {
        using var caller = new CancellationTokenSource();
        caller.Cancel();

        var factoryCalls = 0;
        var suite = new TransportConformanceSuite<InMemoryDelivery, InMemoryMessageBinding>((tracing, _) =>
        {
            factoryCalls++;
            return new(new InMemoryFixture(tracing, capabilities: new()));
        });

        var error = await Assert.That(async () => await suite.RunAsync(TransportConformanceCase.Delay, caller.Token))
            .Throws<InvalidOperationException>();
        await Assert.That(error!.Message).Contains("Delay failed during fixture creation");
        await Assert.That(error.InnerException is OperationCanceledException).IsTrue();
        await Assert.That(factoryCalls).IsEqualTo(0);
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task Pending_fixture_creation_is_bounded_and_late_fixture_is_disposed(bool useDeadline)
    {
        using var caller = new CancellationTokenSource();
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var creation = new TaskCompletionSource<ITransportConformanceFixture<InMemoryDelivery>>(TaskCreationOptions.RunContinuationsAsynchronously);
        var fixture = new FailingFixture();
        CancellationToken factoryToken = default;
        var suite = new TransportConformanceSuite<InMemoryDelivery, InMemoryMessageBinding>((_, token) =>
        {
            factoryToken = token;
            entered.SetResult();
            return new(creation.Task);
        }, useDeadline ? TimeSpan.FromMilliseconds(100) : TimeSpan.FromSeconds(30));
        var run = suite.RunAsync(TransportConformanceCase.SendReceiveSettle, caller.Token);

        try
        {
            await entered.Task.WaitAsync(TimeSpan.FromSeconds(5));

            if (!useDeadline)
            {
                caller.Cancel();
            }

            var error = await CaptureAsync(run.WaitAsync(TimeSpan.FromSeconds(5)));
            await Assert.That(error.Message).Contains("SendReceiveSettle failed during fixture creation");
            await Assert.That(error.InnerException is OperationCanceledException).IsTrue();
            await Assert.That(factoryToken.IsCancellationRequested).IsTrue();
            await Assert.That(creation.Task.IsCompleted).IsFalse();
            await Assert.That(fixture.Disposed).IsFalse();

            creation.SetResult(fixture);
            await fixture.DisposedSignal.Task.WaitAsync(TimeSpan.FromSeconds(5));
            await Assert.That(fixture.DisposeCount).IsEqualTo(1);
        }
        finally
        {
            creation.TrySetResult(fixture);
            await CaptureAsync(run.WaitAsync(TimeSpan.FromSeconds(5)));
        }
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task Fixture_creation_failure_retains_original_exception(bool synchronous)
    {
        var original = new IOException("fixture provisioning failed");
        var suite = new TransportConformanceSuite<InMemoryDelivery, InMemoryMessageBinding>((_, _) =>
            synchronous ? throw original : ValueTask.FromException<ITransportConformanceFixture<InMemoryDelivery>>(original));
        var error = await CaptureAsync(suite.RunAsync(TransportConformanceCase.SendReceiveSettle));
        await Assert.That(error.Message).Contains("fixture creation");
        await Assert.That(error.InnerException).IsSameReferenceAs(original);
    }

    [Test]
    public async Task Cancellation_racing_completed_creation_disposes_fixture_once()
    {
        using var caller = new CancellationTokenSource();
        var fixture = new FailingFixture();
        var suite = new TransportConformanceSuite<InMemoryDelivery, InMemoryMessageBinding>((_, _) =>
        {
            caller.Cancel();
            return new(fixture);
        });
        await CaptureAsync(suite.RunAsync(TransportConformanceCase.SendReceiveSettle, caller.Token));
        await Assert.That(fixture.DisposeCount).IsEqualTo(1);
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task Abandoned_creation_observes_late_factory_and_disposal_failures(bool factoryFails)
    {
        using var caller = new CancellationTokenSource();
        var creation = new ObservedCompletion<ITransportConformanceFixture<InMemoryDelivery>>();
        var disposal = new ObservedCompletion<int>();
        var fixture = new FailingFixture { DisposalCompletion = new ValueTask(disposal, 0) };
        var suite = new TransportConformanceSuite<InMemoryDelivery, InMemoryMessageBinding>((_, _) => new(creation, 0));
        var run = suite.RunAsync(TransportConformanceCase.SendReceiveSettle, caller.Token);

        try
        {
            // RunAsync has returned its pending task, so the factory has been invoked.
            caller.Cancel();
            var error = await CaptureAsync(run.WaitAsync(TimeSpan.FromSeconds(5)));
            await Assert.That(error.Message).Contains("fixture creation");

            if (factoryFails)
            {
                creation.Fail(new IOException("late provisioning failed"));
            }
            else
            {
                creation.Complete(fixture);
                await fixture.DisposedSignal.Task.WaitAsync(TimeSpan.FromSeconds(5));
                disposal.Fail(new IOException("late disposal failed"));
                await disposal.Observed.Task.WaitAsync(TimeSpan.FromSeconds(5));
                await Assert.That(disposal.ConsumptionCount).IsEqualTo(1);
                await Assert.That(fixture.DisposeCount).IsEqualTo(1);
            }

            await creation.Observed.Task.WaitAsync(TimeSpan.FromSeconds(5));
            await Assert.That(creation.ConsumptionCount).IsEqualTo(1);
        }
        finally
        {
            creation.TryComplete(fixture);
            disposal.TryComplete(0);
            await CaptureAsync(run.WaitAsync(TimeSpan.FromSeconds(5)));
        }
    }

    [Test]
    [Arguments(false, false)]
    [Arguments(false, true)]
    [Arguments(true, false)]
    [Arguments(true, true)]
    public async Task Pending_fixture_disposal_is_bounded_and_consumed_once(bool scenarioFails, bool lateFailure)
    {
        using var caller = new CancellationTokenSource();
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var disposal = new ObservedCompletion<int>();
        var disposeCount = 0;
        FailingFixture? failedFixture = null;
        var suite = new TransportConformanceSuite<InMemoryDelivery, InMemoryMessageBinding>((tracing, _) =>
        {
            if (scenarioFails)
            {
                failedFixture = new FailingFixture
                {
                    CancelAfterSend = caller,
                    DisposalCompletion = new ValueTask(disposal, 0),
                };
                return new(failedFixture);
            }

            return new(new InMemoryFixture(tracing)
            {
                AfterDisposal = () =>
                {
                    Interlocked.Increment(ref disposeCount);
                    entered.TrySetResult();
                    return new ValueTask(disposal, 0);
                },
            });
        });
        var run = suite.RunAsync(TransportConformanceCase.SendReceiveSettle, caller.Token);
        var watchdog = TimeSpan.FromSeconds(30);

        try
        {
            await (scenarioFails ? failedFixture!.DisposedSignal.Task : entered.Task).WaitAsync(watchdog);
            var error = await CaptureAsync(run.WaitAsync(watchdog));
            var failures = error is AggregateException aggregate ? aggregate.Flatten().InnerExceptions.ToArray() : [error];
            await Assert.That(failures.Length).IsEqualTo(scenarioFails ? 2 : 1);
            var timeout = failures.OfType<TimeoutException>().Single();
            await Assert.That(timeout.Message).Contains("fixture disposal");
            await Assert.That(timeout.Message).Contains("15");
            await Assert.That(disposal.Observed.Task.IsCompleted).IsFalse();

            if (scenarioFails)
            {
                var scenarioFailure = failures.OfType<InvalidOperationException>().Single();
                await Assert.That(scenarioFailure.Message).Contains("receive completion");
                await Assert.That(scenarioFailure.InnerException is OperationCanceledException).IsTrue();
                await Assert.That(caller.IsCancellationRequested).IsTrue();
                await Assert.That(failedFixture!.StoppedWithLiveToken).IsTrue();
            }

            if (lateFailure)
            {
                disposal.Fail(new IOException("late fixture disposal failed"));
            }
            else
            {
                disposal.Complete(0);
            }

            await disposal.Observed.Task.WaitAsync(watchdog);
            await Assert.That(disposal.ConsumptionCount).IsEqualTo(1);
            await Assert.That(scenarioFails ? failedFixture!.DisposeCount : disposeCount).IsEqualTo(1);
        }
        finally
        {
            disposal.TryComplete(0);

            try
            {
                await run.WaitAsync(watchdog);
            }
            catch (Exception) when (run.IsCompleted)
            {
            }

            await disposal.Observed.Task.WaitAsync(watchdog);
        }
    }

    [Test]
    [Arguments(false, false)]
    [Arguments(true, false)]
    [Arguments(false, true)]
    [Arguments(true, true)]
    public async Task Scenario_failure_survives_shutdown_and_disposal_failures(bool stopFails, bool disposeFails)
    {
        var fixture = new FailingFixture { StopFails = stopFails, DisposeFails = disposeFails };
        var error = await CaptureAsync(Suite(fixture).RunAsync(TransportConformanceCase.SendReceiveSettle));
        var failures = error is AggregateException aggregate ? aggregate.Flatten().InnerExceptions.ToArray() : [error];
        await Assert.That(failures.Length).IsEqualTo(1 + (stopFails ? 1 : 0) + (disposeFails ? 1 : 0));
        var scenarioFailure = failures.Single(failure => ReferenceEquals(failure.InnerException, fixture.PrimaryFailure));
        await Assert.That(scenarioFailure.Message).Contains("send 1");

        if (stopFails)
        {
            await Assert.That(failures).Contains(fixture.StopFailure);
        }

        if (disposeFails)
        {
            await Assert.That(failures).Contains(fixture.DisposeFailure);
        }

        await Assert.That(fixture.StoppedWithLiveToken).IsTrue();
        await Assert.That(fixture.Disposed).IsTrue();
    }

    [Test]
    public async Task Cancelled_receive_reports_phase_and_uses_fresh_shutdown_token()
    {
        using var cancellation = new CancellationTokenSource();
        var fixture = new FailingFixture { CancelAfterSend = cancellation };
        var error = await CaptureAsync(Suite(fixture).RunAsync(TransportConformanceCase.SendReceiveSettle, cancellation.Token));
        await Assert.That(error.Message).Contains("receive completion");
        await Assert.That(error.InnerException).IsTypeOf<OperationCanceledException>();
        await Assert.That(fixture.StoppedWithLiveToken).IsTrue();
        await Assert.That(fixture.Disposed).IsTrue();
    }

    [Test]
    public async Task Cancelled_pending_send_reports_phase_and_preserves_borrowed_buffers_during_cleanup()
    {
        using var cancellation = new CancellationTokenSource();
        var started = new TaskCompletionSource<OutgoingMessage>(TaskCreationOptions.RunContinuationsAsynchronously);
        var pending = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var fixture = new FailingFixture
        {
            Send = message =>
            {
                started.SetResult(message);
                return new ValueTask(pending.Task);
            },
        };

        var run = Suite(fixture).RunAsync(TransportConformanceCase.SendReceiveSettle, cancellation.Token);

        try
        {
            var message = await started.Task.WaitAsync(TimeSpan.FromSeconds(5));
            var body = message.Body.ToArray();
            var headers = message.Headers.ToArray();
            cancellation.Cancel();

            var error = await CaptureAsync(run).WaitAsync(TimeSpan.FromSeconds(5));
            await Assert.That(error.Message).Contains("send 1");
            await Assert.That(error.InnerException).IsTypeOf<OperationCanceledException>();
            await Assert.That(fixture.StoppedWithLiveToken).IsTrue();
            await Assert.That(fixture.Disposed).IsTrue();
            await Assert.That(pending.Task.IsCompleted).IsFalse();
            await Assert.That(message.Body.Span.SequenceEqual(body)).IsTrue();
            await Assert.That(message.Headers.Span.SequenceEqual(headers)).IsTrue();
        }
        finally
        {
            pending.SetResult();
        }
    }

    [Test]
    public async Task Failed_start_is_reported_and_resources_are_cleaned_up()
    {
        var fixture = new FailingFixture { StartFails = true };
        var error = await CaptureAsync(Suite(fixture).RunAsync(TransportConformanceCase.SendReceiveSettle));
        await Assert.That(error.Message).Contains("transport start");
        await Assert.That(error.InnerException).IsSameReferenceAs(fixture.PrimaryFailure);
        await Assert.That(fixture.StoppedWithLiveToken).IsTrue();
        await Assert.That(fixture.Disposed).IsTrue();
    }

    [Test]
    public async Task Clean_shutdown_requires_explicit_native_lifecycle_evidence()
    {
        var fixture = new FailingFixture();
        var error = await CaptureAsync(Suite(fixture).RunAsync(TransportConformanceCase.CleanShutdown));
        await Assert.That(error.Message).Contains("ITransportShutdownConformanceFixture");
        await Assert.That(fixture.Disposed).IsTrue();
    }

    private static TransportConformanceSuite<InMemoryDelivery, InMemoryMessageBinding> Suite(FailingFixture fixture)
    {
        return new((_, _) => new ValueTask<ITransportConformanceFixture<InMemoryDelivery>>(fixture));
    }

    private static async Task<Exception> CaptureAsync(Task task)
    {
        try
        {
            await task;
        }
        catch (Exception error)
        {
            return error;
        }

        throw new InvalidOperationException("Expected conformance failure.");
    }

    // A single-consumption ValueTask source exposes observation without process-wide
    // UnobservedTaskException handlers or forced garbage collection in parallel tests.
    private sealed class ObservedCompletion<T> : IValueTaskSource<T>, IValueTaskSource
    {
        private ManualResetValueTaskSourceCore<T> _source = new() { RunContinuationsAsynchronously = true };
        private bool _completed;

        internal TaskCompletionSource Observed
        {
            get;
        } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        internal int ConsumptionCount
        {
            get; private set;
        }

        internal void Complete(T result)
        {
            _completed = true;
            _source.SetResult(result);
        }

        internal void Fail(Exception error)
        {
            _completed = true;
            _source.SetException(error);
        }

        internal void TryComplete(T result)
        {
            if (!_completed)
            {
                Complete(result);
            }
        }

        public T GetResult(short token)
        {
            ConsumptionCount++;

            try
            {
                return _source.GetResult(token);
            }
            finally
            {
                Observed.TrySetResult();
            }
        }

        void IValueTaskSource.GetResult(short token)
        {
            GetResult(token);
        }

        public ValueTaskSourceStatus GetStatus(short token)
        {
            return _source.GetStatus(token);
        }

        public void OnCompleted(Action<object?> continuation, object? state, short token, ValueTaskSourceOnCompletedFlags flags)
        {
            _source.OnCompleted(continuation, state, token, flags);
        }
    }

    private sealed class FailingFixture : ITransportConformanceFixture<InMemoryDelivery>, ITransport
    {
        internal Exception PrimaryFailure
        {
            get;
        } = new IOException("native operation failed");

        internal Exception StopFailure
        {
            get;
        } = new IOException("native shutdown failed");

        internal Exception DisposeFailure
        {
            get;
        } = new IOException("fixture disposal failed");

        internal bool StopFails
        {
            get; init;
        }

        internal bool DisposeFails
        {
            get; init;
        }

        internal bool StartFails
        {
            get; init;
        }

        internal ValueTask DisposalCompletion
        {
            get; init;
        }

        internal Func<OutgoingMessage, ValueTask>? Send
        {
            get; init;
        }

        internal CancellationTokenSource? CancelAfterSend
        {
            get; init;
        }

        internal bool StoppedWithLiveToken
        {
            get; private set;
        }

        internal bool Disposed
        {
            get; private set;
        }

        internal int DisposeCount
        {
            get; private set;
        }

        internal TaskCompletionSource DisposedSignal
        {
            get;
        } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public ITransport Transport => this;

        public string Name => "failing";

        public string Destination => "isolated";

        public string MessagingSystem => "relayer.inmemory";

        public string SettlementOperation => "complete";

        public HeaderNames HeaderNames
        {
            get;
        } = new();

        public TransportCapabilities GetCapabilities(string endpoint)
        {
            return new();
        }

        public void RegisterConsumer(Func<InMemoryDelivery, CancellationToken, ValueTask<Disposition>> receive)
        {
        }

        public Task StartAsync(CancellationToken cancellationToken)
        {
            return StartFails ? Task.FromException(PrimaryFailure) : Task.CompletedTask;
        }

        public Task StopAsync(CancellationToken cancellationToken)
        {
            StoppedWithLiveToken = cancellationToken.CanBeCanceled && !cancellationToken.IsCancellationRequested;
            return StopFails ? Task.FromException(StopFailure) : Task.CompletedTask;
        }

        public ValueTask SendAsync(OutgoingMessage message, CancellationToken cancellationToken)
        {
            if (Send is not null)
            {
                return Send(message);
            }

            if (CancelAfterSend is null)
            {
                return ValueTask.FromException(PrimaryFailure);
            }

            CancelAfterSend.Cancel();
            return default;
        }

        public ValueTask DisposeAsync()
        {
            DisposeCount++;
            Disposed = true;
            DisposedSignal.TrySetResult();
            return DisposeFails ? ValueTask.FromException(DisposeFailure) : DisposalCompletion;
        }

        public Task DisconnectAsync(CancellationToken cancellationToken)
        {
            throw new NotSupportedException();
        }

        public Task ReconnectAsync(CancellationToken cancellationToken)
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

        public ValueTask SettleAsync(InMemoryDelivery incoming, Disposition disposition, CancellationToken cancellationToken)
        {
            throw new NotSupportedException();
        }

        public ValueTask<ConformanceDeadLetter> ReadDeadLetterAsync(CancellationToken cancellationToken)
        {
            throw new NotSupportedException();
        }
    }
}
