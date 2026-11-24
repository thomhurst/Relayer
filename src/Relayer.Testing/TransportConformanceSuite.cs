using System.Buffers;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Text;
using System.Text.Json;
using System.Threading.Channels;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Relayer.Pipeline;
using Relayer.Serialization;
using Relayer.Transports;

namespace Relayer.Testing;

/// <summary>Reusable native adapter contracts. Run each case as a separate test in any test framework.</summary>
/// <remarks>No broker semantics are emulated. The fixture owns native provisioning, settlement and fault injection.</remarks>
public sealed class TransportConformanceSuite<TIncoming, TBinding>
    where TBinding : struct, IMessageBinding<TIncoming>
{
    private readonly Func<MessageTracing, CancellationToken, ValueTask<ITransportConformanceFixture<TIncoming>>> _create;
    private readonly TimeSpan _timeout;
    private static readonly TimeSpan s_delay = TimeSpan.FromMilliseconds(200);
    private static readonly TimeSpan s_minimumTimerTimeout = TimeSpan.FromMilliseconds(1);
    private static readonly TimeSpan s_outageWatchdogGrace = TimeSpan.FromSeconds(5);
    // The runtime truncates fractional milliseconds and reserves uint.MaxValue for an infinite timer.
    private static readonly TimeSpan s_outageSendTimeoutLimit = TimeSpan.FromMilliseconds(uint.MaxValue) - s_outageWatchdogGrace;
    private const string TraceParent = "00-0123456789abcdef0123456789abcdef-0123456789abcdef-01";
    private const string TraceState = "relayer=conformance";

    /// <summary>Creates a suite with a fresh native fixture per case and a bounded operation deadline.</summary>
    public TransportConformanceSuite(
        Func<MessageTracing, CancellationToken, ValueTask<ITransportConformanceFixture<TIncoming>>> create,
        TimeSpan? timeout = null)
    {
        ArgumentNullException.ThrowIfNull(create);
        _create = create;
        _timeout = timeout ?? TimeSpan.FromSeconds(30);

        // CancelAfter truncates fractional milliseconds before checking the timer range.
        if (_timeout < s_minimumTimerTimeout || (long)_timeout.TotalMilliseconds >= uint.MaxValue)
        {
            throw new ArgumentOutOfRangeException(nameof(timeout), "The scenario timeout must be at least 1 millisecond and less than 4,294,967,295 milliseconds.");
        }
    }

    /// <summary>Runs one transport contract. Contract failures propagate to the caller.</summary>
    /// <returns>
    /// The result, with <see cref="TransportConformanceResult.SkipReason"/> set when the endpoint lacks a required
    /// native capability or <see cref="ITransportConformanceFixture{TIncoming}.OutageSendTimeout"/>
    /// is <see langword="null"/> for <see cref="TransportConformanceCase.SendDuringOutage"/>.
    /// </returns>
    /// <remarks>An out-of-range non-null outage timeout throws <see cref="InvalidOperationException"/>.</remarks>
    public async Task<TransportConformanceResult> RunAsync(TransportConformanceCase scenario, CancellationToken cancellationToken = default)
    {
        if (!Enum.IsDefined(scenario))
        {
            throw new ArgumentOutOfRangeException(nameof(scenario));
        }

        using var deadline = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        deadline.CancelAfter(_timeout);
        var token = deadline.Token;
        using var tracing = new MessageTracing(Options.Create(new TracingOptions()), []);
        var spans = new ConcurrentQueue<Activity>();
        using var listener = new ActivityListener
        {
            ShouldListenTo = source => ReferenceEquals(source, tracing.Source),
            Sample = static (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
            ActivityStopped = spans.Enqueue,
        };
        ActivitySource.AddActivityListener(listener);
        Task<ITransportConformanceFixture<TIncoming>>? creation = null;
        ITransportConformanceFixture<TIncoming> fixture;

        try
        {
            token.ThrowIfCancellationRequested();
            creation = _create(tracing, token).AsTask();
            fixture = await creation.WaitAsync(token).ConfigureAwait(false);
        }
        catch (Exception error)
        {
            // If cancellation wins, cleanup owns the task even when creation completes
            // between WaitAsync throwing and this handoff. Consume each ValueTask only once.
            if (creation is not null)
            {
                _ = DisposeLateFixtureAsync(creation);
            }

            throw new InvalidOperationException($"Transport conformance {scenario} failed during fixture creation.", error);
        }

        Exception? failure = null;

        try
        {
            return await RunFixtureAsync(scenario, fixture, tracing, spans, token).ConfigureAwait(false);
        }
        catch (Exception error)
        {
            failure = error;
            throw;
        }
        finally
        {
            try
            {
                await DisposeFixtureAsync(scenario, fixture).ConfigureAwait(false);

                if (scenario == TransportConformanceCase.CleanShutdown && fixture is ITransportShutdownConformanceFixture shutdown)
                {
                    shutdown.Logs.AssertClean();
                }
            }
            catch (Exception cleanup) when (failure is not null)
            {
                throw new AggregateException("Transport conformance and fixture disposal both failed.", failure, cleanup);
            }
        }
    }

    private static async Task DisposeFixtureAsync(TransportConformanceCase scenario, ITransportConformanceFixture<TIncoming> fixture)
    {
        // Bound only the wait: IAsyncDisposable has no cancellation contract.
        using var cleanupDeadline = new CancellationTokenSource(TimeSpan.FromSeconds(15));
        var disposal = fixture.DisposeAsync().AsTask();

        try
        {
            await disposal.WaitAsync(cleanupDeadline.Token).ConfigureAwait(false);
        }
        catch (OperationCanceledException error) when (cleanupDeadline.IsCancellationRequested)
        {
            ObserveLateFault(disposal);
            throw new TimeoutException($"Transport conformance {scenario} fixture disposal exceeded its independent 15-second cleanup budget. Disposal continues; the fixture must release its owned resources.", error);
        }
    }

    private static void ObserveLateFault(Task abandoned)
    {
        _ = abandoned.ContinueWith(static task =>
        {
            _ = task.Exception;
        }, CancellationToken.None,
            TaskContinuationOptions.OnlyOnFaulted | TaskContinuationOptions.ExecuteSynchronously, TaskScheduler.Default);
    }

    private static async Task DisposeLateFixtureAsync(Task<ITransportConformanceFixture<TIncoming>> creation)
    {
        try
        {
            var fixture = await creation.ConfigureAwait(false);
            await fixture.DisposeAsync().ConfigureAwait(false);
        }
        catch (Exception)
        {
            // The scenario has already failed. Observe late creation and disposal failures
            // without leaving an unobserved fault on this background cleanup task.
        }
    }

    private static async Task<TransportConformanceResult> RunFixtureAsync(TransportConformanceCase scenario,
        ITransportConformanceFixture<TIncoming> fixture, MessageTracing tracing, ConcurrentQueue<Activity> spans, CancellationToken token)
    {
        if (scenario == TransportConformanceCase.CleanShutdown && fixture is not ITransportShutdownConformanceFixture)
        {
            throw new InvalidOperationException("CleanShutdown requires ITransportShutdownConformanceFixture with native lifecycle evidence and fixture-local logging.");
        }

        var outageSendTimeout = TimeSpan.Zero;

        if (scenario == TransportConformanceCase.SendDuringOutage)
        {
            if (fixture.OutageSendTimeout is not { } budget)
            {
                return new(scenario, "Fixture has no bounded broker-outage send contract.");
            }

            Require(budget >= s_minimumTimerTimeout, "OutageSendTimeout must be at least 1 millisecond for SendDuringOutage. Set it to null to skip this case.");
            Require(budget < s_outageSendTimeoutLimit, "OutageSendTimeout must be less than 4,294,962,295 milliseconds to leave five seconds for the SendDuringOutage watchdog.");
            outageSendTimeout = budget;
        }

        var capabilities = fixture.Transport.GetCapabilities(fixture.Destination);
        var skip = SkipReason(scenario, capabilities);

        if (skip is not null)
        {
            return new(scenario, skip);
        }

        var received = Channel.CreateUnbounded<Delivery>();
        var control = new ConformanceControl();
        var services = new ServiceCollection();
        services.AddSingleton(control);
        services.AddRelayer(builder =>
        {
            builder.AddMessageTypesFromThisAssembly();
            new ConsumerBuilder<object>(builder.Services, builder.Infrastructure().Handlers, "manual").Handler<ConformanceHandler>();

        });
        await using var provider = services.BuildServiceProvider();
        var pipeline = new ConsumePipelineBuilder(provider.GetRequiredService<IServiceScopeFactory>(),
            provider.GetRequiredService<MessageTypeRegistry>(), provider.GetRequiredService<IMessageSerializer>(),
            static (_, error, _) => ValueTask.FromException(error))
            .Handler(provider.GetRequiredService<HandlerRegistry>().GetHandler<ConformanceMessage, ConformanceHandler>())
            .Tracing(tracing, new()
            {
                System = fixture.MessagingSystem,
                Destination = fixture.ProcessDestinationName
            })
            .Settle(async context =>
            {
                var native = context.Features.GetRequired<NativeMessage<TIncoming>>();
                await fixture.SettleAsync(native.Message, context.Features.GetRequired<ConformanceDecision>().Value,
                    context.CancellationToken).ConfigureAwait(false);
                native.Disposition = Disposition.SettledByHandler;
            }).Build(32);
        var receive = new ReceivePipeline<TIncoming, TBinding>(pipeline, fixture.HeaderNames, 32);
        var shutdown = new ShutdownProgress();
        fixture.RegisterConsumer(async (incoming, cancellation) =>
        {
            var disposition = await receive.ProcessAsync(incoming, cancellation).ConfigureAwait(false);

            if (Volatile.Read(ref shutdown.StopTask)?.IsCompleted == true)
            {
                Volatile.Write(ref shutdown.StopFinishedFirst, 1);
            }

            Volatile.Write(ref shutdown.ConsumerFinished, 1);
            return disposition;
        });
        var attempts = 0;
        var redeliverAt = DateTimeOffset.MinValue;
        var orderRelease = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        control.Handle = context =>
        {
            var incoming = context.Features.GetRequired<NativeMessage<TIncoming>>().Message;
            var body = TBinding.GetBody(incoming).ToArray();
            Require(TBinding.TryGetHeader(incoming, "conformance", out var header) && header.ToString() == "owned", "Application header changed.");
            var delivery = new Delivery(body, Read(incoming, "traceparent"), Read(incoming, "tracestate"), TBinding.GetDeliveryCount(incoming), DateTimeOffset.UtcNow);
            received.Writer.TryWrite(delivery);

            if (scenario == TransportConformanceCase.FailureHandlingFailure && Interlocked.Increment(ref attempts) == 1)
            {
                throw new InvalidOperationException("Conformance failure handler also fails.");
            }

            if (scenario is TransportConformanceCase.Retry or TransportConformanceCase.RedeliveryDelay && Interlocked.Increment(ref attempts) == 1)
            {
                var delay = scenario == TransportConformanceCase.Retry ? TimeSpan.Zero : NativeDelay(capabilities.MaxRedeliveryDelay!.Value);
                redeliverAt = DateTimeOffset.UtcNow + delay;
                return new(Disposition.Redeliver(delay));
            }

            if (scenario == TransportConformanceCase.DeadLetter && Encoding.UTF8.GetString(body) == "{\"Value\":1}")
            {
                return new(Disposition.DeadLettered(new("conformance_poison", "expected failure")));
            }

            if (scenario == TransportConformanceCase.CleanShutdown)
            {
                return WaitForOrderAsync(orderRelease.Task, token);
            }

            return new(Disposition.Complete);
        };

        var progress = new ScenarioProgress { Phase = "transport start" };
        Exception? failure = null;

        try
        {
            await WaitForLifecycleAsync(fixture.Transport.StartAsync(token), token).ConfigureAwait(false);
            await RunScenarioAsync(scenario, fixture, received.Reader, spans, () => redeliverAt, orderRelease, progress, shutdown,
                outageSendTimeout, token).ConfigureAwait(false);
        }
        catch (Exception error)
        {
            failure = new InvalidOperationException($"Transport conformance {scenario} failed during {progress.Phase}.", error);
            throw failure;
        }
        finally
        {
            orderRelease.TrySetResult();
            // Cleanup has its own bounded budget, even when the scenario deadline expired.
            using var cleanupDeadline = new CancellationTokenSource(TimeSpan.FromSeconds(10));

            try
            {
                await WaitForLifecycleAsync(fixture.Transport.StopAsync(cleanupDeadline.Token), watchdog: TimeSpan.FromSeconds(15)).ConfigureAwait(false);
            }
            catch (Exception cleanup) when (failure is not null)
            {
                throw new AggregateException("Transport conformance and transport shutdown both failed.", failure, cleanup);
            }
        }

        return new(scenario);
    }

    private static async ValueTask WaitForLifecycleAsync(Task operation, CancellationToken token = default, TimeSpan? watchdog = null)
    {
        try
        {
            await operation.WaitAsync(watchdog ?? Timeout.InfiniteTimeSpan, token).ConfigureAwait(false);
        }
        catch (Exception error) when (error is OperationCanceledException or TimeoutException)
        {
            // The operation can fault as the wait ends or after cleanup has finished.
            ObserveLateFault(operation);
            throw;
        }
    }

    private static string? SkipReason(TransportConformanceCase scenario, TransportCapabilities capabilities)
    {
        return scenario switch
        {
            TransportConformanceCase.Retry when capabilities.MaxRedeliveryDelay is null => "No native redelivery.",
            TransportConformanceCase.DeadLetter when !capabilities.DeadLetter => "No native dead-lettering.",
            TransportConformanceCase.Delay when capabilities.MaxDeliveryDelay is null || capabilities.MaxDeliveryDelay <= TimeSpan.Zero => "No positive native delivery delay.",
            TransportConformanceCase.RedeliveryDelay when capabilities.MaxRedeliveryDelay is null || capabilities.MaxRedeliveryDelay <= TimeSpan.Zero => "No positive native redelivery delay.",
            TransportConformanceCase.OrderingByKey when !capabilities.OrderedByKey => "No native ordering by key.",
            _ => null,
        };
    }

    private static async Task RunScenarioAsync(TransportConformanceCase scenario, ITransportConformanceFixture<TIncoming> fixture,
        ChannelReader<Delivery> received, ConcurrentQueue<Activity> spans, Func<DateTimeOffset> redeliverAt,
        TaskCompletionSource orderRelease, ScenarioProgress progress, ShutdownProgress shutdown, TimeSpan outageSendTimeout,
        CancellationToken token)
    {
        WireString? defaultOrderingKey = fixture.DefaultOrderingKey;

        // Keep the shutdown key for ordered fixtures that have no fixture default.
        if (defaultOrderingKey is null && scenario == TransportConformanceCase.CleanShutdown &&
            fixture.Transport.GetCapabilities(fixture.Destination).OrderedByKey)
        {
            defaultOrderingKey = new WireString("conformance");
        }

        async Task<Delivery> Next(string phase = "receive completion")
        {
            progress.Phase = phase;
            return await received.ReadAsync(token).ConfigureAwait(false);
        }

        async Task Send(int value, bool creation = false, WireString? key = null, DateTimeOffset? at = null)
        {
            progress.Phase = $"send {value}";
            var body = Encoding.UTF8.GetBytes($"{{\"Value\":{value}}}");
            var headers = Headers(fixture.HeaderNames, creation);
            var message = new OutgoingMessage(fixture.Destination, body, headers, key ?? defaultOrderingKey, at);
            var send = fixture.Transport.SendAsync(message, token).AsTask();

            try
            {
                await send.WaitAsync(token).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (token.IsCancellationRequested)
            {
                // The wait can end while the adapter still borrows the arrays. Leave them intact
                // and observe any late fault from the send after the scenario exits.
                ObserveLateFault(send);
                throw;
            }
            // A native client must not retain the caller's borrowed buffers after SendAsync completes.
            Array.Clear(body);
            Array.Clear(headers);
        }

        async Task<bool> OutageSend(int value, CancellationToken outageToken)
        {
            var body = Encoding.UTF8.GetBytes($"{{\"Value\":{value}}}");

            try
            {
                var headers = Headers(fixture.HeaderNames, false);
                var message = new OutgoingMessage(fixture.Destination, body, headers, defaultOrderingKey);
                await fixture.Transport.SendAsync(message, outageToken).ConfigureAwait(false);
                return true;
            }
            catch (OperationCanceledException) when (outageToken.IsCancellationRequested && !token.IsCancellationRequested)
            {
                return false;
            }
            catch (Exception error) when (!token.IsCancellationRequested && fixture.IsExpectedOutageFailure(error))
            {
                return false;
            }
        }

        ValueTask Stop()
        {
            progress.Phase = "transport drain";
            return WaitForLifecycleAsync(fixture.Transport.StopAsync(token), token);
        }

        ValueTask Start()
        {
            progress.Phase = "transport restart";
            return WaitForLifecycleAsync(fixture.Transport.StartAsync(token), token);
        }

        switch (scenario)
        {
            case TransportConformanceCase.CleanShutdown:
                await Send(1).ConfigureAwait(false);
                CheckBody(await Next().ConfigureAwait(false), 1);
                progress.Phase = "in-flight transport shutdown";
                var stopping = fixture.Transport.StopAsync(token);
                Volatile.Write(ref shutdown.StopTask, stopping);
                // Observe at completion; the consumer also checks the task in case native
                // task options force this observer to run asynchronously.
                var observedStop = stopping.ContinueWith(static (completed, state) =>
                {
                    completed.GetAwaiter().GetResult();
                    var progress = (ShutdownProgress)state!;
                    Require(Volatile.Read(ref progress.ConsumerFinished) != 0 && Volatile.Read(ref progress.StopFinishedFirst) == 0,
                        "StopAsync completed before the in-flight consumer finished.");
                }, shutdown, CancellationToken.None, TaskContinuationOptions.ExecuteSynchronously, TaskScheduler.Default);
                orderRelease.TrySetResult();
                await WaitForLifecycleAsync(observedStop, token).ConfigureAwait(false);
                progress.Phase = "native shutdown verification";
                await WaitForLifecycleAsync(((ITransportShutdownConformanceFixture)fixture).VerifyShutdownAsync(token).AsTask(), token).ConfigureAwait(false);
                break;
            case TransportConformanceCase.SendReceiveSettle:
                await Send(1).ConfigureAwait(false);
                CheckBody(await Next().ConfigureAwait(false), 1);
                await Stop().ConfigureAwait(false);
                await Start().ConfigureAwait(false);
                await Send(2).ConfigureAwait(false);
                CheckBody(await Next().ConfigureAwait(false), 2);
                await Stop().ConfigureAwait(false);
                progress.Phase = "restart replay verification";

                while (received.TryRead(out var delivery))
                {
                    token.ThrowIfCancellationRequested();
                    Require(!HasBody(delivery, 1), "Original message replayed after the restart marker.");
                }

                progress.Phase = "native settlement verification";
                await WaitForLifecycleAsync(fixture.VerifySettlementAsync(token).AsTask(), token).ConfigureAwait(false);
                break;
            case TransportConformanceCase.FailureHandlingFailure:
                await Send(1, creation: true).ConfigureAwait(false);
                CheckBody(await Next().ConfigureAwait(false), 1);
                var recovered = await Next("redelivery after failure handling failed").ConfigureAwait(false);
                CheckBody(recovered, 1);
                CheckCreation(recovered);
                await Send(2).ConfigureAwait(false);
                CheckBody(await Next("healthy message after failed failure handling").ConfigureAwait(false), 2);
                break;
            case TransportConformanceCase.Retry:
            case TransportConformanceCase.RedeliveryDelay:
                await Send(1, creation: true).ConfigureAwait(false);
                var first = await Next().ConfigureAwait(false);
                var retry = await Next().ConfigureAwait(false);
                CheckBody(first, 1);
                CheckBody(retry, 1);
                CheckCreation(first);
                CheckCreation(retry);
                Require(first.Attempt is null || retry.Attempt > first.Attempt, "Native delivery count did not advance.");
                var retryDue = redeliverAt();
                if (retry.ReceivedAt < retryDue)
                {
                    Require(false, $"Native redelivery ran before its due time. Due: {retryDue:O}; received: {retry.ReceivedAt:O}; early by: {retryDue - retry.ReceivedAt}.");
                }

                break;
            case TransportConformanceCase.DeadLetter:
                await Send(1, creation: true).ConfigureAwait(false);
                CheckBody(await Next().ConfigureAwait(false), 1);
                progress.Phase = "dead-letter receive";
                var read = fixture.ReadDeadLetterAsync(token).AsTask();
                ConformanceDeadLetter dead;

                try
                {
                    dead = await read.WaitAsync(token).ConfigureAwait(false);
                }
                catch (OperationCanceledException) when (token.IsCancellationRequested)
                {
                    ObserveLateFault(read);
                    throw;
                }

                Require(Encoding.UTF8.GetString(dead.Body.Span) == "{\"Value\":1}", "Dead-letter body changed.");
                Require(dead.Reason == "conformance_poison" && dead.Description == "expected failure", "Dead-letter error metadata changed.");
                Require(dead.Headers.TryGetValue("traceparent", out var parent) && parent == TraceParent &&
                    dead.Headers.TryGetValue("tracestate", out var state) && state == TraceState, "Dead-letter creation context changed.");
                await Send(2).ConfigureAwait(false);
                CheckBody(await Next().ConfigureAwait(false), 2);
                break;
            case TransportConformanceCase.Delay:
                var due = DateTimeOffset.UtcNow + NativeDelay(fixture.Transport.GetCapabilities(fixture.Destination).MaxDeliveryDelay!.Value);
                await Send(1, at: due).ConfigureAwait(false);
                var delayed = await Next().ConfigureAwait(false);
                CheckBody(delayed, 1);
                Require(delayed.ReceivedAt >= due, "Scheduled delivery ran before its due time.");
                break;
            case TransportConformanceCase.OrderingByKey:
                for (var i = 0; i < 32; i++)
                {
                    await Send(i, key: new WireString("one-key")).ConfigureAwait(false);
                }
                // Settle as messages arrive: native send admission may depend on prior completion.
                for (var i = 0; i < 32; i++)
                {
                    CheckBody(await Next().ConfigureAwait(false), i);
                }

                break;
            case TransportConformanceCase.TracePropagation:
                await Send(1).ConfigureAwait(false);
                var delivered = await Next().ConfigureAwait(false);
                // Stop drains settlement and closes process spans before inspecting the source-local capture.
                await Stop().ConfigureAwait(false);
                progress.Phase = "trace validation";
                CheckSpans(fixture, spans, delivered);
                break;
            case TransportConformanceCase.CreationContextCopies:
                // The same serialized envelope represents an outbox dispatch, scheduled re-send and DLQ copy.
                // These are adapter boundary checks; durable outbox execution has its own store/worker tests.
                for (var i = 0; i < 3; i++)
                {
                    await Send(i, creation: true).ConfigureAwait(false);
                    var copy = await Next().ConfigureAwait(false);
                    CheckBody(copy, i);
                    CheckCreation(copy);
                }

                await Stop().ConfigureAwait(false);
                progress.Phase = "creation trace validation";
                var sends = spans.Where(span => Equals(span.GetTagItem("messaging.operation.type"), "send")).ToArray();
                Require(sends.Length == 3, "Expected one send span per stored-envelope copy.");
                foreach (var send in sends)
                {
                    Require(send.Kind == ActivityKind.Client, "A stored creation context must not create a producer span.");
                    Require(send.Links.Any(link => MatchesCreation(link.Context)), "Copy send span lost its creation link.");
                }

                break;
            case TransportConformanceCase.SendDuringOutage:
                const int OutageInitial = 0;
                const int OutageWarmup = 5;

                static int CheckOutageDelivery(Delivery message)
                {
                    using var document = JsonDocument.Parse(message.Body);
                    var value = document.RootElement.GetProperty("Value").GetInt32();
                    CheckBody(message, value);
                    Require(value is >= OutageInitial and <= OutageWarmup, "Unexpected outage delivery body.");
                    return value;
                }

                await Send(OutageInitial).ConfigureAwait(false);
                CheckBody(await Next().ConfigureAwait(false), OutageInitial);
                await Stop().ConfigureAwait(false);
                await Start().ConfigureAwait(false);
                // Native Start may return before its stream is established. An initial
                // replay cannot prove that the restarted receiver delivered the warm-up.
                await Send(OutageWarmup).ConfigureAwait(false);
                Delivery outageWarmup;

                do
                {
                    outageWarmup = await Next("outage warm-up receive").ConfigureAwait(false);
                }
                while (HasBody(outageWarmup, OutageInitial));

                CheckBody(outageWarmup, OutageWarmup);
                progress.Phase = "broker disconnect";
                await WaitForLifecycleAsync(fixture.DisconnectAsync(token), token).ConfigureAwait(false);
                progress.Phase = "outage sends";
                using (var outage = CancellationTokenSource.CreateLinkedTokenSource(token))
                {
                    outage.CancelAfter(outageSendTimeout);
                    var pending = Enumerable.Range(1, 3).Select(value => OutageSend(value, outage.Token)).ToArray();
                    var allSends = Task.WhenAll(pending);
                    bool[] acknowledged;

                    try
                    {
                        acknowledged = await allSends.WaitAsync(outageSendTimeout + s_outageWatchdogGrace, token).ConfigureAwait(false);
                    }
                    catch (Exception error) when (error is OperationCanceledException or TimeoutException)
                    {
                        // The wait can end while sends still borrow their buffers.
                        // Observe faults on the aggregate without delaying shutdown or disposal.
                        ObserveLateFault(allSends);
                        throw;
                    }

                    progress.Phase = "offline shutdown";
                    using var offlineShutdown = new CancellationTokenSource(TimeSpan.FromSeconds(10));

                    try
                    {
                        await WaitForLifecycleAsync(fixture.Transport.StopAsync(offlineShutdown.Token), token, TimeSpan.FromSeconds(15)).ConfigureAwait(false);
                    }
                    catch (OperationCanceledException) when (offlineShutdown.IsCancellationRequested && !token.IsCancellationRequested)
                    {
                    }
                    // Some native clients fault permanently on a disconnected stream's initial write.
                    // The fixture must observe that exact native failure and recreate its application
                    // on reconnect; acknowledged-message checks below still apply to the same endpoint.
                    catch (Exception error) when (!token.IsCancellationRequested && fixture.IsExpectedOutageFailure(error))
                    {
                    }

                    progress.Phase = "broker reconnect";
                    await WaitForLifecycleAsync(fixture.ReconnectAsync(token), token).ConfigureAwait(false);
                    // An offline stop can exhaust its budget while native drain is still
                    // running. Finish that shutdown with connectivity restored before
                    // Start can treat the still-started transport as an idempotent no-op.
                    await Stop().ConfigureAwait(false);
                    await Start().ConfigureAwait(false);
                    await Send(4).ConfigureAwait(false);
                    // Failed or cancelled native sends and replays of either pre-outage marker may still arrive.
                    // Every successfully acknowledged send and the recovery marker must arrive.
                    var required = new HashSet<int> { 4 };

                    for (var i = 0; i < acknowledged.Length; i++)
                    {
                        if (acknowledged[i])
                        {
                            required.Add(i + 1);
                        }
                    }

                    while (required.Count != 0)
                    {
                        var message = await Next($"outage recovery receive; outstanding values: {string.Join(", ", required)}").ConfigureAwait(false);
                        required.Remove(CheckOutageDelivery(message));
                    }
                }

                await Stop().ConfigureAwait(false);
                progress.Phase = "outage drain verification";

                while (received.TryRead(out var drainedDelivery))
                {
                    token.ThrowIfCancellationRequested();
                    CheckOutageDelivery(drainedDelivery);
                }

                break;
            case TransportConformanceCase.Reconnect:
                const int Initial = 1;
                const int Warmup = 2;
                const int Recovery = 3;

                await Send(Initial).ConfigureAwait(false);
                CheckBody(await Next().ConfigureAwait(false), Initial);
                await Stop().ConfigureAwait(false);
                await Start().ConfigureAwait(false);
                await Send(Warmup).ConfigureAwait(false);
                Delivery warmupDelivery;

                // A replay of the initial marker does not prove the restarted receiver delivered the warm-up.
                do
                {
                    warmupDelivery = await Next("reconnect warm-up receive").ConfigureAwait(false);
                }
                while (HasBody(warmupDelivery, Initial));

                CheckBody(warmupDelivery, Warmup);
                progress.Phase = "broker disconnect";
                await WaitForLifecycleAsync(fixture.DisconnectAsync(token), token).ConfigureAwait(false);
                progress.Phase = "broker reconnect";
                await WaitForLifecycleAsync(fixture.ReconnectAsync(token), token).ConfigureAwait(false);
                await Send(Recovery).ConfigureAwait(false);
                Delivery recoveredDelivery;

                // The broker may replay either pre-outage marker during recovery.
                do
                {
                    recoveredDelivery = await Next("reconnect recovery receive").ConfigureAwait(false);
                }
                while (HasBody(recoveredDelivery, Initial) || HasBody(recoveredDelivery, Warmup));

                CheckBody(recoveredDelivery, Recovery);
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(scenario));
        }
    }

    private static void CheckSpans(ITransportConformanceFixture<TIncoming> fixture, ConcurrentQueue<Activity> spans, Delivery delivery)
    {
        var all = spans.ToArray();
        Require(!all.Any(span => Equals(span.GetTagItem("messaging.operation.type"), "receive")), "Relayer must not emit internal receive spans.");
        var send = Single("send");
        var process = Single("process");
        var settlements = all.Where(span => Equals(span.GetTagItem("messaging.operation.type"), "settle")).ToArray();
        Require(settlements.Length > 0, "Expected native settlement spans.");
        Require(send.Kind == ActivityKind.Producer && process.Kind == ActivityKind.Consumer, "Messaging span kinds changed.");
        Require(send.OperationName == $"send {fixture.SendDestinationName}" && process.OperationName == $"process {fixture.ProcessDestinationName}", "Messaging span names changed.");
        Require(ActivityContext.TryParse(delivery.TraceParent, delivery.TraceState, out var creation) && creation.TraceId == send.TraceId && creation.SpanId == send.SpanId,
            "Native instrumentation overwrote the producer creation context.");
        Require(process.Links.Any(link => link.Context.TraceId == send.TraceId && link.Context.SpanId == send.SpanId), "Process span lost its creation link.");

        foreach (var settle in settlements)
        {
            Require(settle.Kind == ActivityKind.Client && settle.OperationName == $"{fixture.SettlementOperation} {fixture.ProcessDestinationName}", "Native settlement span name or kind changed.");

            if (fixture.Transport.GetCapabilities(fixture.Destination).ConsumptionModel == ConsumptionModel.Queue)
            {
                Require(settle.ParentSpanId == process.SpanId && settle.TraceId == process.TraceId, "Native queue settlement must run inside the process span.");
            }
            // Log commits may be amortized outside the process span (D13).
        }

        foreach (var span in all)
        {
            Require(Equals(span.GetTagItem("messaging.system"), fixture.MessagingSystem), "Unexpected messaging.system.");
        }

        Activity Single(string operation)
        {
            var matches = all.Where(span => Equals(span.GetTagItem("messaging.operation.type"), operation)).ToArray();
            Require(matches.Length == 1, $"Expected one {operation} span, found {matches.Length}.");
            return matches[0];
        }
    }

    private static KeyValuePair<string, WireString>[] Headers(HeaderNames names, bool creation)
    {
        List<KeyValuePair<string, WireString>> headers =
        [
            new(names.Id, new WireString(Guid.CreateVersion7().ToString("N"))),
            new(names.Type, new WireString("relayer.conformance;v=1")),
            new(names.Source, new WireString("/relayer/conformance")),
            new(names.SpecVersion, new WireString("1.0")),
            new("conformance", new WireString("owned")),
        ];

        if (creation)
        {
            headers.Add(new(names.TraceParent, new WireString(TraceParent)));
            headers.Add(new(names.TraceState, new WireString(TraceState)));
        }

        return headers.ToArray();
    }

    private static string? Read(TIncoming incoming, string name)
    {
        return TBinding.TryGetHeader(incoming, name, out var value) ? value.ToString() : null;
    }

    private static TimeSpan NativeDelay(TimeSpan maximum)
    {
        return maximum < s_delay ? maximum : s_delay;
    }

    private static async ValueTask<Disposition> WaitForOrderAsync(Task release, CancellationToken token)
    {
        await release.WaitAsync(token).ConfigureAwait(false);
        return Disposition.Complete;
    }

    private static bool MatchesCreation(ActivityContext context)
    {
        return context.TraceId.ToString() == "0123456789abcdef0123456789abcdef" && context.SpanId.ToString() == "0123456789abcdef";
    }

    private static void CheckCreation(Delivery delivery)
    {
        Require(delivery.TraceParent == TraceParent && delivery.TraceState == TraceState, "Stored creation context changed.");
    }

    private static void CheckBody(Delivery delivery, int value)
    {
        Require(HasBody(delivery, value), "Native body or delivery order changed.");
    }

    private static bool HasBody(Delivery delivery, int value)
    {
        return Encoding.UTF8.GetString(delivery.Body) == $"{{\"Value\":{value}}}";
    }

    private static void Require(bool condition, string message)
    {
        if (!condition)
        {
            throw new InvalidOperationException($"Transport conformance: {message}");
        }
    }

    private sealed record Delivery(byte[] Body, string? TraceParent, string? TraceState, int? Attempt, DateTimeOffset ReceivedAt);

    private sealed class ShutdownProgress
    {
        internal int ConsumerFinished;
        internal int StopFinishedFirst;
        internal Task? StopTask;
    }

    private sealed class ScenarioProgress
    {
        internal string Phase
        {
            get; set;
        } = "scenario setup";
    }
}
