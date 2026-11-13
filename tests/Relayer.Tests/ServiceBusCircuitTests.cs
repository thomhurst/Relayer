using System.Collections.Concurrent;
using System.Diagnostics.Metrics;
using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.DependencyInjection;
using Relayer.AzureServiceBus;

namespace Relayer.Tests;

public sealed class ServiceBusCircuitTests
{
    [Test]
    public async Task Recovery_start_failure_remains_visible_to_health_and_shutdown()
    {
        using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(20));
        var token = deadline.Token;
        var clock = new ConsumerCircuitTests.Clock();
        var client = new ServiceBusReceiveTests.NativeClient();
        var provider = ServiceBusReceiveTests.Provider(client,
            consumer =>
            {
                consumer.Failures(FailureOptions.None);
                consumer.Options.CircuitBreaker = new() { MinimumThroughput = 1 };
            }, health: true, time: clock);
        try
        {
            await ServiceBusReceiveTests.Start(provider, token);
            var processor = client.Processors.Single();
            await processor.Lifecycle.Reader.ReadAsync(token);
            provider.GetRequiredService<ServiceBusReceiveTests.Control>().Handle = (_, _, _) =>
                ValueTask.FromException(new InvalidOperationException("offline"));
            await processor.Emit(ServiceBusReceiveTests.Args(new()));
            await Assert.That(await processor.Lifecycle.Reader.ReadAsync(token)).IsEqualTo("stop");
            processor.StartWork = () => Task.FromException(new InvalidOperationException("restart failed"));
            clock.Recovery.Fire();
            await Assert.That(await processor.Lifecycle.Reader.ReadAsync(token)).IsEqualTo("start");
            // Await the faulted control task deterministically before checking its retained health failure.
            await Assert.That(async () => await ServiceBusReceiveTests.Stop(provider, token)).Throws<InvalidOperationException>();
            var health = provider.GetRequiredKeyedService<Relayer.HealthChecks.IConsumerHealthProvider>("bus");
            var declaration = provider.GetRequiredService<RelayerConfiguration>().Transports.Single().Consumers.Single();
            await Assert.That(async () => await health.GetStateAsync(declaration, token)).Throws<InvalidOperationException>();
            await Assert.That(async () => await ServiceBusReceiveTests.Start(provider, token)).Throws<InvalidOperationException>();
        }
        finally
        {
            await Assert.That(async () => await provider.DisposeAsync()).Throws<InvalidOperationException>();
        }
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task Concurrent_failures_are_retained_and_only_one_probe_runs_after_native_drain(bool sessions)
    {
        using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(20));
        var token = deadline.Token;
        var clock = new ConsumerCircuitTests.Clock();
        var client = new ServiceBusReceiveTests.NativeClient();
        await using var provider = ServiceBusReceiveTests.Provider(client, consumer =>
        {
            consumer.Failures(FailureOptions.None);
            consumer.Concurrency(2);
            consumer.Options.CircuitBreaker = new() { MinimumThroughput = 1, FailureRatio = 1 };
            if (sessions) consumer.Options.SessionProcessorOptions = new()
            { AutoCompleteMessages = false, MaxConcurrentSessions = 1, MaxConcurrentCallsPerSession = 2 };
            else consumer.Options.ProcessorOptions.MaxConcurrentCalls = 2;
        }, sessions: sessions, time: clock);
        var instrumentation = provider.GetRequiredService<RelayerInstrumentation>();
        var states = new ConcurrentQueue<int>();
        using var listener = new MeterListener();
        listener.InstrumentPublished = (instrument, meterListener) =>
        {
            if (ReferenceEquals(instrument, instrumentation.CircuitState)) meterListener.EnableMeasurementEvents(instrument);
        };
        listener.SetMeasurementEventCallback<int>((_, state, _, _) => states.Enqueue(state));
        listener.Start();
        await ServiceBusReceiveTests.Start(provider, token);
        var lifecycle = sessions ? client.SessionProcessors.Single().Lifecycle : client.Processors.Single().Lifecycle;
        await Assert.That(await lifecycle.Reader.ReadAsync(token)).IsEqualTo("start");
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var probeEntered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var probeRelease = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var calls = 0;
        provider.GetRequiredService<ServiceBusReceiveTests.Control>().Handle = async (message, _, _) =>
        {
            Interlocked.Increment(ref calls);
            if (message.Value == 2) { entered.SetResult(); await release.Task; }
            if (message.Value == 3) { probeEntered.SetResult(); await probeRelease.Task; return; }
            throw new InvalidOperationException("dependency unavailable");
        };
        var drain = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        if (sessions) client.SessionProcessors.Single().Drain = drain.Task;
        else client.Processors.Single().Drain = drain.Task;
        var lateReceiver = new ServiceBusReceiveTests.NativeReceiver();
        var lateSession = new ServiceBusReceiveTests.NativeSessionReceiver();
        var late = Emit(lateReceiver, lateSession, 2);
        await entered.Task.WaitAsync(token);
        var thresholdReceiver = new ServiceBusReceiveTests.NativeReceiver();
        var thresholdSession = new ServiceBusReceiveTests.NativeSessionReceiver();
        await Emit(thresholdReceiver, thresholdSession, 1);
        await Assert.That(await lifecycle.Reader.ReadAsync(token)).IsEqualTo("stop");
        release.SetResult();
        await late.WaitAsync(token);
        await Assert.That(sessions ? thresholdSession.Abandons : thresholdReceiver.Operations.Count(op => op == "abandon")).IsEqualTo(1);
        await Assert.That(sessions ? lateSession.Abandons : lateReceiver.Operations.Count(op => op == "abandon")).IsEqualTo(1);
        var rejected = new ServiceBusReceiveTests.NativeReceiver();
        var rejectedSession = new ServiceBusReceiveTests.NativeSessionReceiver();
        await Emit(rejected, rejectedSession, 9);
        await Assert.That(calls).IsEqualTo(2);
        await Assert.That(rejected.Operations.Count + rejectedSession.Abandons + rejectedSession.Completions + rejectedSession.DeadLetters).IsEqualTo(0);
        clock.Recovery.Fire(); // Probe offered during drain must not start a second native lifecycle operation.
        await Assert.That(lifecycle.Reader.TryRead(out _)).IsFalse();
        drain.SetResult();
        await Assert.That(await lifecycle.Reader.ReadAsync(token)).IsEqualTo("start");
        var successful = new ServiceBusReceiveTests.NativeReceiver();
        var successfulSession = new ServiceBusReceiveTests.NativeSessionReceiver();
        var probe = Emit(successful, successfulSession, 3);
        await probeEntered.Task.WaitAsync(token);
        await Task.WhenAll(Enumerable.Range(0, 10).Select(_ => Emit(new(), new(), 9)));
        await Assert.That(calls).IsEqualTo(3);
        probeRelease.SetResult();
        await probe.WaitAsync(token);
        await Assert.That(sessions ? successfulSession.Completions : successful.Operations.Count(op => op == "complete")).IsEqualTo(1);
        await Assert.That(states.ToArray()).IsEquivalentTo(new[] { 0, 1, 2, 0 });
        await ServiceBusReceiveTests.Stop(provider, token);
        await Assert.That(await lifecycle.Reader.ReadAsync(token)).IsEqualTo("stop");
        clock.Recovery.Fire();
        await Assert.That(lifecycle.Reader.TryRead(out _)).IsFalse();
        await Assert.That(thresholdSession.DeadLetters + lateSession.DeadLetters).IsEqualTo(0);

        Task Emit(ServiceBusReceiveTests.NativeReceiver receiver, ServiceBusReceiveTests.NativeSessionReceiver sessionReceiver, int value)
        {
            var args = ServiceBusReceiveTests.Args(receiver, value);
            return sessions ? client.SessionProcessors.Single().Emit(new(args.Message, sessionReceiver, "circuit", default))
                : client.Processors.Single().Emit(args);
        }
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task Failed_probe_or_settlement_reopens_and_next_probe_can_recover(bool settlementFails)
    {
        using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(20));
        var token = deadline.Token;
        var clock = new ConsumerCircuitTests.Clock();
        var client = new ServiceBusReceiveTests.NativeClient();
        await using var provider = ServiceBusReceiveTests.Provider(client,
            consumer =>
            {
                consumer.Failures(FailureOptions.None);
                consumer.Options.CircuitBreaker = new() { MinimumThroughput = 1 };
            }, time: clock);
        var control = provider.GetRequiredService<ServiceBusReceiveTests.Control>();
        control.Handle = (_, _, _) => ValueTask.FromException(new InvalidOperationException("offline"));
        await ServiceBusReceiveTests.Start(provider, token);
        var processor = client.Processors.Single();
        await processor.Lifecycle.Reader.ReadAsync(token);
        var receiver = new ServiceBusReceiveTests.NativeReceiver();
        await processor.Emit(ServiceBusReceiveTests.Args(receiver));
        await Assert.That(await processor.Lifecycle.Reader.ReadAsync(token)).IsEqualTo("stop");
        clock.Recovery.Fire();
        await Assert.That(await processor.Lifecycle.Reader.ReadAsync(token)).IsEqualTo("start");
        if (settlementFails)
        {
            control.Handle = (_, _, _) => ValueTask.CompletedTask;
            receiver.Failure = new InvalidOperationException("settlement unavailable");
            await Assert.That(async () => await processor.Emit(ServiceBusReceiveTests.Args(receiver))).Throws<InvalidOperationException>();
        }
        else await processor.Emit(ServiceBusReceiveTests.Args(receiver));
        await Assert.That(await processor.Lifecycle.Reader.ReadAsync(token)).IsEqualTo("stop");
        clock.Recovery.Fire();
        await Assert.That(await processor.Lifecycle.Reader.ReadAsync(token)).IsEqualTo("start");
        control.Handle = (_, _, _) => ValueTask.CompletedTask;
        receiver.Failure = null;
        await processor.Emit(ServiceBusReceiveTests.Args(receiver));
        await Assert.That(receiver.Operations.Any(op => op == "dead_letter")).IsFalse();
        await ServiceBusReceiveTests.Stop(provider, token);
    }

    [Test]
    public async Task In_process_retries_count_once_and_a_success_can_cross_the_sampling_threshold()
    {
        using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(20));
        var token = deadline.Token;
        var client = new ServiceBusReceiveTests.NativeClient();
        await using var provider = ServiceBusReceiveTests.Provider(client, consumer =>
        {
            consumer.Options.CircuitBreaker = new() { MinimumThroughput = 3, FailureRatio = .5 };
            consumer.Failures(new() { RetryDelays = new[] { TimeSpan.Zero } });
        });
        await ServiceBusReceiveTests.Start(provider, token);
        var processor = client.Processors.Single();
        await processor.Lifecycle.Reader.ReadAsync(token);
        var calls = 0;
        provider.GetRequiredService<ServiceBusReceiveTests.Control>().Handle = (_, _, _) =>
        {
            calls++;
            return calls <= 4 ? ValueTask.FromException(new InvalidOperationException("offline")) : ValueTask.CompletedTask;
        };
        var receiver = new ServiceBusReceiveTests.NativeReceiver();
        await processor.Emit(ServiceBusReceiveTests.Args(receiver));
        await processor.Emit(ServiceBusReceiveTests.Args(receiver));
        await Assert.That(processor.Lifecycle.Reader.TryRead(out _)).IsFalse();
        await Assert.That(calls).IsEqualTo(4);
        await processor.Emit(ServiceBusReceiveTests.Args(receiver));
        await Assert.That(await processor.Lifecycle.Reader.ReadAsync(token)).IsEqualTo("stop");
        await Assert.That(receiver.Operations.ToArray()).IsEquivalentTo(new[] { "dead_letter", "dead_letter", "complete" });
        await ServiceBusReceiveTests.Stop(provider, token);
    }

    [Test]
    public async Task Circuit_opened_during_native_start_still_drains_and_shutdown_prevents_pending_recovery()
    {
        using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(20));
        var token = deadline.Token;
        var clock = new ConsumerCircuitTests.Clock();
        var client = new ServiceBusReceiveTests.NativeClient();
        var drain = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        client.OnStart = processor =>
        {
            processor.Drain = drain.Task;
            return processor.Emit(ServiceBusReceiveTests.Args(new()));
        };
        await using var provider = ServiceBusReceiveTests.Provider(client,
            consumer =>
            {
                consumer.Failures(FailureOptions.None);
                consumer.Options.CircuitBreaker = new() { MinimumThroughput = 1 };
            }, time: clock);
        provider.GetRequiredService<ServiceBusReceiveTests.Control>().Handle = (_, _, _) =>
            ValueTask.FromException(new InvalidOperationException("offline"));
        await ServiceBusReceiveTests.Start(provider, token);
        var processor = client.Processors.Single();
        await Assert.That(await processor.Lifecycle.Reader.ReadAsync(token)).IsEqualTo("start");
        await Assert.That(await processor.Lifecycle.Reader.ReadAsync(token)).IsEqualTo("stop");
        clock.Recovery.Fire();
        var stopping = ServiceBusReceiveTests.Stop(provider, token);
        await Assert.That(stopping.IsCompleted).IsFalse();
        drain.SetResult();
        await stopping.WaitAsync(token);
        await Assert.That(processor.Lifecycle.Reader.TryRead(out _)).IsFalse();
    }

    [Test]
    [Arguments(0, 1, 1000, 1000)]
    [Arguments(1.1, 1, 1000, 1000)]
    [Arguments(.5, 0, 1000, 1000)]
    [Arguments(.5, 1, 0, 1000)]
    [Arguments(.5, 1, 1000, 0)]
    public async Task Invalid_circuit_configuration_fails_before_native_processor_creation(double ratio, int minimum, int sampling, int duration)
    {
        var client = new ServiceBusReceiveTests.NativeClient();
        await using var provider = ServiceBusReceiveTests.Provider(client, consumer => consumer.Options.CircuitBreaker = new()
        { FailureRatio = ratio, MinimumThroughput = minimum, SamplingDuration = TimeSpan.FromMilliseconds(sampling), BreakDuration = TimeSpan.FromMilliseconds(duration) });
        await Assert.That(async () => await ServiceBusReceiveTests.Start(provider)).Throws<ArgumentOutOfRangeException>();
        await Assert.That(client.Processors.Count).IsEqualTo(0);
    }
}
