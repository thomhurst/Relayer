using Relayer.InMemory;
using Relayer.Testing;
using Relayer.Transports;

namespace Relayer.Conformance.Tests;

public class ConformanceFailureTests
{
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
        if (stopFails) await Assert.That(failures).Contains(fixture.StopFailure);
        if (disposeFails) await Assert.That(failures).Contains(fixture.DisposeFailure);
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
    public async Task Failed_start_is_reported_and_resources_are_cleaned_up()
    {
        var fixture = new FailingFixture { StartFails = true };
        var error = await CaptureAsync(Suite(fixture).RunAsync(TransportConformanceCase.SendReceiveSettle));
        await Assert.That(error.Message).Contains("transport start");
        await Assert.That(error.InnerException).IsSameReferenceAs(fixture.PrimaryFailure);
        await Assert.That(fixture.StoppedWithLiveToken).IsTrue();
        await Assert.That(fixture.Disposed).IsTrue();
    }

    private static TransportConformanceSuite<InMemoryDelivery, InMemoryMessageBinding> Suite(FailingFixture fixture) =>
        new((_, _) => new ValueTask<ITransportConformanceFixture<InMemoryDelivery>>(fixture));

    private static async Task<Exception> CaptureAsync(Task task)
    {
        try { await task; }
        catch (Exception error) { return error; }
        throw new InvalidOperationException("Expected conformance failure.");
    }

    private sealed class FailingFixture : ITransportConformanceFixture<InMemoryDelivery>, ITransport
    {
        internal Exception PrimaryFailure { get; } = new IOException("native operation failed");
        internal Exception StopFailure { get; } = new IOException("native shutdown failed");
        internal Exception DisposeFailure { get; } = new IOException("fixture disposal failed");
        internal bool StopFails { get; init; }
        internal bool DisposeFails { get; init; }
        internal bool StartFails { get; init; }
        internal CancellationTokenSource? CancelAfterSend { get; init; }
        internal bool StoppedWithLiveToken { get; private set; }
        internal bool Disposed { get; private set; }
        public ITransport Transport => this;
        public string Name => "failing";
        public string Destination => "isolated";
        public string MessagingSystem => "relayer.inmemory";
        public string SettlementOperation => "complete";
        public HeaderNames HeaderNames { get; } = new();
        public TransportCapabilities GetCapabilities(string endpoint) => new();
        public void RegisterConsumer(Func<InMemoryDelivery, CancellationToken, ValueTask<Disposition>> receive) { }
        public Task StartAsync(CancellationToken cancellationToken) => StartFails ? Task.FromException(PrimaryFailure) : Task.CompletedTask;
        public Task StopAsync(CancellationToken cancellationToken)
        {
            StoppedWithLiveToken = cancellationToken.CanBeCanceled && !cancellationToken.IsCancellationRequested;
            return StopFails ? Task.FromException(StopFailure) : Task.CompletedTask;
        }
        public ValueTask SendAsync(OutgoingMessage message, CancellationToken cancellationToken)
        {
            if (CancelAfterSend is null) return ValueTask.FromException(PrimaryFailure);
            CancelAfterSend.Cancel();
            return default;
        }
        public ValueTask DisposeAsync()
        {
            Disposed = true;
            return DisposeFails ? ValueTask.FromException(DisposeFailure) : default;
        }
        public Task DisconnectAsync(CancellationToken cancellationToken) => throw new NotSupportedException();
        public Task ReconnectAsync(CancellationToken cancellationToken) => throw new NotSupportedException();
        public ValueTask<TopologyReport> ApplyTopologyAsync(TopologyMode mode, CancellationToken cancellationToken) => throw new NotSupportedException();
        public ValueTask SendBatchAsync(ReadOnlyMemory<OutgoingMessage> messages, CancellationToken cancellationToken) => throw new NotSupportedException();
        public ValueTask SettleAsync(InMemoryDelivery incoming, Disposition disposition, CancellationToken cancellationToken) => throw new NotSupportedException();
        public ValueTask<ConformanceDeadLetter> ReadDeadLetterAsync(CancellationToken cancellationToken) => throw new NotSupportedException();
    }
}
