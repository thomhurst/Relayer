using Relayer.Transports;

namespace Relayer.Tests;

public class ConsumerHostedServiceTests
{
    [Test]
    public async Task Host_shutdown_that_bypasses_the_gate_cancels_a_control_start_in_progress()
    {
        var service = new BlockingStartService();
        await service.StartAsync(default);
        var start = ((IConsumerControl)service).StartAsync(default);
        var startToken = await service.Starting.Task;

        // The control start holds the gate, so an expired shutdown token takes the bypass path.
        using var expired = new CancellationTokenSource();
        await expired.CancelAsync();
        await service.StopAsync(expired.Token);

        await Assert.That(startToken.IsCancellationRequested).IsTrue();
        await Assert.That(async () => await start).Throws<OperationCanceledException>();
        await Assert.That(service.Stops).IsEqualTo(1);
    }

    [Test]
    public async Task Control_start_after_a_host_restart_is_not_cancelled_by_the_earlier_shutdown()
    {
        var service = new BlockingStartService();
        await service.StartAsync(default);
        await service.StopAsync(default);
        await service.StartAsync(default);

        var start = ((IConsumerControl)service).StartAsync(default);
        var startToken = await service.Starting.Task;
        await Assert.That(startToken.IsCancellationRequested).IsFalse();
        service.Release.SetResult();
        await start;
        await service.StopAsync(default);
    }

    private sealed class BlockingStartService() : ConsumerHostedService("test")
    {
        public TaskCompletionSource<CancellationToken> Starting
        {
            get;
        } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public TaskCompletionSource Release
        {
            get;
        } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public int Stops
        {
            get; private set;
        }

        protected override bool AutoStartConsumers => false;

        protected override Task ComposeAsync(CancellationToken cancellationToken)
        {
            return Task.CompletedTask;
        }

        protected override Task StartConsumersAsync(CancellationToken cancellationToken)
        {
            Starting.TrySetResult(cancellationToken);
            return Release.Task.WaitAsync(cancellationToken);
        }

        protected override Task StopConsumersAsync(CancellationToken cancellationToken)
        {
            Stops++;
            return Task.CompletedTask;
        }
    }
}
