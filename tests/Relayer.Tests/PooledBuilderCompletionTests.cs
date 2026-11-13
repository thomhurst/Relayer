using Relayer.TestFixtures;

namespace Relayer.Tests;

public class PooledBuilderCompletionTests
{
    [Test]
    [Arguments(PooledBuilderCompletionFixture.Mode.Owned)]
    [Arguments(PooledBuilderCompletionFixture.Mode.PooledVoid)]
    [Arguments(PooledBuilderCompletionFixture.Mode.PooledResult)]
    public async Task Overlapping_operations_restore_ambient_context_and_can_be_immediately_reused(PooledBuilderCompletionFixture.Mode mode)
    {
        using var fixture = new PooledBuilderCompletionFixture(64, true, true);
        for (var i = 0; i < 20; i++)
        {
            fixture.Run(mode);
            await Assert.That(fixture.ContextRestored).IsTrue();
        }
    }

    [Test]
    [Arguments(PooledBuilderCompletionFixture.Mode.Owned)]
    [Arguments(PooledBuilderCompletionFixture.Mode.PooledVoid)]
    [Arguments(PooledBuilderCompletionFixture.Mode.PooledResult)]
    public async Task Faults_and_cancellation_preserve_identity_and_allow_reuse(PooledBuilderCompletionFixture.Mode mode)
    {
        using var fixture = new PooledBuilderCompletionFixture(64, true, true);
        var failure = new InvalidOperationException("native failure");
        Exception? observed = null;
        try { fixture.Run(mode, failure); }
        catch (Exception error) { observed = error; }
        await Assert.That(observed).IsSameReferenceAs(failure);
        using var cancellation = new CancellationTokenSource();
        cancellation.Cancel();
        var cancelled = new OperationCanceledException(cancellation.Token);
        try { fixture.Run(mode, cancelled); }
        catch (Exception error) { observed = error; }
        await Assert.That(observed).IsSameReferenceAs(cancelled);
        await Assert.That(((OperationCanceledException)observed!).CancellationToken).IsEqualTo(cancellation.Token);
        fixture.Run(mode);
        await Assert.That(fixture.ContextRestored).IsTrue();
    }

    [Test]
    public async Task Owned_completion_retains_zero_allocation_storage_across_worker_changes()
    {
        using var fixture = new PooledBuilderCompletionFixture(64, true, true);
        for (var i = 0; i < 100; i++) fixture.Run(PooledBuilderCompletionFixture.Mode.Owned);
        long bytes = 0;
        for (var i = 0; i < 100; i++) bytes += fixture.Run(PooledBuilderCompletionFixture.Mode.Owned);
        await Assert.That(bytes).IsEqualTo(0);
    }
}
