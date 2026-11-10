namespace Relayer.Tests;

public sealed class ServiceBusStackTests
{
    [Test]
    [Arguments(false, false)]
    [Arguments(false, true)]
    [Arguments(true, false)]
    [Arguments(true, true)]
    public async Task Startup_failure_survives_cleanup(bool provisioningExpired, bool cleanupFails)
    {
        using var provisioning = new CancellationTokenSource();
        if (provisioningExpired) await provisioning.CancelAsync();
        Exception original = provisioningExpired
            ? new OperationCanceledException(provisioning.Token)
            : new InvalidOperationException("startup failed");
        const string report = "original startup diagnostics";
        original.Data[ServiceBusStartupDiagnostics.EvidenceKey] = report;
        var cleanupError = new InvalidOperationException("cleanup failed");
        var cleanupAttempted = false;
        Exception? reportedError = null;
        Exception? failure = null;

        try
        {
            await ServiceBusStack.StartWithCleanupAsync(FailStartupAsync, CleanupAsync,
                provisioning.Token, error => reportedError = error);
        }
        catch (Exception error) { failure = error; }

        await Assert.That(cleanupAttempted).IsTrue();
        await Assert.That(ReferenceEquals(reportedError, cleanupFails ? cleanupError : null)).IsTrue();
        if (provisioningExpired)
        {
            await Assert.That(failure).IsTypeOf<TimeoutException>();
            await Assert.That(failure!.Message).Contains("provisioning exceeded 15 minutes");
            await Assert.That(ReferenceEquals(failure.InnerException, original)).IsTrue();
        }
        else
        {
            await Assert.That(ReferenceEquals(failure, original)).IsTrue();
        }
        await Assert.That(original.StackTrace!).Contains(nameof(FailStartupAsync));
        await Assert.That((string)original.Data[ServiceBusStartupDiagnostics.EvidenceKey]!).IsEqualTo(report);

        Task FailStartupAsync() => throw original;
        ValueTask CleanupAsync()
        {
            cleanupAttempted = true;
            return cleanupFails ? ValueTask.FromException(cleanupError) : ValueTask.CompletedTask;
        }
    }

    [Test]
    public async Task Broken_cleanup_reporting_preserves_startup_failure()
    {
        var original = new InvalidOperationException("startup failed");
        Exception? failure = null;
        try
        {
            await ServiceBusStack.StartWithCleanupAsync(() => Task.FromException(original),
                () => ValueTask.FromException(new InvalidOperationException("cleanup failed")),
                CancellationToken.None, _ => throw new IOException("output unavailable"));
        }
        catch (Exception error) { failure = error; }
        await Assert.That(ReferenceEquals(failure, original)).IsTrue();
    }

    [Test]
    public async Task Successful_startup_does_not_cleanup_or_report()
    {
        await ServiceBusStack.StartWithCleanupAsync(() => Task.CompletedTask,
            () => throw new InvalidOperationException("unexpected cleanup"), CancellationToken.None,
            _ => throw new InvalidOperationException("unexpected report"));
    }
}
