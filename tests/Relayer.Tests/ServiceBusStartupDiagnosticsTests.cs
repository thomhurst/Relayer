using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;

namespace Relayer.Tests;

public sealed class ServiceBusStartupDiagnosticsTests
{
    [Test]
    [Arguments(true)]
    [Arguments(false)]
    public async Task Failed_dependency_or_emulator_retains_live_Docker_state_and_logs_before_cleanup(bool failDatabase)
    {
        await using var database = Container(failDatabase);
        await using var emulator = Container(!failDatabase);
        using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(2));
        Exception? failure = null;
        Exception? startupError = null;
        try
        {
            await ServiceBusStartupDiagnostics.StartAsync(async token =>
            {
                try
                {
                    await database.StartAsync(token);
                    await emulator.StartAsync(token);
                }
                catch (Exception error)
                {
                    startupError = error;
                    // Model an expired provisioning budget before diagnostic capture begins.
                    await timeout.CancelAsync();
                    throw;
                }
            }, database, emulator, timeout.Token);
        }
        catch (Exception error) { failure = error; }

        await Assert.That(failure).IsNotNull();
        await Assert.That(ReferenceEquals(failure, startupError)).IsTrue();
        var evidence = (string)failure!.Data[ServiceBusStartupDiagnostics.EvidenceKey]!;
        await Assert.That(evidence).Contains("SQL Server:");
        await Assert.That(evidence).Contains("Service Bus emulator:");
        await Assert.That(evidence).Contains("exitCode=137, oomKilled=False");
        await Assert.That(evidence).Contains("controlled stdout");
        await Assert.That(evidence).Contains("controlled stderr");
        if (!failDatabase)
        {
            await Assert.That(evidence).Contains("running=True");
            await Assert.That(evidence).Contains("healthy dependency");
        }
    }

    [Test]
    public async Task Unavailable_diagnostics_preserve_the_original_startup_exception_and_stack()
    {
        // Neither container has an ID. Both inspect and log capture must fail independently.
        await using var database = Container(false);
        await using var emulator = Container(false);
        var original = new InvalidOperationException("original startup failure");
        using var cancelled = new CancellationTokenSource();
        await cancelled.CancelAsync();
        Exception? failure = null;
        try { await ServiceBusStartupDiagnostics.StartAsync(FailAsync, database, emulator, cancelled.Token); }
        catch (Exception error) { failure = error; }
        await Assert.That(ReferenceEquals(failure, original)).IsTrue();
        await Assert.That(failure!.StackTrace!).Contains(nameof(FailAsync));
        var evidence = (string)failure.Data[ServiceBusStartupDiagnostics.EvidenceKey]!;
        await Assert.That(evidence).Contains("Docker state unavailable:");
        await Assert.That(evidence).Contains("Startup logs unavailable:");

        Task FailAsync(CancellationToken _) => throw original;
    }

    private static IContainer Container(bool fail) => new ContainerBuilder("alpine:3.22")
        .WithEntrypoint("/bin/sh")
        .WithCommand("-c", fail
            ? "echo 'controlled stdout'; echo 'controlled stderr' >&2; exit 137"
            : "echo 'healthy dependency'; exec sleep infinity")
        .WithWaitStrategy(Wait.ForUnixContainer().UntilMessageIsLogged(fail ? "never ready" : "healthy dependency"))
        .Build();
}
