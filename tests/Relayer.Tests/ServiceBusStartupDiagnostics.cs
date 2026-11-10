using System.Text;
using DotNet.Testcontainers.Configurations;
using DotNet.Testcontainers.Containers;

namespace Relayer.Tests;

// Testcontainers includes exit code/logs for the container whose readiness failed, but not
// Docker's OOM state or the SQL dependency when the dependent emulator fails.
internal static class ServiceBusStartupDiagnostics
{
    internal const string EvidenceKey = "ServiceBusStartupDiagnostics";

    internal static async Task StartAsync(Func<CancellationToken, Task> start,
        IContainer database, IContainer emulator, CancellationToken token)
    {
        try { await start(token); }
        catch (Exception error)
        {
            // Never use the expired provisioning token for diagnostics. Each operation has its
            // own budget, so an unavailable inspect endpoint cannot hide available logs.
            var evidence = new StringBuilder("Service Bus startup failed. Exit 137 alone does not prove OOM.\n");
            await CaptureAsync("SQL Server", database, evidence);
            await CaptureAsync("Service Bus emulator", emulator, evidence);
            var report = evidence.ToString();
            error.Data[EvidenceKey] = report;
            try { Console.Error.WriteLine(report); }
            catch (Exception) { /* A broken output sink must not replace the startup error. */ }
            throw;
        }
    }

    private static async Task CaptureAsync(string name, IContainer container, StringBuilder evidence)
    {
        evidence.AppendLine($"{name}:");
        try
        {
            using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            using var docker = TestcontainersSettings.OS.DockerEndpointAuthConfig.GetDockerClientBuilder(Guid.NewGuid()).Build();
            var inspection = await docker.Containers.InspectContainerAsync(container.Id, timeout.Token).WaitAsync(timeout.Token);
            var state = inspection.State ?? throw new InvalidOperationException("Docker returned no container state.");
            evidence.AppendLine($"id={container.Id}, image={inspection.Config?.Image}, status={state.Status}, " +
                $"running={state.Running}, exitCode={state.ExitCode}, oomKilled={state.OOMKilled}, " +
                $"error={state.Error}, startedAt={state.StartedAt}, finishedAt={state.FinishedAt}");
        }
        catch (Exception error) { evidence.AppendLine($"Docker state unavailable: {error.GetType().Name}: {error.Message}"); }
        try
        {
            using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var logs = await container.GetLogsAsync(ct: timeout.Token).WaitAsync(timeout.Token);
            evidence.AppendLine("stdout:").AppendLine(logs.Stdout).AppendLine("stderr:").AppendLine(logs.Stderr);
        }
        catch (Exception error) { evidence.AppendLine($"Startup logs unavailable: {error.GetType().Name}: {error.Message}"); }
    }
}
