using System.Text;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Configurations;
using Testcontainers.Kafka;

namespace Relayer.Kafka.Tests;

public class KafkaStartupTests
{
    [Test]
    public async Task Startup_waits_for_closed_writer_and_consumes_release_before_restart()
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(2));
        var token = timeout.Token;
        var copies = 0;
        await using var container = new ContainerBuilder("apache/kafka:4.0.2")
            .WithEntrypoint("/bin/sh", "-c")
            .WithCommand(KafkaStartupGate.StartupCommand)
            .WithStartupCallback(async (container, ct) =>
            {
                // Deterministically model the window between archive entry creation and
                // Docker closing the script. No Relayer or Kafka client code is involved.
                // Mode 0777 lets the image's non-root user hold the injected writer open.
                await container.CopyAsync(Encoding.UTF8.GetBytes("#!/bin/sh\nexec sleep infinity\n"),
                    KafkaBuilder.StartupScriptFilePath, fileMode: Unix.FileMode777, ct: ct);
                var writer = await container.ExecAsync(["/bin/sh", "-c",
                    "exec 3>> /testcontainers.sh; /testcontainers.sh"], ct);
                await Assert.That(writer.ExitCode).IsEqualTo(126L);
                await Assert.That(writer.Stderr).Contains("Text file busy");
                var process = await container.ExecAsync(["cat", "/proc/1/comm"], ct);
                await Assert.That(process.Stdout.Trim()).IsEqualTo("sh");
                copies++;
            })
            .WithWaitStrategy(KafkaStartupGate.WaitForStartupCopy()
                .UntilCommandIsCompleted("/bin/sh", "-c", "test \"$(cat /proc/1/comm)\" = sleep"))
            .Build();

        await container.StartAsync(token);
        await container.StopAsync(token);
        await container.StartAsync(token);
        await Assert.That(copies).IsEqualTo(2);
    }
}
