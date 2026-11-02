using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Configurations;
using Testcontainers.Kafka;

namespace Relayer.Kafka.Tests;

/// <summary>Keeps Docker's publication alive while the Kafka process is stopped and restarted.</summary>
internal sealed class KafkaTestBroker : IAsyncDisposable
{
    // Startup is explicitly requested only after StartAsync has finished copying Testcontainers'
    // generated script. Unexpected process exits fail the container instead of retrying startup.
    private const string Supervisor = """
        set -eu
        broker=
        trap 'if [ -n "$broker" ]; then kill -TERM "$broker" 2>/dev/null || true; wait "$broker" || true; fi; exit 0' TERM INT
        while true; do
            while [ ! -f /tmp/relayer-kafka-start ]; do sleep 0.1; done
            : > /tmp/relayer-kafka.log
            rm -f /tmp/relayer-kafka-start /tmp/relayer-kafka-stopped /tmp/relayer-kafka-stop
            /bin/bash /testcontainers.sh >> /tmp/relayer-kafka.log 2>&1 &
            broker=$!
            echo "$broker" > /tmp/relayer-kafka.pid
            wait "$broker" || true
            broker=
            rm /tmp/relayer-kafka.pid
            cat /tmp/relayer-kafka.log
            if [ ! -f /tmp/relayer-kafka-stop ]; then exit 1; fi
            touch /tmp/relayer-kafka-stopped
        done
        """;

    internal KafkaContainer Container { get; }
    internal string Bootstrap => Container.GetBootstrapAddress();

    internal KafkaTestBroker()
    {
        var builder = new KafkaBuilder("apache/kafka:4.0.2").WithVendor(KafkaVendor.ApacheSoftwareFoundation)
            .WithKRaft().WithWaitStrategy(Wait.ForUnixContainer())
            .WithEntrypoint("/bin/bash", "-c")
            .WithCommand(new OverwriteEnumerable<string>([Supervisor.ReplaceLineEndings("\n")]));
        if (OperatingSystem.IsLinux())
        {
            // Docker selects and binds the port atomically, outside Linux's outbound ephemeral range.
            builder = builder.WithPortBinding("20000-29999", "9092").WithPortBinding("20000-29999", "9093");
        }
        Container = builder.Build();
    }

    internal async Task StartAsync(CancellationToken token)
    {
        await Container.StartAsync(token);
        await RestartAsync(token);
    }

    internal async Task StopAsync(CancellationToken token)
    {
        await ExecuteAsync("touch /tmp/relayer-kafka-stop; kill -TERM \"$(cat /tmp/relayer-kafka.pid)\"", token);
        // The supervisor acknowledges only after wait has reaped the process and closed its sockets.
        await WaitAsync("test -f /tmp/relayer-kafka-stopped", token);
    }

    internal async Task RestartAsync(CancellationToken token)
    {
        await ExecuteAsync("touch /tmp/relayer-kafka-start", token);
        // A consumed request and a fresh log prevent a previous generation's readiness from passing.
        await WaitAsync("test ! -f /tmp/relayer-kafka-start && test -f /tmp/relayer-kafka.pid && grep -q 'Transitioning from RECOVERY to RUNNING' /tmp/relayer-kafka.log", token);
    }

    private async Task ExecuteAsync(string command, CancellationToken token)
    {
        var result = await Container.ExecAsync(["/bin/bash", "-ec", command], token);
        if (result.ExitCode != 0)
            throw new InvalidOperationException($"Kafka lifecycle command failed ({result.ExitCode}): {result.Stderr}");
    }

    private async Task WaitAsync(string condition, CancellationToken token)
    {
        try
        {
            while ((await Container.ExecAsync(["/bin/bash", "-c", condition], token)).ExitCode != 0)
                await Task.Delay(TimeSpan.FromMilliseconds(100), token);
        }
        catch (Exception error)
        {
            var log = await Container.ExecAsync(["/bin/bash", "-c", "cat /tmp/relayer-kafka.log 2>/dev/null"]);
            throw new InvalidOperationException($"Kafka lifecycle wait failed: {log.Stdout}", error);
        }
    }

    public ValueTask DisposeAsync() => Container.DisposeAsync();
}
