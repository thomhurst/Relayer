using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Configurations;
using Testcontainers.Kafka;

namespace Relayer.Kafka.Tests;

/// <summary>Keeps Docker's publication alive while the Kafka process is stopped and restarted.</summary>
internal sealed class KafkaRestartableBroker : IAsyncDisposable
{
    // Startup is explicitly requested only after StartAsync has finished copying Testcontainers'
    // generated script. Unexpected process exits fail the container instead of retrying startup.
    private const string Supervisor = """
        set -eu
        broker=
        start=(/bin/bash /testcontainers.sh)
        trap 'if [ -n "$broker" ]; then kill -TERM "$broker" 2>/dev/null || true; wait "$broker" || true; fi; exit 0' TERM INT
        while true; do
            while [ ! -f /tmp/relayer-kafka-start ]; do sleep 0.1; done
            : > /tmp/relayer-kafka.log
            rm -f /tmp/relayer-kafka-start /tmp/relayer-kafka-stopped /tmp/relayer-kafka-stop
            "${start[@]}" >> /tmp/relayer-kafka.log 2>&1 &
            broker=$!
            echo "$broker" > /tmp/relayer-kafka.pid
            wait "$broker" || true
            broker=
            rm /tmp/relayer-kafka.pid
            cat /tmp/relayer-kafka.log
            if [ ! -f /tmp/relayer-kafka-stop ]; then exit 1; fi
            # Configuration and storage formatting already completed on the first start.
            start=(/opt/kafka/bin/kafka-server-start.sh /opt/kafka/config/server.properties)
            touch /tmp/relayer-kafka-stopped
        done
        """;

    internal KafkaContainer Container { get; }
    internal string Bootstrap => Container.GetBootstrapAddress();

    internal KafkaRestartableBroker()
    {
        var builder = new KafkaBuilder("apache/kafka:4.0.2").WithVendor(KafkaVendor.ApacheSoftwareFoundation)
            .ForSmallWorkload()
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

    internal async Task RestartAsync(CancellationToken token, Action? onWaiting = null)
    {
        await ExecuteAsync("touch /tmp/relayer-kafka-start", token);
        // A fresh log prevents stale readiness. Kafka reports RUNNING before its socket acceptors start.
        await WaitAsync("test ! -f /tmp/relayer-kafka-start && test -f /tmp/relayer-kafka.pid && grep -q 'Kafka Server started' /tmp/relayer-kafka.log", token, onWaiting);
    }

    private async Task ExecuteAsync(string command, CancellationToken token)
    {
        var result = await Container.ExecAsync(["/bin/bash", "-ec", command], token);
        if (result.ExitCode != 0)
            throw new InvalidOperationException($"Kafka lifecycle command failed ({result.ExitCode}): {result.Stderr}");
    }

    private async Task WaitAsync(string condition, CancellationToken token, Action? onWaiting = null)
    {
        try
        {
            onWaiting?.Invoke();
            // Keep the 100 ms readiness checks inside one exec. Launching a Docker exec for
            // every probe competes with concurrent broker JVMs and their lifecycle commands.
            await ExecuteAsync($"until {condition}; do sleep 0.1; done", token);
        }
        catch (Exception error)
        {
            var log = await Container.ExecAsync(["/bin/bash", "-c", "cat /tmp/relayer-kafka.log 2>/dev/null"]);
            throw new InvalidOperationException($"Kafka lifecycle wait failed: {log.Stdout}", error);
        }
    }

    public ValueTask DisposeAsync() => Container.DisposeAsync();
}
