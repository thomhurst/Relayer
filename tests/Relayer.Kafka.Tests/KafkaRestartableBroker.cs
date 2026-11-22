using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
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

    internal KafkaContainer Container
    {
        get;
    }

    internal string Bootstrap => Container.GetBootstrapAddress();

    internal KafkaRestartableBroker()
    {
        var builder = new KafkaBuilder("apache/kafka:4.0.2").WithVendor(KafkaVendor.ApacheSoftwareFoundation)
            .ForSmallWorkload();

        // A consumer stopped during an outage cannot leave its group, and its KIP-848 member keeps the
        // partition until the broker's session timeout (default and minimum 45 s) evicts it. Shorten it
        // so recovery assigns the restarted consumer well inside the conformance deadline.
        builder = builder.WithEnvironment("KAFKA_GROUP_CONSUMER_MIN_SESSION_TIMEOUT_MS", "10000")
            .WithEnvironment("KAFKA_GROUP_CONSUMER_SESSION_TIMEOUT_MS", "10000")
            .WithEnvironment("KAFKA_GROUP_CONSUMER_MIN_HEARTBEAT_INTERVAL_MS", "1000")
            .WithEnvironment("KAFKA_GROUP_CONSUMER_HEARTBEAT_INTERVAL_MS", "1000")
            .WithKRaft().WithWaitStrategy(Wait.ForUnixContainer());

        // KafkaBuilder.Build installs its own entrypoint and command when it registers
        // the script-copy callback. Override the Docker create parameters after Build.
        builder = builder.WithCreateParameterModifier(static parameters =>
        {
            parameters.Entrypoint = ["/bin/bash", "-c"];
            parameters.Cmd = [Supervisor.ReplaceLineEndings("\n")];
        });

        if (OperatingSystem.IsLinux())
        {
            // Docker selects and binds the port atomically, outside Linux's outbound ephemeral range.
            builder = builder.WithPortBinding("20000-29999", "9092").WithPortBinding("20000-29999", "9093");
        }

        Container = builder.Build();
    }

    internal Task StartAsync(CancellationToken token)
    {
        return RunLifecycleAsync("start", async () =>
                {
                    await Container.StartAsync(token);
                    await StartBrokerAsync(token);
                });
    }

    internal Task StopAsync(CancellationToken token)
    {
        return RunLifecycleAsync("stop", async () =>
                {
                    await ExecuteAsync("touch /tmp/relayer-kafka-stop; kill -TERM \"$(cat /tmp/relayer-kafka.pid)\"", token);
                    // The supervisor acknowledges only after wait has reaped the process and closed its sockets.
                    await WaitAsync("test -f /tmp/relayer-kafka-stopped", token);
                });
    }

    internal Task RestartAsync(CancellationToken token, Action? onWaiting = null)
    {
        return RunLifecycleAsync("restart", () => StartBrokerAsync(token, onWaiting));
    }

    private async Task StartBrokerAsync(CancellationToken token, Action? onWaiting = null)
    {
        await ExecuteAsync("touch /tmp/relayer-kafka-start", token);
        // A fresh log prevents stale readiness. Kafka reports RUNNING before its socket acceptors start.
        onWaiting?.Invoke();
        await WaitAsync("test ! -f /tmp/relayer-kafka-start && test -f /tmp/relayer-kafka.pid && grep -q 'Kafka Server started' /tmp/relayer-kafka.log", token);
    }

    private async Task ExecuteAsync(string command, CancellationToken token)
    {
        var result = await Container.ExecAsync(["/bin/bash", "-ec", command], token);

        if (result.ExitCode != 0)
        {
            throw new InvalidOperationException($"Kafka lifecycle command failed ({result.ExitCode}): {result.Stderr}");
        }
    }

    // Keep the 100 ms readiness checks inside one exec to avoid competing with concurrent broker JVMs.
    private Task WaitAsync(string condition, CancellationToken token)
    {
        return ExecuteAsync($"until {condition}; do sleep 0.1; done", token);
    }

    private async Task RunLifecycleAsync(string operation, Func<Task> action)
    {
        try
        {
            await action();
        }
        catch (Exception error)
        {
            throw await DescribeFailureAsync(operation, error, token =>
                Container.ExecAsync(["/bin/bash", "-c", "cat /tmp/relayer-kafka.log"], token));
        }
    }

    internal static async Task<InvalidOperationException> DescribeFailureAsync(
        string operation, Exception error, Func<CancellationToken, Task<ExecResult>> collectLog)
    {
        var diagnosticTimeout = TimeSpan.FromSeconds(5);
        using var timeout = new CancellationTokenSource(diagnosticTimeout);
        string diagnostic;

        try
        {
            // Use an independent deadline even when the lifecycle operation was cancelled.
            var log = await collectLog(timeout.Token).WaitAsync(timeout.Token);
            var stdout = string.IsNullOrWhiteSpace(log.Stdout) ? "(empty)" : log.Stdout.TrimEnd();
            var stderr = string.IsNullOrWhiteSpace(log.Stderr) ? "(empty)" : log.Stderr.TrimEnd();
            diagnostic = $"Broker log command exited with code {log.ExitCode?.ToString() ?? "unknown"}.\n"
                + $"stdout: {stdout}\nstderr: {stderr}";
        }
        catch (OperationCanceledException) when (timeout.IsCancellationRequested)
        {
            diagnostic = $"Broker log collection timed out after {diagnosticTimeout.TotalSeconds} seconds.";
        }
        catch (Exception collectionError)
        {
            diagnostic = $"Broker log collection failed: {collectionError.GetType().Name}: {collectionError.Message}";
        }

        return new InvalidOperationException($"Kafka broker {operation} failed. {diagnostic}", error);
    }

    public ValueTask DisposeAsync()
    {
        return Container.DisposeAsync();
    }
}
