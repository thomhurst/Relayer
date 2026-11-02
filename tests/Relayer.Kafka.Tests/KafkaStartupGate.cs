using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Configurations;
using Testcontainers.Kafka;

namespace Relayer.Kafka.Tests;

internal static class KafkaStartupGate
{
    private const string StartupReady = "/tmp/relayer-kafka-startup-ready";

    // Workaround for https://github.com/testcontainers/testcontainers-dotnet/issues/1809.
    // File existence does not mean Docker has closed its writable descriptor. Release the
    // entrypoint only from the readiness phase, after Testcontainers awaits CopyAsync.
    // Consume the marker before exec so Stop/Start cannot reuse the previous release.
    internal const string StartupCommand =
        $"while [ ! -f {StartupReady} ]; do sleep 0.1; done; rm {StartupReady}; exec {KafkaBuilder.StartupScriptFilePath}";

    internal static IWaitForContainerOS WaitForStartupCopy() => Wait.ForUnixContainer()
        .UntilCommandIsCompleted("touch", StartupReady);

    internal static KafkaBuilder AfterStartupCopy(this KafkaBuilder builder) => builder
        .WithKRaft()
        .WithCommand(new OverwriteEnumerable<string>([StartupCommand]))
        .WithWaitStrategy(WaitForStartupCopy()
            .UntilMessageIsLogged(".*Transitioning from RECOVERY to RUNNING.*"));
}
