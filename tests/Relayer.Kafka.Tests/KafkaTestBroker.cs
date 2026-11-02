using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Configurations;
using Testcontainers.Kafka;

namespace Relayer.Kafka.Tests;

internal static class KafkaTestBroker
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

    // Apply to ordinary and restartable brokers: otherwise named-cluster tests start multiple
    // 1 GiB heaps and dedicated-server compiler pools beside the concurrent lifecycle test.
    internal static KafkaBuilder ForSmallWorkload(this KafkaBuilder builder) => builder
        .WithEnvironment("KAFKA_HEAP_OPTS", "-Xms128m -Xmx256m")
        .WithEnvironment("KAFKA_OPTS", "-XX:ActiveProcessorCount=2 -XX:TieredStopAtLevel=1");

    internal static KafkaBuilder AfterStartupCopy(this KafkaBuilder builder) => builder
        .ForSmallWorkload()
        .WithKRaft()
        .WithCommand(new OverwriteEnumerable<string>([StartupCommand]))
        .WithWaitStrategy(WaitForStartupCopy()
            .UntilMessageIsLogged(".*Transitioning from RECOVERY to RUNNING.*"));
}
