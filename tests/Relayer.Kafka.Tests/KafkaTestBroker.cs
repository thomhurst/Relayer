using DotNet.Testcontainers.Builders;
using Testcontainers.Kafka;

namespace Relayer.Kafka.Tests;

internal static class KafkaTestBroker
{
    // Apply to ordinary and restartable brokers: otherwise named-cluster tests start multiple
    // 1 GiB heaps and dedicated-server compiler pools beside the concurrent lifecycle test.
    internal static KafkaBuilder ForSmallWorkload(this KafkaBuilder builder)
    {
        return builder
                .WithEnvironment("KAFKA_HEAP_OPTS", "-Xms128m -Xmx256m")
                .WithEnvironment("KAFKA_OPTS", "-XX:ActiveProcessorCount=2 -XX:TieredStopAtLevel=1");
    }

    internal static KafkaBuilder AfterStartupCopy(this KafkaBuilder builder)
    {
        return builder
                .ForSmallWorkload()
                .WithKRaft()
                // Testcontainers releases its entrypoint after the script copy:
                // https://github.com/testcontainers/testcontainers-dotnet/pull/1810
                .WithWaitStrategy(Wait.ForUnixContainer()
                    .UntilMessageIsLogged(".*Transitioning from RECOVERY to RUNNING.*"));
    }
}
