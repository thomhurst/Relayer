using DotNet.Testcontainers.Containers;

namespace Relayer.Kafka.Tests;

public class KafkaLifecycleDiagnosticsTests
{
    [Test]
    [Arguments("", "", 0L, "stdout: (empty)")]
    [Arguments("", "cat: log file missing", 1L, "stderr: cat: log file missing")]
    public async Task Reports_missing_output_and_keeps_readiness_failure(
        string stdout, string stderr, long exitCode, string expected)
    {
        var readiness = new OperationCanceledException("Readiness cancelled.");

        var error = await KafkaRestartableBroker.DescribeFailureAsync("restart", readiness,
            _ => Task.FromResult(new ExecResult(stdout, stderr, exitCode)));

        await Assert.That(error.InnerException).IsSameReferenceAs(readiness);
        await Assert.That(error.Message).Contains("Kafka broker restart failed");
        await Assert.That(error.Message).Contains($"exited with code {exitCode}");
        await Assert.That(error.Message).Contains(expected);
    }

    [Test]
    public async Task Reports_unavailable_docker_without_replacing_the_operation_failure()
    {
        var stop = new InvalidOperationException("Stop command failed.");

        var error = await KafkaRestartableBroker.DescribeFailureAsync("stop", stop,
            _ => Task.FromException<ExecResult>(new IOException("Docker disconnected.")));

        await Assert.That(error.InnerException).IsSameReferenceAs(stop);
        await Assert.That(error.Message).Contains("Kafka broker stop failed");
        await Assert.That(error.Message).Contains("Docker disconnected.");
    }
}
