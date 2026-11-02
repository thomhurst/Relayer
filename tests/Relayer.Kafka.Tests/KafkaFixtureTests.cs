using System.Text;
using Dekaf.Admin;
using DotNet.Testcontainers.Configurations;
using Microsoft.Extensions.Options;

namespace Relayer.Kafka.Tests;

public class KafkaFixtureTests
{
    [Test]
    // Four concurrent brokers already exercise contention. Keep unrelated Kafka JVMs outside
    // this test's two-minute budget, as with the multi-broker schema registry contract.
    [NotInParallel]
    public async Task Concurrent_fixtures_keep_their_published_endpoint_while_the_broker_is_stopped()
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(2));
        var runs = new Task<string>[4];
        for (var i = 0; i < runs.Length; i++) runs[i] = VerifyLifecycleAsync(timeout.Token);
        var endpoints = await Task.WhenAll(runs);
        await Assert.That(endpoints.Distinct().Count()).IsEqualTo(runs.Length);
    }

    [Test]
    public async Task Publication_and_script_copy_finish_before_the_first_broker_process_starts()
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(2));
        var token = timeout.Token;
        await using var broker = new KafkaRestartableBroker();
        await broker.Container.StartAsync(token);
        var script = await broker.Container.ReadFileAsync("/testcontainers.sh", token);
        var endpoint = new Uri(broker.Bootstrap);
        await Assert.That(Encoding.UTF8.GetString(script)).Contains($"PLAINTEXT://{endpoint.Host}:{endpoint.Port}");
        var waiting = await broker.Container.ExecAsync(["/bin/bash", "-ec", "test ! -f /tmp/relayer-kafka.pid && test ! -f /tmp/relayer-kafka.log"], token);
        await Assert.That(waiting.ExitCode).IsEqualTo(0L);
        await broker.RestartAsync(token);
        await using var admin = new AdminClientBuilder().WithBootstrapServers(broker.Bootstrap).Build();
        await admin.ListTopicsAsync(cancellationToken: token);
    }

    [Test]
    public async Task Pending_readiness_wait_observes_cancellation_and_preserves_broker_output()
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(2));
        await using var broker = new KafkaRestartableBroker();
        await broker.Container.StartAsync(timeout.Token);
        // Replace Kafka with a process that acknowledges startup but never reports readiness.
        // Wait for both broker output and host entry into the readiness wait before cancelling.
        await broker.Container.CopyAsync(Encoding.UTF8.GetBytes(
            "#!/bin/bash\necho 'fixture startup pending'\ntouch /tmp/relayer-fixture-pending\nexec sleep infinity\n"),
            "/testcontainers.sh", ct: timeout.Token);
        using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(timeout.Token);
        var waiting = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var pending = broker.RestartAsync(cancellation.Token, () => waiting.SetResult());
        await waiting.Task.WaitAsync(timeout.Token);
        await broker.Container.ExecAsync(["/bin/bash", "-ec",
            "until test -f /tmp/relayer-fixture-pending; do sleep 0.1; done"], timeout.Token);
        await cancellation.CancelAsync();

        try
        {
            await pending;
            throw new InvalidOperationException("Readiness completed without cancellation.");
        }
        catch (InvalidOperationException error) when (error.InnerException is OperationCanceledException)
        {
            await Assert.That(error.Message).Contains("fixture startup pending");
        }
    }

    private static async Task<string> VerifyLifecycleAsync(CancellationToken token)
    {
        using var tracing = new MessageTracing(Options.Create(new TracingOptions()), []);
        await using var fixture = (KafkaFixture)await KafkaFixture.CreateAsync(tracing, token);
        using var docker = TestcontainersSettings.OS.DockerEndpointAuthConfig.GetDockerClientBuilder(Guid.NewGuid()).Build();
        var endpoint = fixture.Bootstrap;
        var script = await fixture.Broker.Container.ReadFileAsync("/testcontainers.sh", token);
        var configuration = await fixture.Broker.Container.ReadFileAsync("/opt/kafka/config/server.properties", token);

        for (var cycle = 0; cycle < 2; cycle++)
        {
            await fixture.DisconnectAsync(token);
            await Assert.That(fixture.Bootstrap).IsEqualTo(endpoint);
            var stopped = await fixture.Broker.Container.ExecAsync(["/bin/bash", "-ec", "test ! -f /tmp/relayer-kafka.pid"], token);
            await Assert.That(stopped.ExitCode).IsEqualTo(0L);
            // Inspect the live daemon, rather than Testcontainers' cached inspection or a released socket probe.
            var publication = await docker.Containers.InspectContainerAsync(fixture.Broker.Container.Id, token);
            await Assert.That(publication.State!.Running).IsTrue();
            await Assert.That(publication.NetworkSettings!.Ports["9092/tcp"][0].HostPort)
                .IsEqualTo(new Uri(endpoint).Port.ToString());
            await fixture.ReconnectAsync(token);
            await Assert.That(fixture.Bootstrap).IsEqualTo(endpoint);
            await Assert.That(await fixture.Broker.Container.ReadFileAsync("/testcontainers.sh", token)).IsEquivalentTo(script);
            await Assert.That(await fixture.Broker.Container.ReadFileAsync("/opt/kafka/config/server.properties", token)).IsEquivalentTo(configuration);
            await using var admin = new AdminClientBuilder().WithBootstrapServers(endpoint).Build();
            var topics = await admin.ListTopicsAsync(cancellationToken: token);
            await Assert.That(topics.Any(topic => topic.Name == fixture.Destination)).IsTrue();
        }
        return endpoint;
    }
}
