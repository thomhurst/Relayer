using System.Diagnostics;
using Dekaf;
using Dekaf.Admin;
using Dekaf.Consumer;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Npgsql;
using Relayer.Kafka.Tests;
using Testcontainers.Kafka;

namespace Relayer.Outbox.PostgreSql.Tests;

// Kafka -> database -> outbox (design §7): the record is marked processed only after the EF Core
// inbox/outbox transaction commits, and the inbox deduplicates the replay after a crash.
[ClassDataSource<KafkaInboxBroker>(Shared = SharedType.PerTestSession)]
public class KafkaInboxCrashTests(KafkaInboxBroker broker)
{
    [Test]
    public async Task Crash_before_the_database_commit_replays_the_record_and_writes_once()
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(3));
        var token = timeout.Token;
        await using var scenario = await Scenario.StartAsync(broker, token);

        await using (var child = scenario.Probe("block"))
        {
            await child.WaitForAsync(KafkaInboxProbe.Saved, token);
            await child.KillAsync();
        }

        await Assert.That(await scenario.CountsAsync()).IsEqualTo((0L, 0L, 0L));
        await Assert.That(await scenario.CommittedAsync(token)).IsNull();
        var replay = await scenario.ReplayAsync(token);
        await Assert.That(replay.Invocations).IsEqualTo(1);
        await Assert.That(await scenario.CountsAsync()).IsEqualTo((1L, 1L, 1L));
    }

    [Test]
    public async Task Crash_after_the_database_commit_replays_the_record_and_the_inbox_skips_it()
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(3));
        var token = timeout.Token;
        await using var scenario = await Scenario.StartAsync(broker, token);

        await using (var child = scenario.Probe("committed"))
        {
            // The probe's commit interval is an hour, so the offset is still uncommitted when it dies.
            await child.WaitForAsync(KafkaInboxProbe.Saved, token);
            await scenario.WaitForInboxAsync(token);
            await child.KillAsync();
        }

        await Assert.That(await scenario.CountsAsync()).IsEqualTo((1L, 1L, 1L));
        await Assert.That(await scenario.CommittedAsync(token)).IsNull();
        var replay = await scenario.ReplayAsync(token);
        await Assert.That(replay.Invocations).IsEqualTo(0);
        await Assert.That(await scenario.CountsAsync()).IsEqualTo((1L, 1L, 1L));
    }

    [Test]
    public async Task Failed_attempt_rolls_back_before_the_retry_commits()
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(3));
        var token = timeout.Token;
        await using var scenario = await Scenario.StartAsync(broker, token);
        var replay = await scenario.ReplayAsync(token, failFirst: true);
        await Assert.That(replay.Invocations).IsEqualTo(2);
        await Assert.That(await scenario.CountsAsync()).IsEqualTo((1L, 1L, 1L));
    }

    [Test]
    public async Task Cancelled_handler_rolls_back_and_its_record_is_not_committed()
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(3));
        var token = timeout.Token;
        await using var scenario = await Scenario.StartAsync(broker, token);
        var blocked = new KafkaInboxSettings(broker.Bootstrap, scenario.ConnectionString, scenario.Topic,
            TimeSpan.FromMilliseconds(100))
        {
            Block = true
        };

        using (var host = scenario.Host(blocked))
        {
            await host.StartAsync(token);
            await blocked.Entered.Task.WaitAsync(token);
            // StopTimeout (1 s) cancels the handler inside its open transaction.
            await host.StopAsync(token);
        }

        await Assert.That(await scenario.CountsAsync()).IsEqualTo((0L, 0L, 0L));
        await Assert.That(await scenario.CommittedAsync(token)).IsNull();
        var replay = await scenario.ReplayAsync(token);
        await Assert.That(replay.Invocations).IsEqualTo(1);
        await Assert.That(await scenario.CountsAsync()).IsEqualTo((1L, 1L, 1L));
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task Kafka_cleanup_failure_still_drops_the_database(bool failDuringStartup)
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(3));
        var token = timeout.Token;
        var startupFailure = new InvalidOperationException("Could not seed the Kafka inbox scenario.");
        Scenario? scenario = null;
        IAdminClient? admin = null;

        async Task CaptureResourcesAsync(Scenario started, IAdminClient client)
        {
            scenario = started;
            admin = client;

            if (failDuringStartup)
            {
                await client.DisposeAsync();
                throw startupFailure;
            }
        }

        try
        {
            if (failDuringStartup)
            {
                var observed = await Assert.ThrowsAsync<InvalidOperationException>(
                    () => Scenario.StartAsync(broker, token, CaptureResourcesAsync));
                await Assert.That(observed).IsSameReferenceAs(startupFailure);
                await Assert.That(observed!.Data["KafkaInboxScenarioCleanupFailure"]).IsTypeOf<ObjectDisposedException>();
            }
            else
            {
                Scenario started = await Scenario.StartAsync(broker, token, CaptureResourcesAsync);
                await admin!.DisposeAsync();
                await Assert.ThrowsAsync<ObjectDisposedException>(async () => await started.DisposeAsync());
            }

            var session = await PostgreSqlSession.Current;
            await using var command = session.Admin.CreateCommand("SELECT count(*) FROM pg_database WHERE datname = $1");
            command.Parameters.AddWithValue(scenario!.DatabaseFixture.Database);
            await Assert.That((long)(await command.ExecuteScalarAsync(token))!).IsEqualTo(0L);
        }
        finally
        {
            if (scenario is not null)
            {
                await scenario.DatabaseFixture.DisposeAsync();
                await using var cleanup = new AdminClientBuilder().WithBootstrapServers(broker.Bootstrap).Build();
                await cleanup.DeleteTopicsAsync([scenario.Topic]);
            }
        }
    }

    private sealed class Scenario : IAsyncDisposable
    {
        private readonly KafkaInboxBroker _broker;
        private IAdminClient? _admin;

        private Scenario(KafkaInboxBroker broker)
        {
            _broker = broker;
            Topic = TestContext.Current!.Isolation.GetIsolatedName("kafka-inbox").ToLowerInvariant();
        }

        internal string Topic
        {
            get;
        }

        internal PostgreSqlFixture DatabaseFixture
        {
            get;
        } = new();

        internal string ConnectionString => DatabaseFixture.ConnectionString;

        internal static async Task<Scenario> StartAsync(KafkaInboxBroker broker, CancellationToken token,
            Func<Scenario, IAdminClient, Task>? beforeSeedAsync = null)
        {
            var scenario = new Scenario(broker);

            try
            {
                await broker.Started.WaitAsync(token);
                await scenario.DatabaseFixture.StartAsync();
                await scenario.ExecuteAsync(PostgreSqlOutboxSchema.CreateScript() +
                    "CREATE TABLE kafka_orders (\"Id\" uuid PRIMARY KEY, \"Value\" integer NOT NULL)");
                scenario._admin = new AdminClientBuilder().WithBootstrapServers(broker.Bootstrap).Build();
                await scenario._admin.CreateTopicsAsync([new() { Name = scenario.Topic, NumPartitions = 1, ReplicationFactor = 1 }],
                    cancellationToken: token);

                if (beforeSeedAsync is not null)
                {
                    await beforeSeedAsync(scenario, scenario._admin);
                }

                // Relayer's own send writes the wire identity and UUIDv7 message ID the inbox requires.
                // No consumer here: the transport starts every registered consumer, which could process the seed.
                var sending = new ServiceCollection().AddLogging();
                KafkaInboxProbe.Configure(sending, new(broker.Bootstrap, scenario.ConnectionString, scenario.Topic, TimeSpan.FromSeconds(5)),
                    consume: false);
                await using var provider = sending.BuildServiceProvider();
                var transport = provider.GetRequiredService<Relayer.Transports.ITransport>();
                await transport.StartAsync(token);

                await using (var scope = provider.CreateAsyncScope())
                {
                    await scope.ServiceProvider.GetRequiredService<ISender>().SendWithoutOutboxAsync(new KafkaInboxWork(7), token);
                }

                await transport.StopAsync(token);
                return scenario;
            }
            catch (Exception startupFailure)
            {
                try
                {
                    await scenario.DisposeAsync();
                }
                catch (Exception cleanupFailure)
                {
                    startupFailure.Data["KafkaInboxScenarioCleanupFailure"] = cleanupFailure;
                }

                throw;
            }
        }

        internal IHost Host(KafkaInboxSettings settings)
        {
            var builder = Microsoft.Extensions.Hosting.Host.CreateApplicationBuilder();
            KafkaInboxProbe.Configure(builder.Services, settings);
            return builder.Build();
        }

        // Runs to a graceful commit of the single record, then stops.
        internal async Task<KafkaInboxSettings> ReplayAsync(CancellationToken token, bool failFirst = false)
        {
            var settings = new KafkaInboxSettings(_broker.Bootstrap, ConnectionString, Topic, TimeSpan.FromMilliseconds(100))
            {
                FailFirst = failFirst
            };
            using var host = Host(settings);
            await host.StartAsync(token);

            while (await CommittedAsync(token) != 1)
            {
                await Task.Delay(50, token);
            }

            await host.StopAsync(token);
            return settings;
        }

        internal ChildProbe Probe(string mode)
        {
            var start = new ProcessStartInfo("dotnet") { RedirectStandardOutput = true, RedirectStandardError = true, CreateNoWindow = true };
            start.ArgumentList.Add("exec");
            start.ArgumentList.Add("--runtimeconfig");
            start.ArgumentList.Add(Path.ChangeExtension(typeof(KafkaInboxCrashTests).Assembly.Location, ".runtimeconfig.json"));
            start.ArgumentList.Add("--depsfile");
            start.ArgumentList.Add(Path.ChangeExtension(typeof(KafkaInboxCrashTests).Assembly.Location, ".deps.json"));
            start.ArgumentList.Add(typeof(CrashProbeMarker).Assembly.Location);
            start.ArgumentList.Add("kafka");
            start.ArgumentList.Add(_broker.Bootstrap);
            start.ArgumentList.Add(ConnectionString);
            start.ArgumentList.Add(Topic);
            start.ArgumentList.Add(mode);
            return new(Process.Start(start)!);
        }

        internal async Task<long?> CommittedAsync(CancellationToken token)
        {
            await using var reader = new ConsumerBuilder<byte[], byte[]>().WithBootstrapServers(_broker.Bootstrap).WithGroupId(Topic)
                .WithOffsetCommitMode(OffsetCommitMode.Manual).WithAutoOffsetStore(false).Build();
            await reader.InitializeAsync(token);
            var offset = await reader.GetCommittedOffsetAsync(new(Topic, 0), token);
            return offset is null or < 0 ? null : offset;
        }

        internal async Task<(long Orders, long Inbox, long Outbox)> CountsAsync()
        {
            return (await ScalarAsync("SELECT count(*) FROM kafka_orders"), await ScalarAsync("SELECT count(*) FROM relayer_inbox"),
                            await ScalarAsync("SELECT count(*) FROM relayer_outbox"));
        }

        internal async Task WaitForInboxAsync(CancellationToken token)
        {
            while (await ScalarAsync("SELECT count(*) FROM relayer_inbox") == 0)
            {
                await Task.Delay(20, token);
            }
        }

        private async Task<long> ScalarAsync(string sql)
        {
            await using var connection = new NpgsqlConnection(ConnectionString);
            await connection.OpenAsync();
            await using var command = new NpgsqlCommand(sql, connection);
            return (long)(await command.ExecuteScalarAsync())!;
        }

        private async Task ExecuteAsync(string sql)
        {
            await using var connection = new NpgsqlConnection(ConnectionString);
            await connection.OpenAsync();
            await using var command = new NpgsqlCommand(sql, connection);
            await command.ExecuteNonQueryAsync();
        }

        public async ValueTask DisposeAsync()
        {
            try
            {
                await using var admin = _admin;

                if (admin is not null)
                {
                    await admin.DeleteTopicsAsync([Topic]);
                }
            }
            finally
            {
                await DatabaseFixture.DisposeAsync();
            }
        }
    }

    private sealed class ChildProbe(Process process) : IAsyncDisposable
    {
        private readonly Task<string> _errors = process.StandardError.ReadToEndAsync();

        internal async Task WaitForAsync(string signal, CancellationToken token)
        {
            while (true)
            {
                var line = await process.StandardOutput.ReadLineAsync(token);

                if (line == signal)
                {
                    return;
                }

                if (line is null)
                {
                    throw new InvalidOperationException($"Kafka crash probe exited before {signal}: {await _errors}");
                }
            }
        }

        internal async Task KillAsync()
        {
            process.Kill(entireProcessTree: true);
            await process.WaitForExitAsync();
        }

        public async ValueTask DisposeAsync()
        {
            if (!process.HasExited)
            {
                await KillAsync();
            }

            process.Dispose();
        }
    }
}

public sealed class KafkaInboxBroker : IAsyncDisposable
{
    private readonly KafkaContainer _broker = new KafkaBuilder("apache/kafka:4.0.2")
        .WithVendor(KafkaVendor.ApacheSoftwareFoundation).AfterStartupCopy().Build();

    public Task Started
    {
        get;
    }

    public string Bootstrap => _broker.GetBootstrapAddress();

    public KafkaInboxBroker()
    {
        Started = _broker.StartAsync();
    }

    public ValueTask DisposeAsync()
    {
        return _broker.DisposeAsync();
    }
}
