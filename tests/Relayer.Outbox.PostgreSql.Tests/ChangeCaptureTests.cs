using System.Buffers;
using System.Collections.Concurrent;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Npgsql;
using Npgsql.Replication;
using Npgsql.Replication.PgOutput;
using NpgsqlTypes;
using Relayer.Transports;

namespace Relayer.Outbox.PostgreSql.Tests;

public class ChangeCaptureTests
{
    [Test]
    public async Task Registration_validates_protocol_limits_and_duplicate_names()
    {
        await using var source = NpgsqlDataSource.Create("Host=unused");
        var services = new ServiceCollection().AddLogging();
        services.AddRelayerPostgreSqlChangeCapture("orders", source, () => new("Host=unused"), Options());
        var configuration = new ConfigurationBuilder().AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["BatchSize"] = "7", ["ReconnectDelay"] = "00:00:02", ["Schema"] = "custom"
        }).Build();
        var native = new PgOutputReplicationOptions("other", PgOutputProtocolVersion.V1);
        services.AddRelayerPostgreSqlChangeCapture("other", source, () => new("Host=unused"), new("other"), native, configuration);
        configuration["BatchSize"] = "99";
        await using var provider = services.BuildServiceProvider();
        var settings = provider.GetRequiredService<IOptionsMonitor<PostgreSqlChangeCaptureOptions>>().Get("other");
        await Assert.That(settings.BatchSize).IsEqualTo(7);
        await Assert.That(settings.ReconnectDelay).IsEqualTo(TimeSpan.FromSeconds(2));
        await Assert.That(settings.Schema).IsEqualTo("custom");
        await Assert.That(settings.Replication).IsSameReferenceAs(native);
        await Assert.That(provider.GetServices<IHostedService>().Count()).IsEqualTo(2);
        await Assert.That(() => services.AddRelayerPostgreSqlChangeCapture("orders", source, () => new(), Options())).Throws<InvalidOperationException>();
        await Assert.That(() => new PostgreSqlChangeCaptureOptions
        {
            Slot = new("orders"), Replication = new("orders", PgOutputProtocolVersion.V2)
        }.Validate()).Throws<ArgumentException>();
        await Assert.That(() => Options(batchSize: 0).Validate()).Throws<ArgumentOutOfRangeException>();
    }

    [Test]
    public async Task Startup_requires_persistent_slot_and_insert_only_unfiltered_publication()
    {
        await using var fixture = new PostgreSqlFixture();
        await fixture.StartAsync();
        await using var source = NpgsqlDataSource.Create(fixture.ConnectionString);
        await PostgreSqlOutboxStoreTests.CreateSchema(source);
        using var worker = Worker(source, fixture.ConnectionString, new());
        await Assert.That(async () => await worker.StartAsync(default)).Throws<InvalidOperationException>();
        await Provision(source, fixture.ConnectionString, "insert, update, delete, truncate");
        await Assert.That(async () => await worker.StartAsync(default)).Throws<InvalidOperationException>();
        await Sql(source, "ALTER PUBLICATION orders SET (publish = 'insert'); ALTER PUBLICATION orders SET TABLE relayer_outbox WHERE (lane IS NULL)");
        await Assert.That(async () => await worker.StartAsync(default)).Throws<InvalidOperationException>();
    }

    [Test]
    public async Task Captures_commits_drains_pre_slot_backlog_and_preserves_ordered_and_future_rows()
    {
        await using var fixture = new PostgreSqlFixture();
        await fixture.StartAsync();
        await using var source = NpgsqlDataSource.Create(fixture.ConnectionString);
        await PostgreSqlOutboxStoreTests.CreateSchema(source);
        for (var i = 1; i <= 5; i++) await Insert(source, Id(i));
        await Insert(source, Id(6), lane: 0);
        await Insert(source, Id(7), future: true);
        await Provision(source, fixture.ConnectionString);
        var transport = new RecordingTransport();
        using var worker = Worker(source, fixture.ConnectionString, transport, retainSentMessages: true);
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        await worker.StartAsync(timeout.Token);
        try
        {
            await Until(async () => await Count(source) == 2, timeout.Token);
            await Streaming(source, timeout.Token);
            await Insert(source, Id(8), commit: false);
            var position = await Insert(source, Id(9));
            await Acknowledged(source, position, timeout.Token);
            await Assert.That(transport.Ids.Order().SequenceEqual(new[] { Id(1), Id(2), Id(3), Id(4), Id(5), Id(9) })).IsTrue();
            await Assert.That(transport.BatchSizes.All(static size => size <= 2)).IsTrue();
            await Assert.That(transport.BatchSizes.Count).IsGreaterThan(0);
            await Assert.That(await Count(source)).IsEqualTo(2L);
            await using var sent = source.CreateCommand("SELECT count(*) FROM relayer_outbox_sent");
            await Assert.That((long)(await sent.ExecuteScalarAsync(timeout.Token))!).IsEqualTo(6L);
        }
        finally { await worker.StopAsync(timeout.Token); }
    }

    [Test]
    public async Task Shutdown_after_native_acceptance_replays_same_identity_and_body_on_restart()
    {
        await using var fixture = new PostgreSqlFixture();
        await fixture.StartAsync();
        await using var source = NpgsqlDataSource.Create(fixture.ConnectionString);
        await PostgreSqlOutboxStoreTests.CreateSchema(source);
        await Provision(source, fixture.ConnectionString);
        var accepted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var transport = new RecordingTransport(async (_, token) =>
        {
            accepted.TrySetResult();
            await Task.Delay(Timeout.InfiniteTimeSpan, token);
        });
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        NpgsqlLogSequenceNumber position;
        using (var first = Worker(source, fixture.ConnectionString, transport))
        {
            await first.StartAsync(timeout.Token);
            try
            {
                await Streaming(source, timeout.Token);
                position = await Insert(source, Id(1));
                await accepted.Task.WaitAsync(timeout.Token);
            }
            finally { await first.StopAsync(timeout.Token); }
        }
        await Assert.That(await Count(source)).IsEqualTo(1L);
        // No new insert wakes the replacement. Its startup scan recovers the rolled-back dispatch.
        var replay = new RecordingTransport();
        using var second = Worker(source, fixture.ConnectionString, replay);
        await second.StartAsync(timeout.Token);
        try
        {
            await Acknowledged(source, position, timeout.Token);
            await Assert.That(await Count(source)).IsEqualTo(0L);
            await Assert.That(transport.Ids.SequenceEqual([Id(1)])).IsTrue();
            await Assert.That(replay.Ids.SequenceEqual([Id(1)])).IsTrue();
            await Assert.That(transport.Bodies.Single().SequenceEqual(replay.Bodies.Single())).IsTrue();
        }
        finally { await second.StopAsync(timeout.Token); }
    }

    [Test]
    public async Task Failed_send_stays_durable_after_checkpoint_and_restart_recovers_without_new_insert()
    {
        await using var fixture = new PostgreSqlFixture();
        await fixture.StartAsync();
        await using var source = NpgsqlDataSource.Create(fixture.ConnectionString);
        await PostgreSqlOutboxStoreTests.CreateSchema(source);
        await Provision(source, fixture.ConnectionString);
        var transport = new RecordingTransport((_, _) => throw new InvalidOperationException("Broker unavailable"));
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        using (var first = Worker(source, fixture.ConnectionString, transport))
        {
            await first.StartAsync(timeout.Token);
            try
            {
                await Streaming(source, timeout.Token);
                var position = await Insert(source, Id(1));
                await Acknowledged(source, position, timeout.Token);
                await using var attempts = source.CreateCommand("SELECT attempts FROM relayer_outbox");
                await Assert.That((int)(await attempts.ExecuteScalarAsync(timeout.Token))!).IsEqualTo(1);
            }
            finally { await first.StopAsync(timeout.Token); }
        }
        var replay = new RecordingTransport();
        using var second = Worker(source, fixture.ConnectionString, replay);
        await second.StartAsync(timeout.Token);
        try
        {
            await Until(async () => await Count(source) == 0, timeout.Token);
            await Assert.That(replay.Ids.SequenceEqual([Id(1)])).IsTrue();
        }
        finally { await second.StopAsync(timeout.Token); }
    }

    // Replication slot names are unique per server, which every test shares.
    private static string Slot => TestContext.Current!.Isolation.GetIsolatedName("orders");

    private static PostgreSqlChangeCaptureOptions Options(int batchSize = 3, bool retainSentMessages = false) => new()
    {
        Slot = new(Slot), Replication = new("orders", PgOutputProtocolVersion.V1), BatchSize = batchSize, RetainSentMessages = retainSentMessages
    };

    [Test]
    [Arguments("claim")]
    [Arguments("completion")]
    [Arguments("failure")]
    public async Task Database_failure_propagates_without_checkpoint_and_restart_recovers(string phase)
    {
        await using var fixture = new PostgreSqlFixture();
        await fixture.StartAsync();
        await using var source = NpgsqlDataSource.Create(fixture.ConnectionString);
        await PostgreSqlOutboxStoreTests.CreateSchema(source);
        await Provision(source, fixture.ConnectionString);
        var operation = phase == "completion" ? "DELETE" : "UPDATE";
        var condition = phase == "failure" ? "WHEN (NEW.attempts > OLD.attempts)" : "";
        await Sql(source, $"""
            CREATE FUNCTION fail_dispatch() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'dispatch database failure'; END; $$;
            CREATE TRIGGER fail_dispatch BEFORE {operation} ON relayer_outbox FOR EACH ROW {condition} EXECUTE FUNCTION fail_dispatch();
            """);
        var transport = phase == "failure"
            ? new RecordingTransport((_, _) => throw new InvalidOperationException("Broker unavailable"))
            : new RecordingTransport();
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        NpgsqlLogSequenceNumber position;
        using (var first = Worker(source, fixture.ConnectionString, transport))
        {
            await first.StartAsync(timeout.Token);
            try
            {
                await Streaming(source, timeout.Token);
                position = await Insert(source, Id(1));
                // Either the worker faults or it incorrectly acknowledges the failed transaction.
                // Cancel the losing observer before disposing the data source.
                using var observed = CancellationTokenSource.CreateLinkedTokenSource(timeout.Token);
                var acknowledged = Acknowledged(source, position, observed.Token);
                await Task.WhenAny(first.ExecuteTask!, acknowledged).WaitAsync(timeout.Token);
                await observed.CancelAsync();
                try { await acknowledged; }
                catch (OperationCanceledException) when (observed.IsCancellationRequested) { }
                await Assert.That(first.ExecuteTask!.IsFaulted).IsTrue();
                await Assert.That(async () => await first.ExecuteTask!).Throws<PostgresException>();
                await using var checkpoint = source.CreateCommand($"SELECT confirmed_flush_lsn < @position FROM pg_replication_slots WHERE slot_name = '{Slot}'");
                checkpoint.Parameters.AddWithValue("position", NpgsqlDbType.PgLsn, position);
                await Assert.That(await checkpoint.ExecuteScalarAsync(timeout.Token) is true).IsTrue();
                await Assert.That(await Count(source)).IsEqualTo(1L);
            }
            finally { await first.StopAsync(timeout.Token); }
        }
        await Sql(source, "DROP TRIGGER fail_dispatch ON relayer_outbox");
        var replay = new RecordingTransport();
        using var second = Worker(source, fixture.ConnectionString, replay);
        await second.StartAsync(timeout.Token);
        try
        {
            await Acknowledged(source, position, timeout.Token);
            await Assert.That(await Count(source)).IsEqualTo(0L);
            await Assert.That(replay.Ids.SequenceEqual([Id(1)])).IsTrue();
        }
        finally { await second.StopAsync(timeout.Token); }
    }

    [Test]
    public async Task Non_transient_replication_failure_stops_host()
    {
        await using var fixture = new PostgreSqlFixture();
        await fixture.StartAsync();
        await using var source = NpgsqlDataSource.Create(fixture.ConnectionString);
        await PostgreSqlOutboxStoreTests.CreateSchema(source);
        await Provision(source, fixture.ConnectionString);
        var invalid = new NpgsqlConnectionStringBuilder(fixture.ConnectionString) { Password = "invalid" };
        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddRelayerPostgreSqlChangeCapture("orders", source, () => new(invalid.ConnectionString), Options());
        using var host = builder.Build();
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        var stopped = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var registration = host.Services.GetRequiredService<IHostApplicationLifetime>().ApplicationStopping.Register(() => stopped.TrySetResult());
        await host.StartAsync(timeout.Token);
        try
        {
            await stopped.Task.WaitAsync(timeout.Token);
            var worker = (PostgreSqlChangeCaptureWorker)host.Services.GetServices<IHostedService>().Single();
            var failure = await Assert.That(async () => await worker.ExecuteTask!).Throws<PostgresException>();
            await Assert.That(failure!.SqlState).IsEqualTo(PostgresErrorCodes.InvalidPassword);
        }
        finally { await host.StopAsync(timeout.Token); }
    }

    [Test]
    public async Task Transient_native_disconnect_reconnects_to_persistent_slot_and_captures_new_commits()
    {
        await using var fixture = new PostgreSqlFixture();
        await fixture.StartAsync();
        await using var source = NpgsqlDataSource.Create(fixture.ConnectionString);
        await PostgreSqlOutboxStoreTests.CreateSchema(source);
        await Provision(source, fixture.ConnectionString);
        var transport = new RecordingTransport();
        using var worker = new PostgreSqlChangeCaptureWorker(source,
            () => new(fixture.ConnectionString) { WalReceiverStatusInterval = TimeSpan.FromMilliseconds(25) },
            new() { Slot = new(Slot), Replication = new("orders", PgOutputProtocolVersion.V1), ReconnectDelay = TimeSpan.FromMilliseconds(20) },
            [transport], NullLogger<PostgreSqlChangeCaptureWorker>.Instance, NullLogger<PostgreSqlMaintenanceWorker>.Instance,
            NullLogger<PostgreSqlDirectDispatcher>.Instance);
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        await worker.StartAsync(timeout.Token);
        try
        {
            await Streaming(source, timeout.Token);
            int previous;
            await using (var command = source.CreateCommand($"SELECT active_pid FROM pg_replication_slots WHERE slot_name = '{Slot}'"))
                previous = (int)(await command.ExecuteScalarAsync(timeout.Token))!;
            await using (var kill = source.CreateCommand("SELECT pg_terminate_backend(@pid)"))
            {
                kill.Parameters.AddWithValue("pid", previous);
                await kill.ExecuteScalarAsync(timeout.Token);
            }
            var position = await Insert(source, Id(1));
            await Acknowledged(source, position, timeout.Token);
            await Assert.That(transport.Ids.SequenceEqual([Id(1)])).IsTrue();
            await using var current = source.CreateCommand($"SELECT active_pid FROM pg_replication_slots WHERE slot_name = '{Slot}'");
            await Assert.That((int)(await current.ExecuteScalarAsync(timeout.Token))!).IsNotEqualTo(previous);
        }
        finally { await worker.StopAsync(timeout.Token); }
    }

    private static PostgreSqlChangeCaptureWorker Worker(NpgsqlDataSource source, string connectionString, RecordingTransport transport, bool retainSentMessages = false) =>
        new(source, () => new(connectionString) { WalReceiverStatusInterval = TimeSpan.FromMilliseconds(25) }, Options(retainSentMessages: retainSentMessages), [transport],
            NullLogger<PostgreSqlChangeCaptureWorker>.Instance, NullLogger<PostgreSqlMaintenanceWorker>.Instance,
            NullLogger<PostgreSqlDirectDispatcher>.Instance);

    private static async Task Provision(NpgsqlDataSource source, string connectionString, string publish = "insert")
    {
        await Sql(source, $"CREATE PUBLICATION orders FOR TABLE relayer_outbox WITH (publish = '{publish}')");
        await using var connection = new LogicalReplicationConnection(connectionString);
        await connection.Open();
        await connection.CreatePgOutputReplicationSlot(Slot);
    }

    private static async Task<NpgsqlLogSequenceNumber> Insert(NpgsqlDataSource source, Guid id,
        bool commit = true, int? lane = null, bool future = false)
    {
        var headers = new ArrayBufferWriter<byte>();
        OutboxHeaderCodec.Write(headers, [new("ce_id", new(id.ToString()))]);
        await using var connection = await source.OpenConnectionAsync();
        await using var transaction = await connection.BeginTransactionAsync();
        await using var command = new NpgsqlCommand("INSERT INTO relayer_outbox (id, transport, destination, message_type, headers, body, created_at, lane, deliver_at) VALUES (@id, 'events', 'orders', 'order;v=1', @headers, @body, clock_timestamp(), @lane, @due); SELECT pg_current_wal_insert_lsn()", connection, transaction);
        command.Parameters.AddWithValue("id", id);
        command.Parameters.AddWithValue("headers", headers.WrittenMemory.ToArray());
        command.Parameters.AddWithValue("body", new byte[] { 7, 8, 9 });
        command.Parameters.Add(new NpgsqlParameter<int?>("lane", NpgsqlDbType.Integer) { TypedValue = lane });
        command.Parameters.Add(new NpgsqlParameter<DateTimeOffset?>("due", NpgsqlDbType.TimestampTz) { TypedValue = future ? DateTimeOffset.UtcNow.AddDays(1) : null });
        var position = (NpgsqlLogSequenceNumber)(await command.ExecuteScalarAsync())!;
        if (commit) await transaction.CommitAsync();
        return position;
    }

    private static Task Streaming(NpgsqlDataSource source, CancellationToken token) => Until(async () =>
    {
        await using var command = source.CreateCommand($"SELECT EXISTS (SELECT 1 FROM pg_replication_slots s JOIN pg_stat_replication r ON r.pid = s.active_pid WHERE s.slot_name = '{Slot}' AND r.state = 'streaming')");
        return (bool)(await command.ExecuteScalarAsync(token))!;
    }, token);

    private static Task Acknowledged(NpgsqlDataSource source, NpgsqlLogSequenceNumber position, CancellationToken token) => Until(async () =>
    {
        await using var command = source.CreateCommand($"SELECT confirmed_flush_lsn >= @position FROM pg_replication_slots WHERE slot_name = '{Slot}'");
        command.Parameters.AddWithValue("position", NpgsqlDbType.PgLsn, position);
        return await command.ExecuteScalarAsync(token) is true;
    }, token);

    private static async Task Until(Func<Task<bool>> condition, CancellationToken token)
    {
        while (!await condition())
        {
            token.ThrowIfCancellationRequested();
            await Task.Yield();
        }
    }

    private static Guid Id(int i) => Guid.Parse($"01900000-0000-7000-8000-{i:D12}");
    private static async Task Sql(NpgsqlDataSource source, string sql)
    {
        await using var command = source.CreateCommand(sql);
        await command.ExecuteNonQueryAsync();
    }
    private static async Task<long> Count(NpgsqlDataSource source)
    {
        await using var command = source.CreateCommand("SELECT count(*) FROM relayer_outbox");
        return (long)(await command.ExecuteScalarAsync())!;
    }

    private sealed class RecordingTransport(Func<OutgoingMessage, CancellationToken, ValueTask>? send = null) : ITransport
    {
        public string Name => "events";
        internal ConcurrentQueue<Guid> Ids { get; } = new();
        internal ConcurrentQueue<byte[]> Bodies { get; } = new();
        internal ConcurrentQueue<int> BatchSizes { get; } = new();
        public TransportCapabilities GetCapabilities(string endpoint) => new() { MaxBatchSize = 2 };
        public async ValueTask SendAsync(OutgoingMessage message, CancellationToken cancellationToken)
        {
            Ids.Enqueue(Guid.Parse(message.Headers.Span[0].Value.ToString()));
            Bodies.Enqueue(message.Body.ToArray());
            if (send is not null) await send(message, cancellationToken);
        }
        public async ValueTask SendBatchAsync(ReadOnlyMemory<OutgoingMessage> messages, CancellationToken cancellationToken)
        {
            BatchSizes.Enqueue(messages.Length);
            for (var i = 0; i < messages.Length; i++) await SendAsync(messages.Span[i], cancellationToken);
        }
        public ValueTask<TopologyReport> ApplyTopologyAsync(TopologyMode mode, CancellationToken cancellationToken) => new(new TopologyReport([]));
        public Task StartAsync(CancellationToken cancellationToken) => Task.CompletedTask;
        public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }
}
