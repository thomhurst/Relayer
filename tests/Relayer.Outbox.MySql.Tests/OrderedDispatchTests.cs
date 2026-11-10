using System.Buffers;
using System.Collections.Concurrent;
using System.Data;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using MySqlConnector;
using Relayer.Transports;

namespace Relayer.Outbox.MySql.Tests;

[NotInParallel("MySQL")]
public sealed class OrderedDispatchTests
{
    [Test]
    public async Task Named_options_bind_without_reflection_and_reject_duplicate_or_invalid_settings()
    {
        await using var source = new MySqlDataSource("Server=localhost;Database=relayer");
        var services = new ServiceCollection().AddLogging();
        var configuration = new ConfigurationBuilder().AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["TablePrefix"] = "orders_", ["Lanes"] = "8", ["BatchSize"] = "16"
        }).Build();
        services.AddRelayerMySqlOrderedDispatch("orders", source, configuration);
        services.AddRelayerMySqlOrderedDispatch("other", source, new MySqlOrderedDispatchOptions { Lanes = 2 });
        await using var provider = services.BuildServiceProvider();
        var options = provider.GetRequiredService<IOptionsMonitor<MySqlOrderedDispatchOptions>>();
        await Assert.That(options.Get("orders").Lanes).IsEqualTo(8);
        await Assert.That(options.Get("orders").BatchSize).IsEqualTo(16);
        await Assert.That(options.Get("other").Lanes).IsEqualTo(2);
        await Assert.That(() => services.AddRelayerMySqlOrderedDispatch("orders", source)).Throws<InvalidOperationException>();
        await Assert.That(() => new MySqlOrderedDispatchOptions { LeaseDuration = TimeSpan.FromMilliseconds(999) }.Validate()).Throws<ArgumentOutOfRangeException>();
        await Assert.That(() => new MySqlOrderedDispatchOptions { PollInterval = TimeSpan.FromSeconds(10) }.Validate()).Throws<ArgumentOutOfRangeException>();
    }

    [Test]
    public async Task Transaction_send_order_and_sequential_commits_ignore_UUID_sort_order()
    {
        await using var database = new MySqlFixture();
        await database.StartAsync();
        var services = new ServiceCollection().AddRelayerMySqlOutbox(new MySqlOutboxOptions { Lanes = 1 });
        await using var provider = services.BuildServiceProvider();
        await using var scope = provider.CreateAsyncScope();
        var outbox = scope.ServiceProvider.GetRequiredService<MySqlOutbox>();
        var ids = new[] { Id(3), Id(2), Id(1) };
        await using (var transaction = await outbox.BeginTransactionAsync(database.Source))
        {
            await outbox.EnlistAsync(Record(ids[0]), default);
            await outbox.EnlistAsync(Record(ids[1]), default);
            await transaction.CommitAsync();
        }
        await using (var transaction = await outbox.BeginTransactionAsync(database.Source))
        {
            await outbox.EnlistAsync(Record(ids[2]), default);
            await transaction.CommitAsync();
        }
        var sequences = new List<long>();
        var transport = new RecordingTransport((message, _) =>
        {
            sequences.Add(long.Parse(message.Headers.Span.ToArray().Single(h => h.Key == OutboxHeaders.Sequence).Value.ToString()));
            return ValueTask.CompletedTask;
        });
        using var worker = Worker(database.Source, transport);
        await Assert.That(await worker.DispatchLaneAsync(0, default)).IsEqualTo(3);
        await Assert.That(transport.Ids.SequenceEqual(ids)).IsTrue();
        await Assert.That(sequences.SequenceEqual(sequences.Order())).IsTrue();
        await Assert.That(sequences.Distinct().Count()).IsEqualTo(3);
    }

    [Test]
    public async Task Competing_worker_waits_for_inflight_send_then_expired_lease_can_transfer()
    {
        await using var database = new MySqlFixture();
        await database.StartAsync();
        await Insert(database, Id(1));
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var transport = new RecordingTransport(async (_, _) => { entered.TrySetResult(); await release.Task; });
        using var first = Worker(database.Source, transport);
        var otherTransport = new RecordingTransport();
        using var second = Worker(database.Source, otherTransport);
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        var pending = first.DispatchLaneAsync(0, timeout.Token).AsTask();
        await entered.Task.WaitAsync(timeout.Token);
        var rival = second.DispatchLaneAsync(0, timeout.Token).AsTask();
        try
        {
            // Observe the actual database wait, rather than relying on a scheduling delay.
            while (await Scalar(database, "SELECT count(*) FROM performance_schema.data_lock_waits") == 0)
                await Task.Delay(10, timeout.Token);
            await Assert.That(rival.IsCompleted).IsFalse();
        }
        finally { release.TrySetResult(); }
        await Assert.That(await pending).IsEqualTo(1);
        await Assert.That(await rival).IsEqualTo(0);
        await Insert(database, Id(2));
        await Assert.That(await second.DispatchLaneAsync(0, timeout.Token)).IsEqualTo(0);
        await database.Execute("UPDATE relayer_leases SET expires_at = TIMESTAMPADD(SECOND, -1, UTC_TIMESTAMP(6)) WHERE name = 'lane:0'");
        await Assert.That(await second.DispatchLaneAsync(0, timeout.Token)).IsEqualTo(1);
        await Assert.That(otherTransport.Ids.SequenceEqual([Id(2)])).IsTrue();
    }

    [Test]
    public async Task Membership_rebalances_and_shutdown_releases_ownership_without_losing_rows()
    {
        await using var database = new MySqlFixture();
        await database.StartAsync();
        var options = new MySqlOrderedDispatchOptions { Lanes = 8 };
        using var a = new MySqlLaneCoordinator(database.Source, options);
        using var b = new MySqlLaneCoordinator(database.Source, options);
        await a.RebalanceAsync(default); await b.RebalanceAsync(default); await a.RebalanceAsync(default);
        await Assert.That(a.Assigned.Count(x => x)).IsEqualTo(4);
        await Assert.That(b.Assigned.Count(x => x)).IsEqualTo(4);
        await a.ReleaseAsync(default);
        await b.RebalanceAsync(default);
        await Assert.That(b.Assigned.All(x => x)).IsTrue();
        await b.ReleaseAsync(default);
        await Insert(database, Id(1));
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var transport = new RecordingTransport(async (_, ct) =>
        {
            entered.TrySetResult();
            await Task.Delay(Timeout.InfiniteTimeSpan, ct);
        });
        using var worker = Worker(database.Source, transport);
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        await worker.StartAsync(timeout.Token);
        await entered.Task.WaitAsync(timeout.Token);
        await worker.StopAsync(timeout.Token);
        await Assert.That(await Scalar(database, "SELECT count(*) FROM relayer_leases")).IsEqualTo(0L);
        await Assert.That(await Scalar(database, "SELECT count(*) FROM relayer_outbox")).IsEqualTo(1L);
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task Expired_or_replaced_claim_cannot_complete_or_record_failure(bool replace)
    {
        await using var database = new MySqlFixture();
        await database.StartAsync();
        await Insert(database, Id(1));
        using var lane = new MySqlOrderedLane(database.Source, new(), TimeSpan.FromSeconds(30), 10);
        await using var connection = await database.Source.OpenConnectionAsync();
        await using var transaction = await connection.BeginTransactionAsync(IsolationLevel.ReadCommitted);
        var claim = new OutboxClaim(Id(1), Guid.NewGuid());
        await Assert.That(await lane.ClaimAsync(connection, transaction, claim, default)).IsTrue();
        await using (var command = connection.CreateCommand())
        {
            command.Transaction = transaction;
            command.CommandText = replace ? "UPDATE relayer_outbox SET claim_token = UUID_TO_BIN(UUID())" : "UPDATE relayer_outbox SET claim_expires_at = TIMESTAMPADD(SECOND, -1, UTC_TIMESTAMP(6))";
            await command.ExecuteNonQueryAsync();
        }
        await Assert.That(await lane.CompleteAsync(connection, transaction, claim, default)).IsFalse();
        await Assert.That(await lane.FailAsync(connection, transaction, claim, "stale", default)).IsFalse();
    }

    [Test]
    public async Task Exhaustion_parks_original_sequence_and_does_not_block_another_key()
    {
        await using var database = new MySqlFixture();
        await database.StartAsync();
        await Insert(database, Id(3)); await Insert(database, Id(2)); await Insert(database, Id(1), "other");
        var fail = true;
        var transport = new RecordingTransport((message, _) => fail && message.OrderingKey!.Value.ToString() == "key"
            ? ValueTask.FromException(new InvalidOperationException("poison")) : ValueTask.CompletedTask);
        using var worker = new MySqlOrderedDispatcher(database.Source, new() { Lanes = 1, MaxDispatchAttempts = 1 }, [transport], NullLogger<MySqlOrderedDispatcher>.Instance);
        await Assert.That(await worker.DispatchLaneAsync(0, default)).IsEqualTo(0);
        await Assert.That(await worker.DispatchLaneAsync(0, default)).IsEqualTo(1);
        await Assert.That(transport.Ids.SequenceEqual([Id(1)])).IsTrue();
        fail = false;
        await database.Execute("UPDATE relayer_outbox SET status = 'pending', attempts = 0 WHERE status = 'parked'");
        await Assert.That(await worker.DispatchLaneAsync(0, default)).IsEqualTo(2);
        await Assert.That(transport.Ids.SequenceEqual([Id(1), Id(3), Id(2)])).IsTrue();
    }

    [Test]
    public async Task Upgrade_preserves_rows_and_assigns_UUID_order_then_continues_identity()
    {
        await using var database = new MySqlFixture();
        await database.StartAsync();
        await database.Execute("ALTER TABLE relayer_outbox DROP INDEX unordered_idx, DROP INDEX pending_idx, DROP COLUMN seq, ADD INDEX pending_idx (status, lane, id)");
        await Insert(database, Id(3)); await Insert(database, Id(2));
        await database.Execute(MySqlOutboxSchema.UpgradeOrderedScript());
        await Insert(database, Id(1));
        var transport = new RecordingTransport();
        using var worker = Worker(database.Source, transport);
        await Assert.That(await worker.DispatchLaneAsync(0, default)).IsEqualTo(3);
        await Assert.That(transport.Ids.SequenceEqual([Id(2), Id(3), Id(1)])).IsTrue();
    }

    [Test]
    public async Task Slow_healthy_batches_commit_a_bounded_prefix_then_resume_in_order()
    {
        await using var database = new MySqlFixture();
        await database.StartAsync();
        for (var number = 1; number <= 9; number++) await Insert(database, Id(number));
        var clock = new BudgetClock();
        var transport = new RecordingTransport((_, _) => { clock.Advance(); return ValueTask.CompletedTask; });
        using var worker = new MySqlOrderedDispatcher(database.Source, new() { Lanes = 1 }, [transport],
            NullLogger<MySqlOrderedDispatcher>.Instance, clock);
        await Assert.That(await worker.DispatchLaneAsync(0, default)).IsEqualTo(8);
        await Assert.That(await Scalar(database, "SELECT count(*) FROM relayer_outbox")).IsEqualTo(1L);
        await Assert.That(await worker.DispatchLaneAsync(0, default)).IsEqualTo(1);
        await Assert.That(transport.Ids.SequenceEqual(Enumerable.Range(1, 9).Select(Id))).IsTrue();
    }

    private sealed class BudgetClock : TimeProvider
    {
        private long _timestamp;
        public override long TimestampFrequency => TimeSpan.TicksPerSecond;
        public override long GetTimestamp() => _timestamp;
        internal void Advance() => _timestamp += TimeSpan.TicksPerSecond;
    }

    private static MySqlOrderedDispatcher Worker(MySqlDataSource source, RecordingTransport transport) =>
        new(source, new() { Lanes = 1 }, [transport], NullLogger<MySqlOrderedDispatcher>.Instance);
    private static Guid Id(int value) => Guid.Parse($"01900000-0000-7000-8000-{value:x12}");
    private static OutboxRecord Record(Guid id, string key = "key") => new(id, "events", "event;v=1",
        new OutgoingMessage("orders", new byte[] { 1 }, new KeyValuePair<string, WireString>[] { new("ce_id", new(id.ToString())) }, new WireString(key)), DateTimeOffset.UtcNow);
    private static async Task Insert(MySqlFixture database, Guid id, string key = "key")
    {
        var record = Record(id, key);
        var headers = new ArrayBufferWriter<byte>();
        DurableOutboxHeaderCodec.Write(headers, record.Message.Headers.Span);
        await using var connection = await database.Source.OpenConnectionAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = "INSERT INTO relayer_outbox (id, lane, ordering_key, transport, destination, message_type, headers, body, created_at) VALUES (@id, 0, @key, 'events', 'orders', 'event;v=1', @headers, @body, UTC_TIMESTAMP(6))";
        command.Parameters.AddWithValue("id", id.ToByteArray(bigEndian: true));
        command.Parameters.AddWithValue("key", key);
        command.Parameters.AddWithValue("headers", headers.WrittenMemory);
        command.Parameters.AddWithValue("body", record.Message.Body);
        await command.ExecuteNonQueryAsync();
    }
    private static async Task<long> Scalar(MySqlFixture database, string sql)
    {
        await using var command = database.Source.CreateCommand(sql);
        return Convert.ToInt64(await command.ExecuteScalarAsync());
    }
    private sealed class RecordingTransport(Func<OutgoingMessage, CancellationToken, ValueTask>? send = null) : ITransport
    {
        public string Name => "events";
        internal ConcurrentQueue<Guid> Ids { get; } = new();
        public TransportCapabilities GetCapabilities(string endpoint) => new() { OrderedByKey = true };
        public ValueTask<TopologyReport> ApplyTopologyAsync(TopologyMode mode, CancellationToken cancellationToken) => new(new TopologyReport([]));
        public async ValueTask SendAsync(OutgoingMessage message, CancellationToken cancellationToken)
        {
            if (send is not null) await send(message, cancellationToken);
            Ids.Enqueue(Guid.Parse(message.Headers.Span[0].Value.ToString()));
        }
        public ValueTask SendBatchAsync(ReadOnlyMemory<OutgoingMessage> messages, CancellationToken cancellationToken) => throw new NotSupportedException();
        public Task StartAsync(CancellationToken cancellationToken) => Task.CompletedTask;
        public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }
}
