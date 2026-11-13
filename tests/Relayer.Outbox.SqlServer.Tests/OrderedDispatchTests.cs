using System.Collections.Concurrent;
using System.Data;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Relayer.Transports;

namespace Relayer.Outbox.SqlServer.Tests;

[ClassDataSource<SqlServerFixture>(Shared = SharedType.PerTestSession)]
public sealed class OrderedDispatchTests(SqlServerFixture database)
{
    [Test]
    [Arguments("headers = 0x00")]
    [Arguments("destination = N' '")]
    public async Task Malformed_rows_count_attempts_and_park_only_the_original_key(string corruption)
    {
        var data = new StoreData(database);
        await data.Create();
        for (var i = 1; i <= 4; i++)
            await data.Insert(Id(i), DateTimeOffset.UtcNow, 0, orderedHeaders: true);
        // A valid predecessor must commit even when decoding a later row fails.
        await data.Execute($"UPDATE {data.Table} SET {corruption} WHERE seq = 2; " +
            $"UPDATE {data.Table} SET ordering_key = N'other' WHERE seq = 4");
        var transport = new RecordingTransport();
        using var worker = Worker(data, transport, maxAttempts: 2);
        await Assert.That(await worker.DispatchLaneAsync(0, default)).IsEqualTo(1);
        await Assert.That(await worker.DispatchLaneAsync(0, default)).IsEqualTo(0);
        await Assert.That(await worker.DispatchLaneAsync(0, default)).IsEqualTo(1);
        await Assert.That(transport.Ids.SequenceEqual([Id(1), Id(4)])).IsTrue();
        await using var connection = await Open();
        await using var query = new SqlCommand($"SELECT attempts, last_error, status, ordering_key FROM {data.Table} WHERE seq = 2", connection);
        await using var reader = await query.ExecuteReaderAsync();
        await Assert.That(await reader.ReadAsync()).IsTrue();
        await Assert.That(reader.GetInt32(0)).IsEqualTo(2);
        await Assert.That(reader.GetString(1).StartsWith("Malformed outbox row:", StringComparison.Ordinal)).IsTrue();
        await Assert.That(reader.GetString(2)).IsEqualTo("parked");
        await Assert.That(reader.GetString(3)).IsEqualTo("customer-é");
        using var lane = new SqlServerOrderedLane(database.ConnectionString, data.Options, TimeSpan.FromSeconds(30), 2);
        var ready = new bool[1];
        await lane.FindReadyAsync([true], ready, default);
        await Assert.That(ready[0]).IsFalse();
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task Bulk_flush_preserves_send_order_and_sequential_commits_despite_reversed_UUIDs(bool synchronous)
    {
        var data = new StoreData(database, new() { Lanes = 1, TablePrefix = TestContext.Current!.Isolation.GetIsolatedName("s") + "_" });
        await data.Create();
        await using var services = new ServiceCollection().AddRelayerSqlServerOutbox(data.Options).BuildServiceProvider();
        await using var scope = services.CreateAsyncScope();
        var outbox = scope.ServiceProvider.GetRequiredService<SqlServerOutbox>();
        var expected = new List<Guid>();
        for (var batch = 0; batch < 2; batch++)
        {
            await using var transaction = await outbox.BeginTransactionAsync(database.ConnectionString);
            for (var i = 0; i < 64; i++)
            {
                var id = Id(128 - (batch * 64 + i));
                expected.Add(id);
                await outbox.EnlistAsync(new(id, "events", "probe;v=1", new OutgoingMessage("orders", new byte[] { 1 },
                    new KeyValuePair<string, WireString>[] { new("ce_id", new(id.ToString())), new(OutboxHeaders.Sequence, new("spoofed")) }, new WireString("key")), DateTimeOffset.UtcNow), default);
            }
            if (synchronous) transaction.Commit(); else await transaction.CommitAsync();
        }
        var transport = new RecordingTransport();
        using var worker = Worker(data, transport, batchSize: 128);
        // A bounded dispatch may stop at its admission deadline on a busy database.
        // Drain with a progress assertion; ordering must hold across batch boundaries too.
        var dispatched = 0;
        while (dispatched < expected.Count)
        {
            var sent = await worker.DispatchLaneAsync(0, default);
            await Assert.That(sent).IsGreaterThan(0);
            dispatched += sent;
        }
        await Assert.That(dispatched).IsEqualTo(128);
        await Assert.That(transport.Ids.SequenceEqual(expected)).IsTrue();
        await Assert.That(transport.Sequences.Count).IsEqualTo(128);
        var sequences = transport.Sequences.ToArray();
        await Assert.That(sequences.All(value => value > 0)).IsTrue();
        await Assert.That(sequences.SequenceEqual(sequences.Order())).IsTrue();
        await Assert.That(sequences.Distinct().Count()).IsEqualTo(128);
    }

    [Test]
    [Arguments("CUSTOMER-É")]
    [Arguments("customer-é ")]
    public async Task Poison_parks_its_key_and_ready_probe_uses_sequence_and_exact_key(string otherKey)
    {
        var data = new StoreData(database);
        await data.Create();
        await data.Insert(Id(3), DateTimeOffset.UtcNow, 0, orderedHeaders: true);
        await data.Insert(Id(2), DateTimeOffset.UtcNow, 0, orderedHeaders: true);
        await data.Insert(Id(1), DateTimeOffset.UtcNow, 0, orderedHeaders: true);
        // SQL equality normally ignores trailing spaces even under BIN2; keys must match exactly.
        await using (var change = await Open())
        await using (var routing = new SqlCommand($"UPDATE {data.Table} SET ordering_key = CASE WHEN id = @id THEN @other ELSE N'customer-é' END", change))
        {
            routing.Parameters.Add("@id", SqlDbType.Binary, 16).Value = Id(1).ToByteArray(bigEndian: true);
            routing.Parameters.Add("@other", SqlDbType.NVarChar, -1).Value = otherKey;
            await routing.ExecuteNonQueryAsync();
        }
        // A malformed persisted row is a permanent preparation failure, not a broker outage.
        await data.Execute($"UPDATE {data.Table} SET headers = 0x00 WHERE seq = 1");
        var transport = new RecordingTransport();
        using var worker = Worker(data, transport, maxAttempts: 1);
        await Assert.That(await worker.DispatchLaneAsync(0, default)).IsEqualTo(0);
        using var lane = new SqlServerOrderedLane(database.ConnectionString, data.Options, TimeSpan.FromSeconds(30), 1);
        var ready = new bool[1];
        await lane.FindReadyAsync([true], ready, default);
        await Assert.That(ready[0]).IsTrue();
        await Assert.That(await worker.DispatchLaneAsync(0, default)).IsEqualTo(1);
        await Assert.That(transport.Ids.SequenceEqual([Id(1)])).IsTrue();
        await lane.FindReadyAsync([true], ready, default);
        await Assert.That(ready[0]).IsFalse();
        await using var connection = await Open();
        await using var query = new SqlCommand($"SELECT attempts, last_error, status FROM {data.Table} WHERE id = @id", connection);
        query.Parameters.Add("@id", SqlDbType.Binary, 16).Value = Id(3).ToByteArray(bigEndian: true);
        await using var reader = await query.ExecuteReaderAsync();
        await Assert.That(await reader.ReadAsync()).IsTrue();
        await Assert.That(reader.GetInt32(0)).IsEqualTo(1);
        await Assert.That(reader.GetString(1).StartsWith("Malformed outbox row:", StringComparison.Ordinal)).IsTrue();
        await Assert.That(reader.GetString(2)).IsEqualTo("parked");
    }

    [Test]
    public async Task Live_lane_owner_excludes_another_worker_then_expired_owner_is_fenced()
    {
        var data = new StoreData(database);
        await data.Create();
        await data.Insert(Id(1), DateTimeOffset.UtcNow, 0, orderedHeaders: true);
        var one = new RecordingTransport();
        var two = new RecordingTransport();
        using var first = Worker(data, one);
        using var second = Worker(data, two);
        await Assert.That(await first.DispatchLaneAsync(0, default)).IsEqualTo(1);
        await data.Insert(Id(2), DateTimeOffset.UtcNow, 0, orderedHeaders: true);
        await Assert.That(await second.DispatchLaneAsync(0, default)).IsEqualTo(0);
        await data.Execute($"UPDATE {SqlServerOutboxSchema.OrderedLeaseTable(data.Options)} SET expires_at = DATEADD(second, -1, SYSUTCDATETIME())");
        await Assert.That(await second.DispatchLaneAsync(0, default)).IsEqualTo(1);
        await data.Insert(Id(3), DateTimeOffset.UtcNow, 0, orderedHeaders: true);
        await Assert.That(await first.DispatchLaneAsync(0, default)).IsEqualTo(0);
        await Assert.That(one.Ids.SequenceEqual([Id(1)])).IsTrue();
        await Assert.That(two.Ids.SequenceEqual([Id(2)])).IsTrue();
    }

    [Test]
    public async Task Expiry_does_not_steal_a_lane_while_its_native_transaction_is_locked()
    {
        var data = new StoreData(database);
        await data.Create();
        var options = Options(data);
        var first = new SqlServerLaneCoordinator(database.ConnectionString, options);
        var second = new SqlServerLaneCoordinator(database.ConnectionString, options);
        await using var connection = await Open();
        await using var transaction = (SqlTransaction)await connection.BeginTransactionAsync();
        await Assert.That(await first.AcquireAsync(connection, transaction, 0, default)).IsTrue();
        await using (var expire = new SqlCommand($"UPDATE {SqlServerOutboxSchema.OrderedLeaseTable(data.Options)} SET expires_at = DATEADD(second, -1, SYSUTCDATETIME())", connection, transaction))
            await expire.ExecuteNonQueryAsync();
        await using var rival = await Open();
        await using var rivalTransaction = (SqlTransaction)await rival.BeginTransactionAsync();
        using var cancellation = new CancellationTokenSource();
        var blocked = second.AcquireAsync(rival, rivalTransaction, 0, cancellation.Token).AsTask();
        await WaitForContention(data.Options.TablePrefix);
        cancellation.Cancel();
        var cancelled = false;
        try { await blocked; }
        catch (OperationCanceledException) { cancelled = true; }
        catch (SqlException error) when (cancellation.IsCancellationRequested && error.Message.Contains("Operation cancelled by user", StringComparison.Ordinal)) { cancelled = true; }
        await Assert.That(cancelled).IsTrue();
        await transaction.CommitAsync();
        // Retry in a new transaction after cancellation. Expiry is now visible and takeover succeeds.
        await rivalTransaction.RollbackAsync();
        await using var retry = (SqlTransaction)await rival.BeginTransactionAsync();
        await Assert.That(await second.AcquireAsync(rival, retry, 0, default)).IsTrue();
        await retry.CommitAsync();
    }

    [Test]
    public async Task Concurrent_workers_never_overlap_native_sends_on_a_live_lane()
    {
        var data = new StoreData(database);
        await data.Create();
        await data.Insert(Id(1), DateTimeOffset.UtcNow, 0, orderedHeaders: true);
        await data.Insert(Id(2), DateTimeOffset.UtcNow, 0, orderedHeaders: true);
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var one = new RecordingTransport(async (_, token) => { entered.TrySetResult(); await release.Task.WaitAsync(token); });
        var two = new RecordingTransport();
        using var first = Worker(data, one, batchSize: 1);
        using var second = Worker(data, two, batchSize: 1);
        using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(20));
        var owner = first.DispatchLaneAsync(0, deadline.Token).AsTask();
        Task<int>? contender = null;
        try
        {
            await entered.Task.WaitAsync(deadline.Token);
            contender = second.DispatchLaneAsync(0, deadline.Token).AsTask();
            await WaitForContention(data.Options.TablePrefix);
            await Assert.That(two.Ids.Count).IsEqualTo(0);
        }
        finally { release.TrySetResult(); }
        await Assert.That(await owner).IsEqualTo(1);
        await Assert.That(await contender!).IsEqualTo(0);
        await data.Execute($"UPDATE {SqlServerOutboxSchema.OrderedLeaseTable(data.Options)} SET expires_at = DATEADD(second, -1, SYSUTCDATETIME())");
        await Assert.That(await second.DispatchLaneAsync(0, default)).IsEqualTo(1);
        await Assert.That(one.Ids.SequenceEqual([Id(1)])).IsTrue();
        await Assert.That(two.Ids.SequenceEqual([Id(2)])).IsTrue();
    }

    [Test]
    public async Task Membership_balances_lanes_and_releases_only_its_own_rows()
    {
        var data = new StoreData(database);
        await data.Create();
        var options = new SqlServerOrderedDispatchOptions { TablePrefix = data.Options.TablePrefix, Lanes = 8 };
        var first = new SqlServerLaneCoordinator(database.ConnectionString, options);
        var second = new SqlServerLaneCoordinator(database.ConnectionString, options);
        await first.RebalanceAsync(default);
        await second.RebalanceAsync(default);
        await first.RebalanceAsync(default);
        await Assert.That(first.Assigned.Count(value => value)).IsEqualTo(4);
        await Assert.That(second.Assigned.Count(value => value)).IsEqualTo(4);
        await Assert.That(first.Assigned.Zip(second.Assigned).All(pair => pair.First != pair.Second)).IsTrue();
        await first.ReleaseAsync(default);
        await second.RebalanceAsync(default);
        await Assert.That(second.Assigned.All(value => value)).IsTrue();
        await data.Execute($"UPDATE {SqlServerOutboxSchema.OrderedLeaseTable(data.Options)} SET expires_at = DATEADD(second, -1, SYSUTCDATETIME())");
        await first.RebalanceAsync(default);
        await Assert.That(first.Assigned.All(value => value)).IsTrue();
    }

    [Test]
    public async Task Shutdown_cancels_send_retains_rows_and_releases_membership_and_lane()
    {
        var data = new StoreData(database);
        await data.Create();
        await data.Insert(Id(1), DateTimeOffset.UtcNow, 0, orderedHeaders: true);
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var transport = new RecordingTransport(async (_, token) => { entered.SetResult(); await Task.Delay(Timeout.InfiniteTimeSpan, token); });
        using var worker = Worker(data, transport);
        await worker.StartAsync(default);
        try { await entered.Task.WaitAsync(TimeSpan.FromSeconds(15)); }
        finally
        {
            using var shutdown = new CancellationTokenSource(TimeSpan.FromSeconds(10));
            await worker.StopAsync(shutdown.Token);
        }
        var healthy = new RecordingTransport();
        using var replacement = Worker(data, healthy);
        await Assert.That(await replacement.DispatchLaneAsync(0, default)).IsEqualTo(1);
        await Assert.That(healthy.Ids.SequenceEqual([Id(1)])).IsTrue();
    }

    [Test]
    public async Task Expired_or_replaced_message_claim_cannot_complete_or_record_failure()
    {
        var data = new StoreData(database);
        await data.Create();
        await data.Insert(Id(1), DateTimeOffset.UtcNow, 0, orderedHeaders: true);
        using var lane = new SqlServerOrderedLane(database.ConnectionString, data.Options, TimeSpan.FromSeconds(30), 2);
        await using var connection = await Open();
        await using var transaction = (SqlTransaction)await connection.BeginTransactionAsync();
        var claim = new OutboxClaim(Id(1), Guid.NewGuid());
        await Assert.That(await lane.ClaimAsync(connection, transaction, claim, default)).IsTrue();
        var wrong = new OutboxClaim(Id(1), Guid.NewGuid());
        await Assert.That(await lane.CompleteAsync(connection, transaction, wrong, default)).IsFalse();
        await Assert.That(await lane.FailAsync(connection, transaction, wrong, "wrong", default)).IsFalse();
        await using (var expire = new SqlCommand($"UPDATE {data.Table} SET claim_expires_at = DATEADD(second, -1, SYSUTCDATETIME())", connection, transaction))
            await expire.ExecuteNonQueryAsync();
        await Assert.That(await lane.CompleteAsync(connection, transaction, claim, default)).IsFalse();
        await Assert.That(await lane.FailAsync(connection, transaction, claim, "expired", default)).IsFalse();
        var next = new OutboxClaim(Id(1), Guid.NewGuid());
        await Assert.That(await lane.ClaimAsync(connection, transaction, next, default)).IsTrue();
        await Assert.That(await lane.CompleteAsync(connection, transaction, claim, default)).IsFalse();
        await Assert.That(await lane.CompleteAsync(connection, transaction, next, default)).IsTrue();
        await transaction.CommitAsync();
    }

    [Test]
    public async Task Named_options_bind_independently_and_fail_early()
    {
        var configuration = new ConfigurationBuilder().AddInMemoryCollection(new Dictionary<string, string?>
        { ["Lanes"] = "8", ["BatchSize"] = "17", ["TablePrefix"] = "orders_" }).Build();
        var services = new ServiceCollection().AddLogging();
        services.AddSingleton<ITransport>(new RecordingTransport());
        services.AddRelayerSqlServerOrderedDispatch("orders", "Server=unused", configuration);
        services.AddRelayerSqlServerOrderedDispatch("billing", "Server=unused", new SqlServerOrderedDispatchOptions { Lanes = 2 });
        await using var provider = services.BuildServiceProvider();
        var monitor = provider.GetRequiredService<IOptionsMonitor<SqlServerOrderedDispatchOptions>>();
        await Assert.That(monitor.Get("orders").Lanes).IsEqualTo(8);
        await Assert.That(monitor.Get("orders").BatchSize).IsEqualTo(17);
        await Assert.That(monitor.Get("billing").Lanes).IsEqualTo(2);
        await Assert.That(provider.GetServices<IHostedService>().Count()).IsEqualTo(2);
        await Assert.That(() => services.AddRelayerSqlServerOrderedDispatch("orders", "Server=unused")).Throws<InvalidOperationException>();
        await Assert.That(() => new SqlServerOrderedDispatchOptions { Lanes = 0 }.Validate()).Throws<ArgumentOutOfRangeException>();
        await Assert.That(() => new SqlServerOrderedDispatchOptions { BatchSize = 0 }.Validate()).Throws<ArgumentOutOfRangeException>();
        await Assert.That(() => new SqlServerOrderedDispatchOptions { LeaseDuration = TimeSpan.Zero }.Validate()).Throws<ArgumentOutOfRangeException>();
        await Assert.That(() => new SqlServerOrderedDispatchOptions { PollInterval = TimeSpan.FromSeconds(10) }.Validate()).Throws<ArgumentOutOfRangeException>();
        await Assert.That(() => new SqlServerOrderedDispatcher("Server=unused", new(), [], NullLogger<SqlServerOrderedDispatcher>.Instance)).Throws<InvalidOperationException>();
    }

    private SqlServerOrderedDispatcher Worker(StoreData data, RecordingTransport transport, int batchSize = 100, int maxAttempts = 10) =>
        new(database.ConnectionString, Options(data, batchSize, maxAttempts), [transport], NullLogger<SqlServerOrderedDispatcher>.Instance);
    private static SqlServerOrderedDispatchOptions Options(StoreData data, int batchSize = 100, int maxAttempts = 10) => new()
    { Schema = data.Options.Schema, TablePrefix = data.Options.TablePrefix, Lanes = 1, BatchSize = batchSize, MaxDispatchAttempts = maxAttempts };
    private async Task WaitForContention(string prefix)
    {
        using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(20));
        await using var connection = await Open();
        await using var query = new SqlCommand("SELECT COUNT_BIG(*) FROM sys.dm_exec_requests r CROSS APPLY sys.dm_exec_sql_text(r.sql_handle) t WHERE r.wait_type LIKE 'LCK%' AND t.text LIKE @pattern", connection);
        query.Parameters.Add("@pattern", SqlDbType.NVarChar, -1).Value = "%" + prefix + "ordered_leases%";
        while ((long)(await query.ExecuteScalarAsync(deadline.Token))! == 0)
            await Task.Delay(10, deadline.Token);
    }

    private async Task<SqlConnection> Open()
    {
        var connection = new SqlConnection(database.ConnectionString);
        await connection.OpenAsync();
        return connection;
    }
    private static Guid Id(int number) => Guid.Parse($"01900000-0000-7000-8000-{number:x12}");

    private sealed class RecordingTransport(Func<OutgoingMessage, CancellationToken, ValueTask>? send = null) : ITransport
    {
        internal Func<OutgoingMessage, CancellationToken, ValueTask>? Send { get; set; } = send;
        internal ConcurrentQueue<Guid> Ids { get; } = new();
        internal ConcurrentQueue<long> Sequences { get; } = new();
        public string Name => "events";
        public TransportCapabilities GetCapabilities(string endpoint) => new() { OrderedByKey = true };
        public async ValueTask SendAsync(OutgoingMessage message, CancellationToken cancellationToken)
        {
            if (Send is not null) await Send(message, cancellationToken);
            foreach (var header in message.Headers.Span)
            {
                if (header.Key == "ce_id") Ids.Enqueue(Guid.Parse(header.Value.ToString()));
                if (header.Key == OutboxHeaders.Sequence) Sequences.Enqueue(long.Parse(header.Value.ToString(), System.Globalization.CultureInfo.InvariantCulture));
            }
        }
        public ValueTask SendBatchAsync(ReadOnlyMemory<OutgoingMessage> messages, CancellationToken token) => throw new NotSupportedException();
        public ValueTask<TopologyReport> ApplyTopologyAsync(TopologyMode mode, CancellationToken token) => new(new TopologyReport([]));
        public Task StartAsync(CancellationToken token) => Task.CompletedTask;
        public Task StopAsync(CancellationToken token) => Task.CompletedTask;
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }
}
