using System.Buffers;
using System.Collections.Concurrent;
using System.Threading.Channels;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Npgsql;
using NpgsqlTypes;
using Relayer.Transports;

namespace Relayer.Outbox.PostgreSql.Tests;

public class OrderedDispatchTests
{
    [Test]
    public async Task Named_immutable_options_bind_independently_and_registration_validates()
    {
        await using var source = NpgsqlDataSource.Create("Host=unused");
        var services = new ServiceCollection().AddLogging();
        services.AddSingleton<ITransport>(new RecordingTransport());
        var configuration = new ConfigurationBuilder().AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["Lanes"] = "8", ["BatchSize"] = "17", ["Schema"] = "custom", ["TablePrefix"] = "orders_"
        }).Build();
        services.AddRelayerPostgreSqlOrderedDispatch("orders", source, configuration);
        services.AddRelayerPostgreSqlOrderedDispatch("billing", source, new PostgreSqlOrderedDispatchOptions { Lanes = 2 });
        await using var provider = services.BuildServiceProvider();
        var monitor = provider.GetRequiredService<IOptionsMonitor<PostgreSqlOrderedDispatchOptions>>();
        await Assert.That(monitor.Get("orders").Lanes).IsEqualTo(8);
        await Assert.That(monitor.Get("orders").BatchSize).IsEqualTo(17);
        await Assert.That(monitor.Get("billing").Lanes).IsEqualTo(2);
        await Assert.That(provider.GetServices<IHostedService>().Count()).IsEqualTo(2);
        await Assert.That(() => services.AddRelayerPostgreSqlOrderedDispatch("orders", source)).Throws<InvalidOperationException>();
        await Assert.That(() => new PostgreSqlOrderedDispatchOptions { Lanes = 0 }.Validate()).Throws<ArgumentOutOfRangeException>();
        await Assert.That(() => new PostgreSqlOrderedDispatchOptions { BatchSize = 0 }.Validate()).Throws<ArgumentOutOfRangeException>();
        await Assert.That(() => new PostgreSqlOrderedDispatchOptions { LeaseDuration = TimeSpan.Zero }.Validate()).Throws<ArgumentOutOfRangeException>();
        await Assert.That(() => new PostgreSqlOrderedDispatchOptions { PollInterval = TimeSpan.FromSeconds(10) }.Validate()).Throws<ArgumentOutOfRangeException>();
    }

    [Test]
    public async Task Membership_heartbeats_assign_fair_shares_and_remove_crashed_members()
    {
        await using var fixture = new PostgreSqlFixture();
        await fixture.StartAsync();
        await using var source = NpgsqlDataSource.Create(fixture.ConnectionString);
        var options = new PostgreSqlOrderedDispatchOptions { Lanes = 8 };
        await PostgreSqlOutboxStoreTests.CreateSchema(source);
        var workers = new[] { new PostgreSqlLaneCoordinator(source, options), new(source, options), new(source, options) };
        foreach (var worker in workers) await worker.HeartbeatAsync(default);
        foreach (var worker in workers) await worker.RebalanceAsync(default);
        foreach (var worker in workers)
            await Assert.That(worker.Assigned.Count(static assigned => assigned) is 2 or 3).IsTrue();
        for (var lane = 0; lane < 8; lane++)
            await Assert.That(workers.Count(worker => worker.Assigned[lane])).IsEqualTo(1);
        await workers[0].ReleaseAsync(default);
        await using (var expire = source.CreateCommand("UPDATE relayer_leases SET expires_at = clock_timestamp() - interval '1 second' WHERE name LIKE 'member:%'"))
            await expire.ExecuteNonQueryAsync();
        await workers[1].RebalanceAsync(default);
        await Assert.That(workers[1].Assigned.All(static assigned => assigned)).IsTrue();
        await using var count = source.CreateCommand("SELECT count(*) FROM relayer_leases WHERE name LIKE 'member:%'");
        await Assert.That((long)(await count.ExecuteScalarAsync())!).IsEqualTo(1);
    }

    [Test]
    public async Task Competing_owners_cannot_send_while_prior_native_send_is_in_flight()
    {
        await using var fixture = new PostgreSqlFixture();
        await fixture.StartAsync();
        await using var source = NpgsqlDataSource.Create(fixture.ConnectionString);
        await PostgreSqlOutboxStoreTests.CreateSchema(source);
        await Insert(source, Id(1), 0);
        await Insert(source, Id(2), 0);
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var firstTransport = new RecordingTransport(async (_, _) => { entered.TrySetResult(); await release.Task; });
        using var first = Worker(source, firstTransport);
        using var second = Worker(source, new RecordingTransport());
        using var cancellation = new CancellationTokenSource(TimeSpan.FromSeconds(20));
        var pending = first.DispatchLaneAsync(0, cancellation.Token).AsTask();
        await entered.Task.WaitAsync(cancellation.Token);
        var rival = second.DispatchLaneAsync(0, cancellation.Token).AsTask();
        try
        {
            await WaitForLock(source, cancellation.Token);
            await Assert.That(rival.IsCompleted).IsFalse();
        }
        finally { release.TrySetResult(); }
        await Assert.That(await pending).IsEqualTo(2);
        await Assert.That(await rival).IsEqualTo(0);
        await Assert.That(firstTransport.Ids.SequenceEqual([Id(1), Id(2)])).IsTrue();
    }

    [Test]
    public async Task Failed_head_blocks_later_rows_and_retries_preserve_native_payload()
    {
        await using var fixture = new PostgreSqlFixture();
        await fixture.StartAsync();
        await using var source = NpgsqlDataSource.Create(fixture.ConnectionString);
        await PostgreSqlOutboxStoreTests.CreateSchema(source);
        await Insert(source, Id(1), 0);
        await Insert(source, Id(2), 0);
        await Insert(source, Id(3), null);
        var fail = true;
        var transport = new RecordingTransport((message, _) =>
        {
            if (fail) throw new InvalidOperationException("broker offline");
            return ValueTask.CompletedTask;
        });
        using var worker = Worker(source, transport);
        await Assert.That(await worker.DispatchLaneAsync(0, default)).IsEqualTo(0);
        await using (var error = source.CreateCommand("SELECT attempts::text || ':' || last_error FROM relayer_outbox WHERE id = @id"))
        {
            error.Parameters.AddWithValue("id", Id(1));
            await Assert.That((string)(await error.ExecuteScalarAsync())!).IsEqualTo("1:broker offline");
        }
        fail = false;
        await Assert.That(await worker.DispatchLaneAsync(0, default)).IsEqualTo(2);
        await Assert.That(transport.Ids.SequenceEqual([Id(1), Id(2)])).IsTrue();
        await Assert.That(transport.LastBody).IsEquivalentTo(new byte[] { 7, 8, 9 });
        await Assert.That(transport.LastKey).IsEqualTo("customer-é");
        await Assert.That(transport.LastDeliverAt).IsNull();
        await using var remaining = source.CreateCommand("SELECT count(*) FROM relayer_outbox WHERE lane IS NULL");
        await Assert.That((long)(await remaining.ExecuteScalarAsync())!).IsEqualTo(1);
    }

    [Test]
    [Arguments("unknown_transport")]
    [Arguments("missing_key")]
    [Arguments("unsupported_endpoint")]
    public async Task Preparation_failure_commits_successful_prefix_and_stops_before_later_rows(string kind)
    {
        await using var fixture = new PostgreSqlFixture();
        await fixture.StartAsync();
        await using var source = NpgsqlDataSource.Create(fixture.ConnectionString);
        await PostgreSqlOutboxStoreTests.CreateSchema(source);
        for (var index = 1; index <= 3; index++) await Insert(source, Id(index), 0);
        var change = kind switch
        {
            "unknown_transport" => "transport = 'missing'",
            "missing_key" => "ordering_key = NULL",
            _ => "destination = 'unsupported'"
        };
        await using (var command = source.CreateCommand($"UPDATE relayer_outbox SET {change} WHERE id = @id"))
        {
            command.Parameters.AddWithValue("id", Id(2));
            await command.ExecuteNonQueryAsync();
        }
        var transport = new RecordingTransport();
        using var worker = Worker(source, transport);
        await Assert.That(await worker.DispatchLaneAsync(0, default)).IsEqualTo(1);
        await Assert.That(await worker.DispatchLaneAsync(0, default)).IsEqualTo(0);
        await Assert.That(transport.Ids.SequenceEqual([Id(1)])).IsTrue();
        await using (var command = source.CreateCommand("SELECT id, attempts, last_error, claim_token, claim_expires_at FROM relayer_outbox ORDER BY id"))
        await using (var reader = await command.ExecuteReaderAsync())
        {
            await Assert.That(await reader.ReadAsync()).IsTrue();
            await Assert.That(reader.GetGuid(0)).IsEqualTo(Id(2));
            await Assert.That(reader.GetInt32(1)).IsEqualTo(2);
            await Assert.That(reader.GetString(2).Length > 0).IsTrue();
            await Assert.That(reader.IsDBNull(3) && reader.IsDBNull(4)).IsTrue();
            await Assert.That(await reader.ReadAsync()).IsTrue();
            await Assert.That(reader.GetGuid(0)).IsEqualTo(Id(3));
            await Assert.That(reader.GetInt32(1)).IsEqualTo(0);
            await Assert.That(await reader.ReadAsync()).IsFalse();
        }
        await using (var command = source.CreateCommand("UPDATE relayer_outbox SET transport = 'events', destination = 'orders', ordering_key = 'customer-é' WHERE id = @id"))
        {
            command.Parameters.AddWithValue("id", Id(2));
            await command.ExecuteNonQueryAsync();
        }
        await Assert.That(await worker.DispatchLaneAsync(0, default)).IsEqualTo(2);
        await Assert.That(transport.Ids.SequenceEqual([Id(1), Id(2), Id(3)])).IsTrue();
    }

    [Test]
    [Arguments("future")]
    [Arguments("claimed")]
    [Arguments("parked")]
    public async Task Ineligible_head_is_never_skipped(string kind)
    {
        await using var fixture = new PostgreSqlFixture();
        await fixture.StartAsync();
        await using var source = NpgsqlDataSource.Create(fixture.ConnectionString);
        await PostgreSqlOutboxStoreTests.CreateSchema(source);
        await Insert(source, Id(1), 0);
        await Insert(source, Id(2), 0);
        var change = kind switch
        {
            "future" => "deliver_at = clock_timestamp() + interval '1 day'",
            "claimed" => "claim_token = gen_random_uuid(), claim_expires_at = clock_timestamp() + interval '1 day'",
            _ => "status = 'parked'"
        };
        await using (var command = source.CreateCommand($"UPDATE relayer_outbox SET {change} WHERE id = @id"))
        {
            command.Parameters.AddWithValue("id", Id(1));
            await command.ExecuteNonQueryAsync();
        }
        var transport = new RecordingTransport();
        using var worker = Worker(source, transport);
        await Assert.That(await worker.DispatchLaneAsync(0, default)).IsEqualTo(0);
        await Assert.That(transport.Ids.Count).IsEqualTo(0);
        await using (var command = source.CreateCommand("UPDATE relayer_outbox SET status = 'pending', deliver_at = clock_timestamp(), claim_token = NULL, claim_expires_at = NULL WHERE id = @id"))
        {
            command.Parameters.AddWithValue("id", Id(1));
            await command.ExecuteNonQueryAsync();
        }
        await Assert.That(await worker.DispatchLaneAsync(0, default)).IsEqualTo(2);
    }

    [Test]
    public async Task Database_connection_crash_replays_head_before_later_rows_during_rebalance()
    {
        await using var fixture = new PostgreSqlFixture();
        await fixture.StartAsync();
        await using var source = NpgsqlDataSource.Create(fixture.ConnectionString);
        await PostgreSqlOutboxStoreTests.CreateSchema(source);
        for (var i = 1; i <= 20; i++) await Insert(source, Id(i), (i - 1) % 4);
        var observed = new ConcurrentQueue<Guid>();
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var crashing = new RecordingTransport(async (message, _) =>
        {
            observed.Enqueue(HeaderId(message));
            entered.TrySetResult();
            await release.Task;
        });
        using var first = Worker(source, crashing, lanes: 4);
        using var cancellation = new CancellationTokenSource(TimeSpan.FromSeconds(20));
        var pending = first.DispatchLaneAsync(0, cancellation.Token).AsTask();
        await entered.Task.WaitAsync(cancellation.Token);
        try
        {
            await using var terminate = source.CreateCommand("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE state = 'idle in transaction' AND query LIKE 'UPDATE %SET claim_token%'");
            await terminate.ExecuteNonQueryAsync(cancellation.Token);
        }
        finally { release.TrySetResult(); }
        await Assert.ThrowsAsync<NpgsqlException>(async () => await pending);
        var recovered = new RecordingTransport((message, _) => { observed.Enqueue(HeaderId(message)); return ValueTask.CompletedTask; });
        using var second = Worker(source, recovered, lanes: 4);
        // Repeated ownership changes: each transfer uses the existing lease, then an expired lease simulates a lost instance.
        for (var lane = 0; lane < 4; lane++)
        {
            await using (var expire = source.CreateCommand("UPDATE relayer_leases SET expires_at = clock_timestamp() - interval '1 second' WHERE name LIKE 'lane:%'"))
                await expire.ExecuteNonQueryAsync();
            await Assert.That(await second.DispatchLaneAsync(lane, cancellation.Token)).IsEqualTo(5);
        }
        var seen = new HashSet<Guid>();
        var last = new int[4];
        foreach (var id in observed)
        {
            if (!seen.Add(id)) continue; // Crash replay is at-least-once, with the original identity.
            var number = int.Parse(id.ToString("N")[24..], System.Globalization.NumberStyles.HexNumber);
            var lane = (number - 1) % 4;
            await Assert.That(number > last[lane]).IsTrue();
            last[lane] = number;
        }
        await Assert.That(seen.Count).IsEqualTo(20);
        await Assert.That(observed.Count).IsEqualTo(21);
    }

    [Test]
    public async Task Parked_keys_do_not_block_other_keys_and_unsupported_native_ordering_fails()
    {
        await using var fixture = new PostgreSqlFixture();
        await fixture.StartAsync();
        await using var source = NpgsqlDataSource.Create(fixture.ConnectionString);
        await PostgreSqlOutboxStoreTests.CreateSchema(source);
        await Insert(source, Id(1), 0);
        await Insert(source, Id(2), 0);
        await Insert(source, Id(3), 0);
        await using (var park = source.CreateCommand("UPDATE relayer_outbox SET status = 'parked' WHERE id = @first; UPDATE relayer_outbox SET ordering_key = 'other-key' WHERE id = @last"))
        {
            park.Parameters.AddWithValue("first", Id(1));
            park.Parameters.AddWithValue("last", Id(3));
            await park.ExecuteNonQueryAsync();
        }
        var transport = new RecordingTransport();
        using var worker = Worker(source, transport);
        await Assert.That(await worker.DispatchLaneAsync(0, default)).IsEqualTo(1);
        await Assert.That(transport.Ids.Single()).IsEqualTo(Id(3));
        await Insert(source, Id(4), 1);
        var unsupported = new RecordingTransport { Ordered = false };
        using var invalid = Worker(source, unsupported, 2);
        await Assert.That(await invalid.DispatchLaneAsync(1, default)).IsEqualTo(0);
        await Assert.That(unsupported.Ids.Count).IsEqualTo(0);
    }

    [Test]
    public async Task Hosted_shutdown_drains_cancelled_send_then_releases_membership_and_lane()
    {
        await using var fixture = new PostgreSqlFixture();
        await fixture.StartAsync();
        await using var source = NpgsqlDataSource.Create(fixture.ConnectionString);
        await PostgreSqlOutboxStoreTests.CreateSchema(source);
        await Insert(source, Id(1), 0);
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var cancelled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var transport = new RecordingTransport(async (_, cancellationToken) =>
        {
            entered.TrySetResult();
            try { await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken); }
            finally { cancelled.TrySetResult(); }
        });
        using var worker = Worker(source, transport);
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(20));
        await worker.StartAsync(timeout.Token);
        await entered.Task.WaitAsync(timeout.Token);
        await worker.StopAsync(timeout.Token);
        await cancelled.Task.WaitAsync(timeout.Token);
        await using var count = source.CreateCommand("SELECT (SELECT count(*) FROM relayer_leases), (SELECT count(*) FROM relayer_outbox)");
        await using var reader = await count.ExecuteReaderAsync(timeout.Token);
        await reader.ReadAsync(timeout.Token);
        await Assert.That(reader.GetInt64(0)).IsEqualTo(0);
        await Assert.That(reader.GetInt64(1)).IsEqualTo(1);
    }

    [Test]
    public async Task Slow_healthy_batches_commit_prefix_and_eventually_drain()
    {
        await using var fixture = new PostgreSqlFixture();
        await fixture.StartAsync();
        await using var source = NpgsqlDataSource.Create(fixture.ConnectionString);
        await PostgreSqlOutboxStoreTests.CreateSchema(source);
        for (var number = 1; number <= 20; number++) await Insert(source, Id(number), 0);
        var clock = new BudgetClock();
        var transport = new RecordingTransport((_, _) =>
        {
            clock.Advance(TimeSpan.FromSeconds(1));
            return ValueTask.CompletedTask;
        });
        using var worker = new PostgreSqlOrderedDispatcher(source, new(), [transport],
            NullLogger<PostgreSqlOrderedDispatcher>.Instance, clock);
        await Assert.That(await worker.DispatchLaneAsync(0, default)).IsEqualTo(8);
        await using (var count = source.CreateCommand("SELECT count(*) FROM relayer_outbox"))
            await Assert.That((long)(await count.ExecuteScalarAsync())!).IsEqualTo(12);
        await Assert.That(await worker.DispatchLaneAsync(0, default)).IsEqualTo(8);
        await Assert.That(await worker.DispatchLaneAsync(0, default)).IsEqualTo(4);
        await Assert.That(transport.Ids.SequenceEqual(Enumerable.Range(1, 20).Select(Id))).IsTrue();
        await using var final = source.CreateCommand("SELECT count(*) FROM relayer_outbox");
        await Assert.That((long)(await final.ExecuteScalarAsync())!).IsEqualTo(0);
    }

    [Test]
    public async Task Hosted_worker_drains_backlog_without_waiting_for_poll_timer()
    {
        await using var fixture = new PostgreSqlFixture();
        await fixture.StartAsync();
        await using var source = NpgsqlDataSource.Create(fixture.ConnectionString);
        await PostgreSqlOutboxStoreTests.CreateSchema(source);
        for (var number = 1; number <= 5; number++) await Insert(source, Id(number), 0);
        var delivered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var clock = new PollClock();
        var transport = new RecordingTransport((message, _) =>
        {
            if (HeaderId(message) == Id(5)) delivered.TrySetResult();
            return ValueTask.CompletedTask;
        });
        using var worker = new PostgreSqlOrderedDispatcher(source,
            new() { Lanes = 1, BatchSize = 2, PollInterval = TimeSpan.FromSeconds(9) },
            [transport], NullLogger<PostgreSqlOrderedDispatcher>.Instance, clock);
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(20));
        await worker.StartAsync(timeout.Token);
        try
        {
            var first = await Task.WhenAny(delivered.Task, clock.FirstPoll.Task).WaitAsync(timeout.Token);
            await Assert.That(first).IsSameReferenceAs(delivered.Task);
            while (true)
            {
                await using var count = source.CreateCommand("SELECT count(*) FROM relayer_outbox");
                if ((long)(await count.ExecuteScalarAsync(timeout.Token))! == 0) break;
                await Task.Yield();
            }
        }
        finally { await worker.StopAsync(timeout.Token); }
        await Assert.That(transport.Ids.SequenceEqual(Enumerable.Range(1, 5).Select(Id))).IsTrue();
    }

    [Test]
    public async Task Idle_scans_do_not_write_lane_leases_and_refresh_membership_only_when_due()
    {
        await using var fixture = new PostgreSqlFixture();
        await fixture.StartAsync();
        await using var source = NpgsqlDataSource.Create(fixture.ConnectionString);
        await PostgreSqlOutboxStoreTests.CreateSchema(source);
        var clock = new ScanClock();
        using var worker = new PostgreSqlOrderedDispatcher(source, new(), [new RecordingTransport()],
            NullLogger<PostgreSqlOrderedDispatcher>.Instance, clock);
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(20));
        await worker.StartAsync(timeout.Token);
        try
        {
            var poll = await clock.Polls.Reader.ReadAsync(timeout.Token);
            await using var lanes = source.CreateCommand("SELECT count(*) FROM relayer_leases WHERE name LIKE 'lane:%'");
            await Assert.That((long)(await lanes.ExecuteScalarAsync(timeout.Token))!).IsEqualTo(0);
            await using var member = source.CreateCommand("SELECT xmin::text FROM relayer_leases WHERE name LIKE 'member:%'");
            var firstVersion = (string)(await member.ExecuteScalarAsync(timeout.Token))!;
            poll.Callback(poll.State);
            poll = await clock.Polls.Reader.ReadAsync(timeout.Token);
            await Assert.That((string)(await member.ExecuteScalarAsync(timeout.Token))!).IsEqualTo(firstVersion);
            clock.Advance(TimeSpan.FromSeconds(10));
            poll.Callback(poll.State);
            poll = await clock.Polls.Reader.ReadAsync(timeout.Token);
            await Assert.That((string)(await member.ExecuteScalarAsync(timeout.Token))!).IsNotEqualTo(firstVersion);
            await Assert.That((long)(await lanes.ExecuteScalarAsync(timeout.Token))!).IsEqualTo(0);
            // A read-only empty scan must not hide rows committed before the next poll.
            await Insert(source, Id(1), 0);
            poll.Callback(poll.State);
            await clock.Polls.Reader.ReadAsync(timeout.Token);
            await using var remaining = source.CreateCommand("SELECT count(*) FROM relayer_outbox");
            await Assert.That((long)(await remaining.ExecuteScalarAsync(timeout.Token))!).IsEqualTo(0);
        }
        finally { await worker.StopAsync(timeout.Token); }
    }

    [Test]
    [Arguments("future")]
    [Arguments("claimed")]
    [Arguments("parked")]
    public async Task Hosted_probe_respects_ineligible_heads_and_still_dispatches_other_lanes(string kind)
    {
        await using var fixture = new PostgreSqlFixture();
        await fixture.StartAsync();
        await using var source = NpgsqlDataSource.Create(fixture.ConnectionString);
        await PostgreSqlOutboxStoreTests.CreateSchema(source);
        await Insert(source, Id(1), 0);
        await Insert(source, Id(2), 0);
        await Insert(source, Id(3), 1);
        var change = kind switch
        {
            "future" => "deliver_at = clock_timestamp() + interval '1 day'",
            "claimed" => "claim_token = gen_random_uuid(), claim_expires_at = clock_timestamp() + interval '1 day'",
            _ => "status = 'parked'"
        };
        await using (var command = source.CreateCommand($"UPDATE relayer_outbox SET {change} WHERE id = @id"))
        {
            command.Parameters.AddWithValue("id", Id(1));
            await command.ExecuteNonQueryAsync();
        }
        var clock = new ScanClock();
        var transport = new RecordingTransport();
        using var worker = new PostgreSqlOrderedDispatcher(source, new() { Lanes = 2 }, [transport],
            NullLogger<PostgreSqlOrderedDispatcher>.Instance, clock);
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(20));
        await worker.StartAsync(timeout.Token);
        try
        {
            var poll = await clock.Polls.Reader.ReadAsync(timeout.Token);
            await Assert.That(transport.Ids.SequenceEqual([Id(3)])).IsTrue();
            await using var lanes = source.CreateCommand("SELECT count(*) FROM relayer_leases WHERE name = 'lane:0'");
            await Assert.That((long)(await lanes.ExecuteScalarAsync(timeout.Token))!).IsEqualTo(0);
            await using (var command = source.CreateCommand("UPDATE relayer_outbox SET status = 'pending', deliver_at = clock_timestamp(), claim_token = NULL, claim_expires_at = NULL WHERE id = @id"))
            {
                command.Parameters.AddWithValue("id", Id(1));
                await command.ExecuteNonQueryAsync(timeout.Token);
            }
            poll.Callback(poll.State);
            await clock.Polls.Reader.ReadAsync(timeout.Token);
            await Assert.That(transport.Ids.SequenceEqual([Id(3), Id(1), Id(2)])).IsTrue();
        }
        finally { await worker.StopAsync(timeout.Token); }
    }

    [Test]
    public async Task Hosted_worker_refreshes_membership_after_a_slow_lane_before_renewing_another()
    {
        await using var fixture = new PostgreSqlFixture();
        await fixture.StartAsync();
        await using var source = NpgsqlDataSource.Create(fixture.ConnectionString);
        await PostgreSqlOutboxStoreTests.CreateSchema(source);
        await Insert(source, Id(1), 0);
        await Insert(source, Id(2), 1);
        var clock = new ScanClock();
        var transport = new RecordingTransport(async (_, _) =>
        {
            // This member sorts after the worker, so the worker retains only even lanes after refreshing.
            await using var join = source.CreateCommand("INSERT INTO relayer_leases (name, owner, expires_at) VALUES ('member:zzzz', 'member:zzzz', clock_timestamp() + interval '1 day')");
            await join.ExecuteNonQueryAsync();
            clock.Advance(TimeSpan.FromSeconds(10));
        });
        using var worker = new PostgreSqlOrderedDispatcher(source, new() { Lanes = 2 }, [transport],
            NullLogger<PostgreSqlOrderedDispatcher>.Instance, clock);
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(20));
        await worker.StartAsync(timeout.Token);
        try
        {
            await clock.Polls.Reader.ReadAsync(timeout.Token);
            await Assert.That(transport.Ids.SequenceEqual([Id(1)])).IsTrue();
            await using var lane = source.CreateCommand("SELECT count(*) FROM relayer_leases WHERE name = 'lane:1'");
            await Assert.That((long)(await lane.ExecuteScalarAsync(timeout.Token))!).IsEqualTo(0);
        }
        finally { await worker.StopAsync(timeout.Token); }
    }

    [Test]
    [Arguments("stale")]
    [Arguments("expired")]
    public async Task Reused_mutation_commands_preserve_token_and_expiry_fences(string kind)
    {
        await using var fixture = new PostgreSqlFixture();
        await fixture.StartAsync();
        await using var source = NpgsqlDataSource.Create(fixture.ConnectionString);
        await PostgreSqlOutboxStoreTests.CreateSchema(source);
        await Insert(source, Id(1), 0);
        using var lane = new PostgreSqlOrderedLane(source, new(), TimeSpan.FromSeconds(30));
        await using var connection = await source.OpenConnectionAsync();
        await using (var transaction = await connection.BeginTransactionAsync())
        {
            var claim = new OutboxClaim(Id(1), Guid.NewGuid());
            await Assert.That(await lane.ClaimAsync(connection, transaction, claim, default)).IsTrue();
            if (kind == "stale") claim = new(claim.MessageId, Guid.NewGuid());
            else
            {
                await using var expire = new NpgsqlCommand("UPDATE relayer_outbox SET claim_expires_at = clock_timestamp() - interval '1 second'", connection, transaction);
                await expire.ExecuteNonQueryAsync();
            }
            await Assert.That(await lane.CompleteAsync(connection, transaction, claim, default)).IsFalse();
            await Assert.That(await lane.FailAsync(connection, transaction, claim, "stale failure", default)).IsFalse();
            await transaction.RollbackAsync();
        }
        // A new transaction must not retain a disposed transaction, prior token or error parameter.
        await using (var transaction = await connection.BeginTransactionAsync())
        {
            var claim = new OutboxClaim(Id(1), Guid.NewGuid());
            await Assert.That(await lane.ClaimAsync(connection, transaction, claim, default)).IsTrue();
            await Assert.That(await lane.CompleteAsync(connection, transaction, claim, default)).IsTrue();
            await transaction.CommitAsync();
        }
    }

    private sealed class ScanClock : TimeProvider
    {
        private long _ticks;
        internal Channel<(TimerCallback Callback, object? State)> Polls { get; } =
            Channel.CreateUnbounded<(TimerCallback, object?)>();
        public override long TimestampFrequency => TimeSpan.TicksPerSecond;
        public override long GetTimestamp() => Interlocked.Read(ref _ticks);
        internal void Advance(TimeSpan duration) => Interlocked.Add(ref _ticks, duration.Ticks);
        public override ITimer CreateTimer(TimerCallback callback, object? state, TimeSpan dueTime, TimeSpan period)
        {
            Polls.Writer.TryWrite((callback, state));
            return new SilentTimer();
        }
        private sealed class SilentTimer : ITimer
        {
            public bool Change(TimeSpan dueTime, TimeSpan period) => true;
            public void Dispose() { }
            public ValueTask DisposeAsync() => ValueTask.CompletedTask;
        }
    }

    private sealed class PollClock : TimeProvider
    {
        internal TaskCompletionSource FirstPoll { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public override ITimer CreateTimer(TimerCallback callback, object? state, TimeSpan dueTime, TimeSpan period)
        {
            FirstPoll.TrySetResult();
            return new SilentTimer();
        }
        private sealed class SilentTimer : ITimer
        {
            public bool Change(TimeSpan dueTime, TimeSpan period) => true;
            public void Dispose() { }
            public ValueTask DisposeAsync() => ValueTask.CompletedTask;
        }
    }

    private sealed class BudgetClock : TimeProvider
    {
        private long _ticks;
        public override long TimestampFrequency => TimeSpan.TicksPerSecond;
        public override long GetTimestamp() => Interlocked.Read(ref _ticks);
        internal void Advance(TimeSpan duration) => Interlocked.Add(ref _ticks, duration.Ticks);
    }

    private static PostgreSqlOrderedDispatcher Worker(NpgsqlDataSource source, RecordingTransport transport, int lanes = 1) =>
        new(source, new PostgreSqlOrderedDispatchOptions { Lanes = lanes }, [transport], NullLogger<PostgreSqlOrderedDispatcher>.Instance);

    private static Guid Id(int number) => Guid.Parse($"01900000-0000-7000-8000-{number:x12}");
    private static Guid HeaderId(OutgoingMessage message) => Guid.Parse(message.Headers.Span[0].Value.ToString());

    private static async Task Insert(NpgsqlDataSource source, Guid id, int? lane)
    {
        var headers = new ArrayBufferWriter<byte>();
        OutboxHeaderCodec.Write(headers, [new("ce_id", new(id.ToString()))]);
        await using var command = source.CreateCommand("INSERT INTO relayer_outbox (id, lane, ordering_key, transport, destination, message_type, headers, body, created_at, deliver_at) VALUES (@id, @lane, 'customer-é', 'events', 'orders', 'order.placed;v=1', @headers, @body, clock_timestamp(), clock_timestamp())");
        command.Parameters.AddWithValue("id", id);
        command.Parameters.AddWithValue("lane", NpgsqlDbType.Integer, (object?)lane ?? DBNull.Value);
        command.Parameters.AddWithValue("headers", headers.WrittenMemory.ToArray());
        command.Parameters.AddWithValue("body", new byte[] { 7, 8, 9 });
        await command.ExecuteNonQueryAsync();
    }

    private static async Task WaitForLock(NpgsqlDataSource source, CancellationToken cancellationToken)
    {
        while (true)
        {
            await using var command = source.CreateCommand("SELECT count(*) FROM pg_stat_activity WHERE wait_event_type = 'Lock' AND query LIKE 'INSERT INTO %leases%'");
            if ((long)(await command.ExecuteScalarAsync(cancellationToken))! > 0) return;
            await Task.Yield();
        }
    }

    private sealed class RecordingTransport(Func<OutgoingMessage, CancellationToken, ValueTask>? send = null) : ITransport
    {
        public string Name => "events";
        internal ConcurrentQueue<Guid> Ids { get; } = new();
        internal byte[]? LastBody { get; private set; }
        internal string? LastKey { get; private set; }
        internal DateTimeOffset? LastDeliverAt { get; private set; }
        internal bool Ordered { get; init; } = true;
        public TransportCapabilities GetCapabilities(string endpoint) => new() { OrderedByKey = Ordered && endpoint != "unsupported" };
        public ValueTask<TopologyReport> ApplyTopologyAsync(TopologyMode mode, CancellationToken cancellationToken) => new(new TopologyReport([]));
        public async ValueTask SendAsync(OutgoingMessage message, CancellationToken cancellationToken)
        {
            if (send is not null) await send(message, cancellationToken);
            Ids.Enqueue(HeaderId(message));
            LastBody = message.Body.ToArray();
            LastKey = message.OrderingKey?.ToString();
            LastDeliverAt = message.DeliverAt;
        }
        public ValueTask SendBatchAsync(ReadOnlyMemory<OutgoingMessage> messages, CancellationToken cancellationToken) => throw new NotSupportedException();
        public Task StartAsync(CancellationToken cancellationToken) => Task.CompletedTask;
        public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }
}
