using System.Diagnostics.Metrics;
using System.Security.Cryptography;
using System.Text;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using MySqlConnector;
using Relayer.Transports;

namespace Relayer.Outbox.MySql.Tests;

public sealed class MaintenanceTests
{
    private static readonly DateTimeOffset s_databaseTime = new(2026, 11, 24, 0, 0, 0, TimeSpan.Zero);
    private static readonly DateTimeOffset s_firstMySqlDate = new(1000, 1, 1, 0, 0, 0, TimeSpan.Zero);

    [Test]
    [Arguments(false, false, 1)]
    [Arguments(true, false, 0)]
    [Arguments(false, true, 0)]
    public async Task Recovery_handles_the_mysql_datetime_lower_bound(bool beforeMinimum, bool overflow, int expected)
    {
        await using var data = new MySqlFixture();
        await data.StartAsync();
        await data.Insert(Guid.CreateVersion7(), s_firstMySqlDate);
        var warnings = new List<MySqlError>();
        await using var source = WithDatabaseTime(data, s_databaseTime, (_, args) => warnings.AddRange(args.Errors));
        var transport = new Transport();
        using var worker = new MySqlMaintenanceWorker(source, new()
        {
            RecoveryDelay = CutoffAge(beforeMinimum, overflow)
        }, [transport], NullLogger<MySqlMaintenanceWorker>.Instance);

        await Assert.That(await worker.SweepAsync()).IsEqualTo(expected);
        await Assert.That(transport.Sent).IsEqualTo(expected);
        await Assert.That(warnings).IsEmpty();
    }

    [Test]
    [Arguments(false, false, false, 1)]
    [Arguments(false, true, false, 0)]
    [Arguments(false, false, true, 0)]
    [Arguments(true, false, false, 1)]
    [Arguments(true, true, false, 0)]
    [Arguments(true, false, true, 0)]
    public async Task Retention_handles_the_mysql_datetime_lower_bound(bool dead, bool beforeMinimum, bool overflow, int expected)
    {
        await using var data = new MySqlFixture();
        await data.StartAsync();
        await data.Execute("""
            INSERT INTO relayer_dead (id, transport, destination, message_type, headers, body, created_at, attempts, failed_at)
            VALUES (UUID_TO_BIN(UUID()), 'events', 'orders', 'order', X'', X'', '1000-01-01', 1, '1000-01-01');
            INSERT INTO relayer_inbox (consumer, message_id, processed_at)
            VALUES ('orders', UUID_TO_BIN(UUID()), '1000-01-01');
            """);
        await using var source = WithDatabaseTime(data, s_databaseTime);
        var age = CutoffAge(beforeMinimum, overflow);
        using var worker = new MySqlMaintenanceWorker(source, new()
        {
            DeadRetention = dead ? age : null,
            InboxRetention = dead ? null : age
        }, [], NullLogger<MySqlMaintenanceWorker>.Instance);

        await Assert.That(await worker.CleanupAsync()).IsEqualTo(expected);
        await Assert.That(await Scalar(data, "SELECT COUNT(*) FROM relayer_dead")).IsEqualTo(dead ? 1L - expected : 1L);
        await Assert.That(await Scalar(data, "SELECT COUNT(*) FROM relayer_inbox")).IsEqualTo(dead ? 1L : 1L - expected);
    }

    [Test]
    public async Task Recovery_uses_mysql_time_for_due_rows_and_claims()
    {
        await using var data = new MySqlFixture();
        await data.StartAsync();
        var databaseNow = DateTimeOffset.FromUnixTimeSeconds(DateTimeOffset.UtcNow.AddMinutes(5).ToUnixTimeSeconds());
        await using var source = WithDatabaseTime(data, databaseNow);

        var createdAt = databaseNow.AddMinutes(-10);
        await data.Insert(Guid.CreateVersion7(), createdAt, deliverAt: databaseNow.AddSeconds(-1));
        await data.Execute("UPDATE relayer_outbox SET headers=X'FF'");
        await data.Insert(Guid.CreateVersion7(), createdAt, deliverAt: databaseNow.AddSeconds(-1));
        await data.Insert(Guid.CreateVersion7(), createdAt, expires: databaseNow.AddSeconds(-1));
        await data.Insert(Guid.CreateVersion7(), createdAt, deliverAt: databaseNow.AddSeconds(1));
        await data.Insert(Guid.CreateVersion7(), createdAt, expires: databaseNow.AddSeconds(1));
        var transport = new Transport();
        using var worker = new MySqlMaintenanceWorker(source, new()
        {
            RecoveryDelay = TimeSpan.Zero
        }, [transport], NullLogger<MySqlMaintenanceWorker>.Instance);

        await Assert.That(await worker.SweepAsync()).IsEqualTo(3);
        await Assert.That(transport.Sent).IsEqualTo(2);
        await Assert.That(await Scalar(data, "SELECT COUNT(*) FROM relayer_outbox")).IsEqualTo(2L);
        await Assert.That(await Scalar(data, "SELECT COUNT(*) FROM relayer_dead WHERE headers=X'FF'")).IsEqualTo(1L);
    }

    [Test]
    public async Task Recovery_sets_durable_retry_deadline_from_mysql_time()
    {
        await using var data = new MySqlFixture();
        await data.StartAsync();
        var databaseNow = DateTimeOffset.FromUnixTimeSeconds(DateTimeOffset.UtcNow.AddMinutes(5).ToUnixTimeSeconds());
        await using var source = WithDatabaseTime(data, databaseNow);
        await data.Insert(Guid.CreateVersion7(), databaseNow.AddMinutes(-10));
        var transport = new Transport(static _ => ValueTask.FromException(new InvalidOperationException("offline")));
        using var worker = new MySqlMaintenanceWorker(source, new()
        {
            RecoveryDelay = TimeSpan.Zero
        }, [transport], NullLogger<MySqlMaintenanceWorker>.Instance);

        await Assert.That(await worker.SweepAsync()).IsEqualTo(1);
        await using var connection = await data.Source.OpenConnectionAsync();
        await using var command = new MySqlCommand(
            "SELECT COUNT(*) FROM relayer_outbox WHERE attempts=1 AND deliver_at=@retry AND claim_token IS NULL AND claim_expires_at IS NULL",
            connection);
        command.Parameters.AddWithValue("retry", databaseNow.AddSeconds(1));
        await Assert.That(Convert.ToInt64(await command.ExecuteScalarAsync())).IsEqualTo(1L);
    }

    [Test]
    public async Task Retention_rejects_non_autocommit_sessions_before_deleting()
    {
        await using var data = new MySqlFixture();
        await data.StartAsync();
        await data.Execute("INSERT INTO relayer_inbox (consumer, message_id, processed_at) VALUES ('orders', UUID_TO_BIN(UUID()), UTC_TIMESTAMP(6)-INTERVAL 8 DAY)");
        var autocommit = false;
        await using var source = new MySqlDataSourceBuilder(data.Source.ConnectionString + ";Pooling=false")
            .UseConnectionOpenedCallback(async (context, token) =>
            {
                await using var command = new MySqlCommand("SET SESSION autocommit = @enabled", context.Connection);
                command.Parameters.AddWithValue("enabled", autocommit);
                await command.ExecuteNonQueryAsync(token);
            }).Build();
        using var worker = new MySqlMaintenanceWorker(source, new()
        {
            DeadRetention = null
        }, [], NullLogger<MySqlMaintenanceWorker>.Instance);

        var error = await Assert.That(async () => await worker.CleanupAsync()).Throws<InvalidOperationException>();
        await Assert.That(error!.Message).Contains("autocommit=1");
        await Assert.That(await Scalar(data, "SELECT COUNT(*) FROM relayer_inbox")).IsEqualTo(1L);
        await Assert.That(await Scalar(data, "SELECT COUNT(*) FROM relayer_leases")).IsEqualTo(0L);

        autocommit = true;
        await Assert.That(await worker.CleanupAsync()).IsEqualTo(1);
        await Assert.That(await Scalar(data, "SELECT COUNT(*) FROM relayer_inbox")).IsEqualTo(0L);
    }

    [Test]
    public async Task Overlapping_sweeps_dispatch_with_two_shared_pool_connections()
    {
        await using var data = new MySqlFixture();
        await data.StartAsync();
        var other = new MySqlOutboxOptions { TablePrefix = "other_" };
        var firstTable = MySqlOutboxSchema.Table(data.Options);
        var secondTable = MySqlOutboxSchema.Table(other);
        await data.Execute(MySqlOutboxSchema.CreateScript(other));
        await data.Insert(Guid.CreateVersion7(), DateTimeOffset.UtcNow.AddDays(-1));
        await data.Execute($"INSERT INTO {secondTable} SELECT * FROM {firstTable}");

        await using var source = new MySqlDataSource(data.Source.ConnectionString + ";MaximumPoolSize=2");
        var firstTransport = new Transport();
        var secondTransport = new Transport();
        using var first = new MySqlMaintenanceWorker(source, new()
        {
            RecoveryDelay = TimeSpan.Zero
        }, [firstTransport], NullLogger<MySqlMaintenanceWorker>.Instance);
        using var second = new MySqlMaintenanceWorker(source, new()
        {
            TablePrefix = other.TablePrefix,
            RecoveryDelay = TimeSpan.Zero
        }, [secondTransport], NullLogger<MySqlMaintenanceWorker>.Instance);

        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        await using var blockingSession = await data.Source.OpenConnectionAsync(timeout.Token);
        await using var gate = new MySqlCommand($"LOCK TABLES {firstTable} WRITE, {secondTable} WRITE", blockingSession);
        await using var observerSession = await data.Source.OpenConnectionAsync(timeout.Token);
        await using var observe = new MySqlCommand(
            "SELECT IS_USED_LOCK(@first) IS NOT NULL AND IS_USED_LOCK(@second) IS NOT NULL", observerSession);
        observe.Parameters.AddWithValue("first", SweeperResource(data.Database, data.Options));
        observe.Parameters.AddWithValue("second", SweeperResource(data.Database, other));

        // Hold both scans behind the table locks until both sweeps hold admission.
        await gate.ExecuteNonQueryAsync(timeout.Token);
        var sweeps = Task.WhenAll(first.SweepAsync(timeout.Token).AsTask(), second.SweepAsync(timeout.Token).AsTask());

        try
        {
            while (!Convert.ToBoolean(await observe.ExecuteScalarAsync(timeout.Token)))
            {
                await Task.Delay(20, timeout.Token);
            }
        }
        finally
        {
            gate.CommandText = "UNLOCK TABLES";
            await gate.ExecuteNonQueryAsync();
            await sweeps;
        }

        await Assert.That(firstTransport.Sent).IsEqualTo(1);
        await Assert.That(secondTransport.Sent).IsEqualTo(1);
        await Assert.That(await Scalar(data, $"SELECT (SELECT COUNT(*) FROM {firstTable}) + (SELECT COUNT(*) FROM {secondTable})"))
            .IsEqualTo(0L);
    }

    [Test]
    [Arguments("malformed")]
    [Arguments("transport")]
    public async Task Unordered_poison_metrics_follow_committed_deletions_once(string failure)
    {
        await using var data = new MySqlFixture();
        await data.StartAsync();
        await data.Insert(Guid.CreateVersion7(), DateTimeOffset.UtcNow.AddDays(-1));

        if (failure == "malformed")
        {
            await data.Execute("UPDATE relayer_outbox SET headers=X'FF'");
        }

        if (failure == "transport")
        {
            await data.Execute("UPDATE relayer_outbox SET transport='missing'");
        }

        using var instrumentation = new RelayerInstrumentation();
        long pending = 0, poison = 0;
        using var listener = new MeterListener();
        listener.InstrumentPublished = (instrument, meterListener) =>
        {
            if (instrument == instrumentation.OutboxPending || instrument == instrumentation.OutboxPoison)
            {
                meterListener.EnableMeasurementEvents(instrument);
            }
        };
        listener.SetMeasurementEventCallback<long>((instrument, count, _, _) =>
        {
            if (instrument == instrumentation.OutboxPending)
            {
                Interlocked.Add(ref pending, count);
            }
            else
            {
                Interlocked.Add(ref poison, count);
            }
        });
        listener.Start();
        instrumentation.AddOutboxPending(1);
        var transport = new Transport();
        using var worker = new MySqlMaintenanceWorker(data.Source,
            new()
            {
                BatchSize = 4,
                RecoveryDelay = TimeSpan.Zero,
                MaxDispatchAttempts = 2
            },
            [transport], NullLogger<MySqlMaintenanceWorker>.Instance, instrumentation: instrumentation);
        await worker.SweepAsync();
        await Assert.That(poison).IsEqualTo(failure == "malformed" ? 1L : 0L);
        await Assert.That(pending).IsEqualTo(failure == "malformed" ? 0L : 1L);
        // The fixed-bound pass first reaches its end, then starts a new pass.
        await worker.SweepAsync();
        await worker.SweepAsync();
        await Assert.That(await Scalar(data, "SELECT COUNT(*) FROM relayer_outbox")).IsEqualTo(0L);
        await Assert.That(await Scalar(data, "SELECT COUNT(*) FROM relayer_dead")).IsEqualTo(1L);
        await Assert.That(pending).IsEqualTo(0L);
        await Assert.That(poison).IsEqualTo(1L);
        await worker.SweepAsync();
        await Assert.That(pending).IsEqualTo(0L);
        await Assert.That(poison).IsEqualTo(1L);
    }

    [Test]
    public async Task Malformed_row_is_copied_raw_to_dead_without_stalling_valid_rows()
    {
        await using var data = new MySqlFixture();
        await data.StartAsync();
        var id = Guid.CreateVersion7();
        await data.Insert(id, DateTimeOffset.UtcNow.AddDays(-1));
        await data.Execute("UPDATE relayer_outbox SET headers=X'FF'");
        await data.Insert(Guid.CreateVersion7(), DateTimeOffset.UtcNow.AddDays(-1));
        var transport = new Transport();
        using var worker = Worker(data, transport);
        await Assert.That(await worker.SweepAsync()).IsEqualTo(2);
        await Assert.That(transport.Sent).IsEqualTo(1);
        await Assert.That(await Scalar(data, "SELECT COUNT(*) FROM relayer_dead WHERE headers=X'FF' AND last_error LIKE '%Malformed%'")).IsEqualTo(1L);
        await Assert.That(await Scalar(data, "SELECT COUNT(*) FROM relayer_outbox")).IsEqualTo(0L);
    }

    [Test]
    public async Task Expired_crashed_leader_and_claim_are_recovered_but_live_leader_is_respected()
    {
        await using var data = new MySqlFixture();
        await data.StartAsync();
        await data.Insert(Guid.CreateVersion7(), DateTimeOffset.UtcNow.AddDays(-1), expires: DateTimeOffset.UtcNow.AddSeconds(-1));
        await data.Execute("INSERT INTO relayer_leases VALUES ('sweeper',X'01010101010101010101010101010101',UTC_TIMESTAMP(6)+INTERVAL 1 DAY)");
        var transport = new Transport();
        using var worker = Worker(data, transport);
        await Assert.That(await worker.SweepAsync()).IsEqualTo(0);
        await data.Execute("UPDATE relayer_leases SET expires_at=UTC_TIMESTAMP(6)-INTERVAL 1 SECOND");
        await Assert.That(await worker.SweepAsync()).IsEqualTo(1);
        await Assert.That(transport.Sent).IsEqualTo(1);
    }

    [Test]
    public async Task Native_admission_prevents_takeover_during_send_even_when_leadership_expires()
    {
        await using var data = new MySqlFixture();
        await data.StartAsync();
        await data.Insert(Guid.CreateVersion7(), DateTimeOffset.UtcNow.AddDays(-1));
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var transport = new Transport(async token =>
        {
            entered.SetResult();
            await release.Task.WaitAsync(token);
        });
        using var first = Worker(data, transport);
        using var rival = Worker(data, new());
        var send = first.SweepAsync().AsTask();

        try
        {
            await entered.Task.WaitAsync(TimeSpan.FromSeconds(30));
            await data.Execute("UPDATE relayer_leases SET expires_at=UTC_TIMESTAMP(6)-INTERVAL 1 SECOND");
            await Assert.That(await rival.SweepAsync()).IsEqualTo(0);
        }
        finally
        {
            release.TrySetResult();
            await send;
        }

        await first.ReleaseAsync();
        await Assert.That(await rival.SweepAsync()).IsEqualTo(0);
        await Assert.That(transport.Sent).IsEqualTo(1);
    }

    [Test]
    public async Task Exhaustion_is_atomic_fenced_and_keeps_ordered_barriers_during_retention()
    {
        await using var data = new MySqlFixture();
        await data.StartAsync();
        var id = Guid.CreateVersion7();
        await data.Insert(id, DateTimeOffset.UtcNow, lane: 3);
        using var store = new MySqlOutboxStore(data.Source, new()
        {
            MaxDispatchAttempts = 1
        });
        var claim = new OutboxClaim(id, Guid.NewGuid());
        var now = DateTimeOffset.UtcNow;
        await Assert.That(await store.TryClaimAsync(claim, now, now.AddMinutes(1), default)).IsTrue();
        await Assert.That(await store.RecordFailureAsync(new(id, Guid.NewGuid()), "stale", now, default)).IsFalse();
        await Assert.That(await store.RecordFailureAsync(claim, "poison", now, default)).IsTrue();
        await data.Execute("UPDATE relayer_dead SET failed_at=UTC_TIMESTAMP(6)-INTERVAL 60 DAY");
        using var worker = Worker(data, new());
        await Assert.That(await worker.CleanupAsync()).IsEqualTo(0);
        await Assert.That(await Scalar(data, "SELECT COUNT(*) FROM relayer_outbox WHERE status='parked' AND attempts=1 AND lane=3")).IsEqualTo(1L);
        await Assert.That(await Scalar(data, "SELECT COUNT(*) FROM relayer_dead WHERE last_error='poison'")).IsEqualTo(1L);
    }

    [Test]
    public async Task Repaired_ordered_row_can_park_again_and_refresh_its_dead_copy()
    {
        await using var data = new MySqlFixture();
        await data.StartAsync();
        var id = Guid.CreateVersion7();
        await data.Insert(id, DateTimeOffset.UtcNow, lane: 3);
        using var store = new MySqlOutboxStore(data.Source, new()
        {
            MaxDispatchAttempts = 2
        });
        var now = DateTimeOffset.UtcNow;

        for (var cycle = 0; cycle < 2; cycle++)
        {
            for (var attempt = 0; attempt < 2; attempt++)
            {
                var claim = new OutboxClaim(id, Guid.NewGuid());
                await Assert.That(await store.TryClaimAsync(claim, now, now.AddMinutes(1), default)).IsTrue();
                await Assert.That(await store.RecordFailureAsync(claim, cycle == 0 ? "original" : "repaired", now, default)).IsTrue();
                await Assert.That(await store.RecordFailureAsync(claim, "stale", now, default)).IsFalse();
            }

            await Assert.That(await Scalar(data, "SELECT COUNT(*) FROM relayer_outbox WHERE status='parked' AND attempts=2 AND lane=3")).IsEqualTo(1L);

            if (cycle == 0)
            {
                await data.Execute("UPDATE relayer_outbox SET status='pending', attempts=0, body=X'ABCD'");
            }
        }

        await Assert.That(await Scalar(data, "SELECT COUNT(*) FROM relayer_dead")).IsEqualTo(1L);
        await Assert.That(await Scalar(data, "SELECT COUNT(*) FROM relayer_dead WHERE attempts=2 AND last_error='repaired' AND body=X'ABCD'")).IsEqualTo(1L);
    }

    [Test]
    public async Task Invalid_transport_exhausts_recovery_attempts_without_stalling_valid_groups()
    {
        await using var data = new MySqlFixture();
        await data.StartAsync();
        await data.Insert(Guid.CreateVersion7(), DateTimeOffset.UtcNow.AddDays(-1));
        await data.Execute("UPDATE relayer_outbox SET transport='missing'");
        await data.Insert(Guid.CreateVersion7(), DateTimeOffset.UtcNow.AddDays(-1));
        var transport = new Transport();
        using var worker = Worker(data, transport);
        await Assert.That(await worker.SweepAsync()).IsEqualTo(2);
        await worker.SweepAsync();
        await Assert.That(await worker.SweepAsync()).IsEqualTo(1);
        await Assert.That(transport.Sent).IsEqualTo(1);
        await Assert.That(await Scalar(data, "SELECT COUNT(*) FROM relayer_dead WHERE last_error LIKE '%missing%'")).IsEqualTo(1L);
    }

    [Test]
    public async Task Failed_dead_copy_rolls_back_attempts_and_leaves_claim_owned()
    {
        await using var data = new MySqlFixture();
        await data.StartAsync();
        var id = Guid.CreateVersion7();
        await data.Insert(id, DateTimeOffset.UtcNow);
        await data.Execute("CREATE TRIGGER fail_dead BEFORE INSERT ON relayer_dead FOR EACH ROW SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='dead unavailable'");
        using var store = new MySqlOutboxStore(data.Source, new()
        {
            MaxDispatchAttempts = 1
        });
        var claim = new OutboxClaim(id, Guid.NewGuid());
        var now = DateTimeOffset.UtcNow;
        await Assert.That(await store.TryClaimAsync(claim, now, now.AddMinutes(1), default)).IsTrue();
        await Assert.That(async () => await store.RecordDispatchFailureAsync(claim, "offline", now, default)).Throws<MySqlException>();
        await Assert.That(await Scalar(data, "SELECT COUNT(*) FROM relayer_outbox WHERE attempts=0 AND claim_token IS NOT NULL")).IsEqualTo(1L);
        await data.Execute("DROP TRIGGER fail_dead");
        var committed = await store.RecordDispatchFailureAsync(claim, "offline", now, default);
        await Assert.That(committed.Accepted).IsTrue();
        await Assert.That(committed.Removed).IsTrue();
        var stale = await store.RecordDispatchFailureAsync(claim, "offline", now, default);
        await Assert.That(stale.Accepted).IsFalse();
        await Assert.That(stale.Removed).IsFalse();
        await Assert.That(await Scalar(data, "SELECT COUNT(*) FROM relayer_outbox")).IsEqualTo(0L);
    }

    [Test]
    public async Task Cancelled_native_send_is_recovered_after_claim_expiry_without_consuming_attempts()
    {
        await using var data = new MySqlFixture();
        await data.StartAsync();
        await data.Insert(Guid.CreateVersion7(), DateTimeOffset.UtcNow.AddDays(-1));
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var blocked = new Transport(async token =>
        {
            entered.SetResult();
            await Task.Delay(Timeout.InfiniteTimeSpan, token);
        });
        // Session reset is application configuration. Cleanup cannot rely on pool reset to drop named locks.
        await using var source = new MySqlDataSource(data.Source.ConnectionString + ";ConnectionReset=false;MaximumPoolSize=1");
        using var first = new MySqlMaintenanceWorker(source, new()
        {
            BatchSize = 4,
            RecoveryDelay = TimeSpan.Zero
        },
            [blocked], NullLogger<MySqlMaintenanceWorker>.Instance);
        using var stop = new CancellationTokenSource();
        var sweep = first.SweepAsync(stop.Token).AsTask();
        await entered.Task.WaitAsync(TimeSpan.FromSeconds(30));
        stop.Cancel();
        await Assert.That(async () => await sweep).Throws<OperationCanceledException>();
        await Assert.That(await Scalar(data, "SELECT COUNT(*) FROM relayer_outbox WHERE attempts=0 AND claim_token IS NOT NULL")).IsEqualTo(1L);
        await data.Execute("UPDATE relayer_outbox SET claim_expires_at=UTC_TIMESTAMP(6)-INTERVAL 1 SECOND; UPDATE relayer_leases SET expires_at=UTC_TIMESTAMP(6)-INTERVAL 1 SECOND");
        var transport = new Transport();
        using var replacement = Worker(data, transport);
        await Assert.That(await replacement.SweepAsync()).IsEqualTo(1);
        await Assert.That(transport.Sent).IsEqualTo(1);
    }

    [Test]
    public async Task Broken_admission_session_does_not_replace_send_cancellation()
    {
        await using var data = new MySqlFixture();
        await data.StartAsync();
        await data.Insert(Guid.CreateVersion7(), DateTimeOffset.UtcNow.AddDays(-1));
        using var stop = new CancellationTokenSource();
        var resource = SweeperResource(data.Database, data.Options);
        var transport = new Transport(async _ =>
        {
            await using var connection = await data.Source.OpenConnectionAsync();
            await using var command = new MySqlCommand("SELECT IS_USED_LOCK(@resource)", connection);
            command.Parameters.AddWithValue("resource", resource);
            var session = Convert.ToInt64(await command.ExecuteScalarAsync());
            await data.Execute($"KILL CONNECTION {session}");
            stop.Cancel();
            stop.Token.ThrowIfCancellationRequested();
        });
        using var worker = Worker(data, transport);
        await Assert.That(async () => await worker.SweepAsync(stop.Token)).Throws<OperationCanceledException>();
        await Assert.That(await Scalar(data, "SELECT COUNT(*) FROM relayer_outbox WHERE attempts=0")).IsEqualTo(1L);
        await data.Execute("UPDATE relayer_outbox SET claim_expires_at=UTC_TIMESTAMP(6)-INTERVAL 1 SECOND; UPDATE relayer_leases SET expires_at=UTC_TIMESTAMP(6)-INTERVAL 1 SECOND");
        var replacementTransport = new Transport();
        using var replacement = Worker(data, replacementTransport);
        await Assert.That(await replacement.SweepAsync()).IsEqualTo(1);
        await Assert.That(replacementTransport.Sent).IsEqualTo(1);
    }

    [Test]
    public async Task Ordered_malformed_row_parks_raw_and_unrelated_key_continues_in_original_sequence()
    {
        await using var data = new MySqlFixture();
        await data.StartAsync();
        await data.Insert(Guid.CreateVersion7(), DateTimeOffset.UtcNow.AddDays(-1), lane: 0);
        await data.Execute("UPDATE relayer_outbox SET headers=X'FF',ordering_key='poison'");
        await data.Insert(Guid.CreateVersion7(), DateTimeOffset.UtcNow.AddDays(-1), lane: 0);
        using var instrumentation = new RelayerInstrumentation();
        long poisonCount = 0;
        using var listener = new MeterListener();
        listener.InstrumentPublished = (instrument, meterListener) =>
        {
            if (instrument == instrumentation.OutboxPoison)
            {
                meterListener.EnableMeasurementEvents(instrument);
            }
        };
        listener.SetMeasurementEventCallback<long>((_, count, _, _) => Interlocked.Add(ref poisonCount, count));
        listener.Start();
        var transport = new Transport();
        using var ordered = new MySqlOrderedDispatcher(data.Source, new()
        {
            Lanes = 1,
            MaxDispatchAttempts = 2
        },
            [transport], NullLogger<MySqlOrderedDispatcher>.Instance, instrumentation: instrumentation);
        await Assert.That(await ordered.DispatchLaneAsync(0, default)).IsEqualTo(0);
        await Assert.That(await ordered.DispatchLaneAsync(0, default)).IsEqualTo(1);
        await Assert.That(poisonCount).IsEqualTo(1L);
        await Assert.That(await ordered.DispatchLaneAsync(0, default)).IsEqualTo(1);
        await Assert.That(poisonCount).IsEqualTo(1L);
        await Assert.That(transport.Sent).IsEqualTo(1);
        await Assert.That(await Scalar(data, "SELECT COUNT(*) FROM relayer_outbox WHERE status='parked' AND headers=X'FF' AND last_error LIKE '%Malformed%'")).IsEqualTo(1L);
    }

    private static TimeSpan CutoffAge(bool beforeMinimum, bool overflow)
    {
        if (overflow)
        {
            return TimeSpan.FromDays(1_000_000);
        }

        var age = s_databaseTime - s_firstMySqlDate;
        return beforeMinimum ? age + TimeSpan.FromMicroseconds(1) : age;
    }

    private static MySqlDataSource WithDatabaseTime(MySqlFixture data, DateTimeOffset databaseNow,
        MySqlInfoMessageEventHandler? onWarning = null)
    {
        return new MySqlDataSourceBuilder(data.Source.ConnectionString)
            .UseConnectionOpenedCallback(async (context, token) =>
            {
                await using var command = new MySqlCommand("SET SESSION timestamp = @timestamp", context.Connection);
                command.Parameters.AddWithValue("timestamp", databaseNow.ToUnixTimeSeconds());
                await command.ExecuteNonQueryAsync(token);

                if (onWarning is not null)
                {
                    context.Connection.InfoMessage += onWarning;
                }
            }).Build();
    }

    private static MySqlMaintenanceWorker Worker(MySqlFixture data, Transport transport)
    {
        return new(data.Source,
                new()
                {
                    BatchSize = 4,
                    RecoveryDelay = TimeSpan.Zero,
                    MaxDispatchAttempts = 2
                }, [transport], NullLogger<MySqlMaintenanceWorker>.Instance);
    }

    private static string SweeperResource(string database, MySqlOutboxOptions options)
    {
        var identity = database + ":" + MySqlOutboxSchema.LeaseTable(options);
        return Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(identity)))[..48] + ":sweeper";
    }

    private static async Task<long> Scalar(MySqlFixture data, string sql)
    {
        await using var command = data.Source.CreateCommand(sql);
        return Convert.ToInt64(await command.ExecuteScalarAsync());
    }

    private sealed class Transport(Func<CancellationToken, ValueTask>? send = null) : ITransport
    {
        public int Sent
        {
            get; private set;
        }

        public string Name => "events";

        public TransportCapabilities GetCapabilities(string endpoint)
        {
            return new()
            {
                MaxBatchSize = 1,
                OrderedByKey = true
            };
        }

        public async ValueTask SendAsync(OutgoingMessage message, CancellationToken cancellationToken)
        {
            if (send is not null)
            {
                await send(cancellationToken);
            }

            Sent++;
        }

        public async ValueTask SendBatchAsync(ReadOnlyMemory<OutgoingMessage> messages, CancellationToken cancellationToken)
        {
            for (var i = 0; i < messages.Length; i++)
            {
                await SendAsync(messages.Span[i], cancellationToken);
            }
        }

        public ValueTask<TopologyReport> ApplyTopologyAsync(TopologyMode mode, CancellationToken cancellationToken)
        {
            return new(new TopologyReport([]));
        }

        public Task StartAsync(CancellationToken cancellationToken)
        {
            return Task.CompletedTask;
        }

        public Task StopAsync(CancellationToken cancellationToken)
        {
            return Task.CompletedTask;
        }

        public ValueTask DisposeAsync()
        {
            return ValueTask.CompletedTask;
        }
    }
}

public sealed class MaintenanceRegistrationTests
{
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task Claim_duration_above_the_mysql_limit_fails_at_registration(bool fromConfiguration)
    {
        await using var source = new MySqlDataSource("Server=unused;Database=orders");
        var services = new ServiceCollection();
        var configuration = new ConfigurationBuilder().AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["ClaimDuration"] = TimeSpan.MaxValue.ToString("c")
        }).Build();
        var options = new MySqlMaintenanceOptions
        {
            ClaimDuration = TimeSpan.MaxValue
        };
        var error = await Assert.That(() => fromConfiguration
            ? services.AddRelayerMySqlMaintenance("store", source, configuration)
            : services.AddRelayerMySqlMaintenance("store", source, options))
            .Throws<ArgumentOutOfRangeException>().WithMessageContaining("at most 1000000 days");
        await Assert.That(error!.ParamName).IsEqualTo("ClaimDuration");
    }

    [Test]
    public async Task Named_immutable_options_bind_and_validate_before_connecting()
    {
        await using var source = new MySqlDataSource("Server=unused;Database=orders");
        var config = new ConfigurationBuilder().AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["BatchSize"] = "7",
            ["CleanupBatchLimit"] = "3",
            ["ClaimDuration"] = "00:02:00",
            ["DeadRetention"] = "infinite",
            ["InboxRetention"] = "2.00:00:00"
        }).Build();
        var services = new ServiceCollection().AddLogging();
        services.AddRelayerMySqlMaintenance("a", source, config);
        config["BatchSize"] = "99";
        await using var provider = services.BuildServiceProvider();
        var options = provider.GetRequiredService<IOptionsMonitor<MySqlMaintenanceOptions>>().Get("a");
        await Assert.That(options.CleanupBatchLimit).IsEqualTo(3);
        await Assert.That(options.BatchSize).IsEqualTo(7);
        await Assert.That(options.ClaimDuration).IsEqualTo(TimeSpan.FromMinutes(2));
        await Assert.That(options.DeadRetention).IsNull();
        await Assert.That(options.InboxRetention).IsEqualTo(TimeSpan.FromDays(2));
        await Assert.That(provider.GetServices<IHostedService>().Count()).IsEqualTo(1);
        await Assert.That(() => services.AddRelayerMySqlMaintenance("a", source)).Throws<InvalidOperationException>();
        await using var singleConnection = new MySqlDataSource("Server=unused;Database=orders;MaximumPoolSize=1");
        services.AddRelayerMySqlMaintenance("small", singleConnection);
        await Assert.That(() => new MySqlMaintenanceOptions { CleanupBatchLimit = 0 }.Validate()).Throws<ArgumentOutOfRangeException>();
        await Assert.That(() => new MySqlMaintenanceOptions { CleanupBatchLimit = 101 }.Validate()).Throws<ArgumentOutOfRangeException>();
        await Assert.That(() => new MySqlMaintenanceOptions { BatchSize = 0 }.Validate()).Throws<ArgumentOutOfRangeException>();
        await Assert.That(() => new MySqlMaintenanceOptions { RecoveryDelay = TimeSpan.FromSeconds(-1) }.Validate()).Throws<ArgumentOutOfRangeException>();
        await Assert.That(() => new MySqlMaintenanceOptions { ClaimDuration = TimeSpan.Zero }.Validate()).Throws<ArgumentOutOfRangeException>();
    }
}
