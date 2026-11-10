using System.Diagnostics.Metrics;
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
    [Test]
    [Arguments("malformed")]
    [Arguments("transport")]
    [Arguments("send")]
    public async Task Unordered_poison_metrics_follow_committed_deletions_once(string failure)
    {
        await using var data = new MySqlFixture();
        await data.StartAsync();
        await data.Insert(Guid.CreateVersion7(), DateTimeOffset.UtcNow.AddDays(-1));
        if (failure == "malformed") await data.Execute("UPDATE relayer_outbox SET headers=X'FF'");
        if (failure == "transport") await data.Execute("UPDATE relayer_outbox SET transport='missing'");
        using var instrumentation = new RelayerInstrumentation();
        long pending = 0, poison = 0;
        using var listener = new MeterListener();
        listener.InstrumentPublished = (instrument, meterListener) =>
        {
            if (instrument == instrumentation.OutboxPending || instrument == instrumentation.OutboxPoison)
                meterListener.EnableMeasurementEvents(instrument);
        };
        listener.SetMeasurementEventCallback<long>((instrument, count, _, _) =>
        {
            if (instrument == instrumentation.OutboxPending) Interlocked.Add(ref pending, count);
            else Interlocked.Add(ref poison, count);
        });
        listener.Start();
        instrumentation.AddOutboxPending(1);
        var transport = failure == "send" ? new Transport(_ => throw new InvalidOperationException("native send failed")) : new Transport();
        using var worker = new MySqlMaintenanceWorker(data.Source,
            new() { BatchSize = 4, RecoveryDelay = TimeSpan.Zero, MaxDispatchAttempts = 2 },
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
        var transport = new Transport(async token => { entered.SetResult(); await release.Task.WaitAsync(token); });
        using var first = Worker(data, transport);
        using var rival = Worker(data, new());
        var send = first.SweepAsync().AsTask();
        try
        {
            await entered.Task.WaitAsync(TimeSpan.FromSeconds(30));
            await data.Execute("UPDATE relayer_leases SET expires_at=UTC_TIMESTAMP(6)-INTERVAL 1 SECOND");
            await Assert.That(await rival.SweepAsync()).IsEqualTo(0);
        }
        finally { release.TrySetResult(); await send; }
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
        using var store = new MySqlOutboxStore(data.Source, new() { MaxDispatchAttempts = 1 });
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
        using var store = new MySqlOutboxStore(data.Source, new() { MaxDispatchAttempts = 2 });
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
                await data.Execute("UPDATE relayer_outbox SET status='pending', attempts=0, body=X'ABCD'");
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
        using var store = new MySqlOutboxStore(data.Source, new() { MaxDispatchAttempts = 1 });
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
        var blocked = new Transport(async token => { entered.SetResult(); await Task.Delay(Timeout.InfiniteTimeSpan, token); });
        // Session reset is application configuration. Cleanup cannot rely on pool reset to drop named locks.
        await using var source = new MySqlDataSource(data.Source.ConnectionString + ";ConnectionReset=false;MaximumPoolSize=2");
        using var first = new MySqlMaintenanceWorker(source, new() { BatchSize = 4, RecoveryDelay = TimeSpan.Zero },
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
        var resource = Convert.ToHexString(System.Security.Cryptography.SHA256.HashData(
            System.Text.Encoding.UTF8.GetBytes(data.Database + ":" + MySqlOutboxSchema.LeaseTable(new()))))[..48] + ":sweeper";
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
            if (instrument == instrumentation.OutboxPoison) meterListener.EnableMeasurementEvents(instrument);
        };
        listener.SetMeasurementEventCallback<long>((_, count, _, _) => Interlocked.Add(ref poisonCount, count));
        listener.Start();
        var transport = new Transport();
        using var ordered = new MySqlOrderedDispatcher(data.Source, new() { Lanes = 1, MaxDispatchAttempts = 2 },
            [transport], NullLogger<MySqlOrderedDispatcher>.Instance, instrumentation: instrumentation);
        await Assert.That(await ordered.DispatchLaneAsync(0, default)).IsEqualTo(0);
        await Assert.That(await ordered.DispatchLaneAsync(0, default)).IsEqualTo(0);
        await Assert.That(poisonCount).IsEqualTo(1L);
        await Assert.That(await ordered.DispatchLaneAsync(0, default)).IsEqualTo(1);
        await Assert.That(poisonCount).IsEqualTo(1L);
        await Assert.That(transport.Sent).IsEqualTo(1);
        await Assert.That(await Scalar(data, "SELECT COUNT(*) FROM relayer_outbox WHERE status='parked' AND headers=X'FF' AND last_error LIKE '%Malformed%'")).IsEqualTo(1L);
    }

    private static MySqlMaintenanceWorker Worker(MySqlFixture data, Transport transport) => new(data.Source,
        new() { BatchSize = 4, RecoveryDelay = TimeSpan.Zero, MaxDispatchAttempts = 2 }, [transport], NullLogger<MySqlMaintenanceWorker>.Instance);
    private static async Task<long> Scalar(MySqlFixture data, string sql)
    {
        await using var command = data.Source.CreateCommand(sql);
        return Convert.ToInt64(await command.ExecuteScalarAsync());
    }

    private sealed class Transport(Func<CancellationToken, ValueTask>? send = null) : ITransport
    {
        public int Sent { get; private set; }
        public string Name => "events";
        public TransportCapabilities GetCapabilities(string endpoint) => new() { MaxBatchSize = 1, OrderedByKey = true };
        public async ValueTask SendAsync(OutgoingMessage message, CancellationToken cancellationToken)
        {
            if (send is not null) await send(cancellationToken);
            Sent++;
        }
        public async ValueTask SendBatchAsync(ReadOnlyMemory<OutgoingMessage> messages, CancellationToken cancellationToken)
        {
            for (var i = 0; i < messages.Length; i++) await SendAsync(messages.Span[i], cancellationToken);
        }
        public ValueTask<TopologyReport> ApplyTopologyAsync(TopologyMode mode, CancellationToken cancellationToken) => new(new TopologyReport([]));
        public Task StartAsync(CancellationToken cancellationToken) => Task.CompletedTask;
        public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }
}

public sealed class MaintenanceRegistrationTests
{
    [Test]
    public async Task Named_immutable_options_bind_and_validate_before_connecting()
    {
        await using var source = new MySqlDataSource("Server=unused;Database=orders");
        var config = new ConfigurationBuilder().AddInMemoryCollection(new Dictionary<string, string?>
        { ["BatchSize"] = "7", ["DeadRetention"] = "infinite", ["InboxRetention"] = "2.00:00:00" }).Build();
        var services = new ServiceCollection().AddLogging();
        services.AddRelayerMySqlMaintenance("a", source, config);
        config["BatchSize"] = "99";
        await using var provider = services.BuildServiceProvider();
        var options = provider.GetRequiredService<IOptionsMonitor<MySqlMaintenanceOptions>>().Get("a");
        await Assert.That(options.BatchSize).IsEqualTo(7);
        await Assert.That(options.DeadRetention).IsNull();
        await Assert.That(options.InboxRetention).IsEqualTo(TimeSpan.FromDays(2));
        await Assert.That(provider.GetServices<IHostedService>().Count()).IsEqualTo(1);
        await Assert.That(() => services.AddRelayerMySqlMaintenance("a", source)).Throws<InvalidOperationException>();
        await using var undersized = new MySqlDataSource("Server=unused;Database=orders;MaximumPoolSize=1");
        await Assert.That(() => services.AddRelayerMySqlMaintenance("small", undersized)).Throws<ArgumentException>();
        await Assert.That(() => new MySqlMaintenanceOptions { BatchSize = 0 }.Validate()).Throws<ArgumentOutOfRangeException>();
        await Assert.That(() => new MySqlMaintenanceOptions { RecoveryDelay = TimeSpan.FromSeconds(-1) }.Validate()).Throws<ArgumentOutOfRangeException>();
        await Assert.That(() => new MySqlMaintenanceOptions { ClaimDuration = TimeSpan.Zero }.Validate()).Throws<ArgumentOutOfRangeException>();
    }
}
