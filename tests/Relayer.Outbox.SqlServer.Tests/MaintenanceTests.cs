using Microsoft.Data.SqlClient;
using System.Diagnostics;
using System.Text;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Relayer.Transports;

namespace Relayer.Outbox.SqlServer.Tests;

[ClassDataSource<SqlServerFixture>(Shared = SharedType.PerTestSession)]
public sealed class MaintenanceTests(SqlServerFixture database)
{
    [Test]
    [Arguments("headers=0x01", "header")]
    [Arguments("transport=N''", "transport")]
    [Arguments("message_type=N''", "message type")]
    [Arguments("destination=N' '", "destination")]
    public async Task Malformed_row_is_quarantined_and_unrelated_rows_dispatch_in_same_pass(string corruption, string error)
    {
        var data = new StoreData(database);
        await data.Create();
        var bad = Guid.CreateVersion7();
        var good = Guid.CreateVersion7();
        await data.Insert(bad, DateTimeOffset.UtcNow.AddDays(-1));
        await data.Insert(good, DateTimeOffset.UtcNow.AddDays(-1));
        await data.Execute($"UPDATE {data.Table} SET {corruption} WHERE id=0x{Convert.ToHexString(bad.ToByteArray(bigEndian: true))}");
        var transport = new Transport();
        using var worker = Worker(data, transport);
        await Assert.That(await worker.SweepAsync()).IsEqualTo(2);
        await Assert.That(transport.Sent).IsEqualTo(1);
        await Assert.That(await Scalar($"SELECT COUNT_BIG(*) FROM {SqlServerOutboxSchema.DeadTable(data.Options)} WHERE last_error LIKE '%{error}%' AND attempts=1")).IsEqualTo(1L);
        await Assert.That(await Scalar($"SELECT COUNT_BIG(*) FROM {data.Table}")).IsEqualTo(0L);
    }

    [Test]
    public async Task Repeated_recovery_query_failures_do_not_suppress_inbox_retention()
    {
        var data = new StoreData(database);
        await data.Create();
        await data.Insert(Guid.CreateVersion7(), DateTimeOffset.UtcNow.AddDays(-1));
        var inbox = SqlServerOutboxSchema.InboxTable(data.Options);
        // Break recovery's row projection while leaving the retention tables and predicates valid.
        await data.Execute($"EXEC sp_rename N'{data.Table}.headers', N'broken_headers', N'COLUMN'; " +
            $"INSERT INTO {inbox} (consumer, message_id, processed_at) VALUES ('expired', NEWID(), DATEADD(day,-30,SYSUTCDATETIME()))");
        var logger = new FailureLogger();
        using var worker = new SqlServerMaintenanceWorker(
            new SqlConnectionStringBuilder(database.ConnectionString) { InitialCatalog = "master" }.ConnectionString, new()
        {
            TablePrefix = data.Options.TablePrefix, RecoveryDelay = TimeSpan.Zero,
            SweepInterval = TimeSpan.FromMilliseconds(1)
        }, [new Transport()], logger);
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        await worker.StartAsync(timeout.Token);
        try
        {
            // The second sweep error proves the first iteration has finished its cleanup attempt.
            await logger.SecondFailure.Task.WaitAsync(timeout.Token);
            await Assert.That(await Scalar($"SELECT COUNT_BIG(*) FROM {inbox}")).IsEqualTo(0L);
        }
        finally { await worker.StopAsync(timeout.Token); }
    }

    private sealed class FailureLogger : ILogger<SqlServerMaintenanceWorker>
    {
        private int _failures;
        public TaskCompletionSource SecondFailure { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;
        public bool IsEnabled(LogLevel logLevel) => true;
        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception,
            Func<TState, Exception?, string> formatter)
        {
            if (eventId.Id == 1 && Interlocked.Increment(ref _failures) == 2) SecondFailure.TrySetResult();
        }
    }

    [Test]
    public async Task Connected_leader_excludes_rival_and_idle_expiry_allows_takeover()
    {
        var data = new StoreData(database);
        await data.Create();
        await data.Insert(Guid.CreateVersion7(), DateTimeOffset.UtcNow.AddDays(-1));
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var transport = new Transport(async token => { entered.TrySetResult(); await release.Task.WaitAsync(token); });
        using var first = Worker(data, transport);
        using var rival = Worker(data, new());
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        var pass = first.SweepAsync(timeout.Token).AsTask();
        try
        {
            await entered.Task.WaitAsync(timeout.Token);
            await Assert.That(await rival.SweepAsync(timeout.Token)).IsEqualTo(0);
        }
        finally { release.TrySetResult(); }
        await Assert.That(await pass).IsEqualTo(1);
        await data.Insert(Guid.CreateVersion7(), DateTimeOffset.UtcNow.AddDays(-1));
        await Assert.That(await rival.SweepAsync(timeout.Token)).IsEqualTo(0);
        await data.Execute($"UPDATE {SqlServerOutboxSchema.LeaseTable(data.Options)} SET expires_at=DATEADD(second,-1,SYSUTCDATETIME())");
        await Assert.That(await rival.SweepAsync(timeout.Token)).IsEqualTo(1);
    }

    [Test]
    public async Task Interrupted_send_rolls_back_claim_and_preserves_original_identity_for_recovery()
    {
        var data = new StoreData(database);
        await data.Create();
        var id = Guid.CreateVersion7();
        await data.Insert(id, DateTimeOffset.UtcNow.AddDays(-1));
        using var timeout = new CancellationTokenSource();
        var transport = new Transport(token => { timeout.Cancel(); return ValueTask.FromCanceled(token); });
        using var first = Worker(data, transport);
        await Assert.ThrowsAsync<OperationCanceledException>(() => first.SweepAsync(timeout.Token).AsTask());
        var rows = new OutboxRecord[1];
        await Assert.That(await data.Store().ReadPendingAsync(new(null, DateTimeOffset.UtcNow), rows, default)).IsEqualTo(1);
        await Assert.That(rows[0].Id).IsEqualTo(id);
        using var rival = Worker(data, new());
        await Assert.That(await rival.SweepAsync()).IsEqualTo(1);
    }

    [Test]
    public async Task Killed_process_recovers_broker_accepted_identity_after_connection_rollback()
    {
        var data = new StoreData(database);
        await data.Create();
        var id = Guid.CreateVersion7();
        await data.Insert(id, DateTimeOffset.UtcNow.AddDays(-1));
        using var headers = new MemoryStream();
        using (var writer = new BinaryWriter(headers, Encoding.UTF8, leaveOpen: true))
        {
            writer.Write(1); writer.Write(1); writer.Write(5); writer.Write(Encoding.UTF8.GetBytes("ce_id"));
            var value = Encoding.UTF8.GetBytes(id.ToString()); writer.Write(value.Length); writer.Write(value);
        }
        var acceptances = $"[dbo].[{data.Options.TablePrefix}acceptances]";
        await data.Execute($"UPDATE {data.Table} SET headers=0x{Convert.ToHexString(headers.ToArray())}; CREATE TABLE {acceptances} (id uniqueidentifier NOT NULL)");
        var start = new ProcessStartInfo("dotnet") { RedirectStandardOutput = true, RedirectStandardError = true, CreateNoWindow = true };
        foreach (var argument in new[] { "exec", "--runtimeconfig", Path.ChangeExtension(typeof(MaintenanceTests).Assembly.Location, ".runtimeconfig.json"),
            "--depsfile", Path.ChangeExtension(typeof(MaintenanceTests).Assembly.Location, ".deps.json"), typeof(CrashProbeMarker).Assembly.Location,
            "sqlserver", new SqlConnectionStringBuilder(database.ConnectionString) { InitialCatalog = "master" }.ConnectionString, data.Options.TablePrefix })
            start.ArgumentList.Add(argument);
        using var child = Process.Start(start)!;
        var errors = child.StandardError.ReadToEndAsync();
        try
        {
            var signal = await child.StandardOutput.ReadLineAsync().WaitAsync(TimeSpan.FromSeconds(30));
            if (signal != "BROKER_ACCEPTED")
            {
                if (!child.HasExited) { child.Kill(entireProcessTree: true); await child.WaitForExitAsync(); }
                throw new InvalidOperationException($"Crash probe did not reach send: {signal}; {await errors}");
            }
        }
        finally
        {
            if (!child.HasExited) { child.Kill(entireProcessTree: true); await child.WaitForExitAsync(); }
        }
        await Assert.That(await Scalar($"SELECT COUNT_BIG(*) FROM {acceptances} WHERE id='{id}'")).IsEqualTo(1L);
        var rows = new OutboxRecord[1];
        await Assert.That(await data.Store().ReadPendingAsync(new(null, DateTimeOffset.UtcNow), rows, default)).IsEqualTo(1);
        await Assert.That(rows[0].Id).IsEqualTo(id);
        var transport = new Transport();
        using var rival = Worker(data, transport);
        await Assert.That(await rival.SweepAsync()).IsEqualTo(1);
        await Assert.That(transport.Sent).IsEqualTo(1);
    }

    [Test]
    public async Task Failure_threshold_transfers_unordered_rows_and_fences_stale_tokens()
    {
        var data = new StoreData(database, new() { TablePrefix = "poison_" + Guid.NewGuid().ToString("N") + "_", MaxDispatchAttempts = 2 });
        await data.Create();
        var id = Guid.CreateVersion7();
        await data.Insert(id, DateTimeOffset.UtcNow);
        var store = data.Store();
        var now = DateTimeOffset.UtcNow;
        var stale = new OutboxClaim(id, Guid.NewGuid());
        await Assert.That(await store.TryClaimAsync(stale, now, now.AddMinutes(1), default)).IsTrue();
        await Assert.That(await store.RecordFailureAsync(stale, "offline", now, default)).IsTrue();
        var current = new OutboxClaim(id, Guid.NewGuid());
        await Assert.That(await store.TryClaimAsync(current, now, now.AddMinutes(1), default)).IsTrue();
        await Assert.That(await store.RecordFailureAsync(stale, "stale", now, default)).IsFalse();
        await Assert.That(await store.RecordFailureAsync(current, "offline again", now, default)).IsTrue();
        await Assert.That(await Scalar($"SELECT COUNT_BIG(*) FROM {data.Table}")).IsEqualTo(0L);
        await Assert.That(await Scalar($"SELECT COUNT_BIG(*) FROM {SqlServerOutboxSchema.DeadTable(data.Options)} WHERE attempts=2 AND last_error='offline again'")).IsEqualTo(1L);
    }

    [Test]
    public async Task Exhausted_ordered_row_parks_and_cleanup_preserves_its_dead_barrier()
    {
        var data = new StoreData(database, new() { TablePrefix = "park_" + Guid.NewGuid().ToString("N") + "_", MaxDispatchAttempts = 1 });
        await data.Create();
        var id = Guid.CreateVersion7();
        await data.Insert(id, DateTimeOffset.UtcNow, lane: 3);
        var store = data.Store();
        var claim = new OutboxClaim(id, Guid.NewGuid());
        var now = DateTimeOffset.UtcNow;
        await Assert.That(await store.TryClaimAsync(claim, now, now.AddMinutes(1), default)).IsTrue();
        await Assert.That(await store.RecordFailureAsync(claim, "poison", now, default)).IsTrue();
        var dead = SqlServerOutboxSchema.DeadTable(data.Options);
        await data.Execute($"UPDATE {dead} SET failed_at=DATEADD(day,-60,SYSUTCDATETIME())");
        using var worker = Worker(data, new());
        await Assert.That(await worker.CleanupAsync()).IsEqualTo(0);
        await Assert.That(await Scalar($"SELECT COUNT_BIG(*) FROM {data.Table} WHERE status='parked' AND lane=3 AND attempts=1")).IsEqualTo(1L);
        await Assert.That(await Scalar($"SELECT COUNT_BIG(*) FROM {dead}")).IsEqualTo(1L);
    }

    [Test]
    public async Task Unregistered_transport_consumes_recovery_attempts_without_stalling_other_groups()
    {
        var data = new StoreData(database, new() { TablePrefix = "missing_" + Guid.NewGuid().ToString("N") + "_", MaxDispatchAttempts = 2 });
        await data.Create();
        var id = Guid.CreateVersion7();
        await data.Insert(id, DateTimeOffset.UtcNow.AddDays(-1));
        await data.Execute($"UPDATE {data.Table} SET transport='missing'");
        await data.Insert(Guid.CreateVersion7(), DateTimeOffset.UtcNow.AddDays(-1));
        var transport = new Transport();
        using var worker = Worker(data, transport);
        await Assert.That(await worker.SweepAsync()).IsEqualTo(2);
        await Assert.That(transport.Sent).IsEqualTo(1);
        await worker.SweepAsync(); // Wrap the fixed traversal boundary.
        await Assert.That(await worker.SweepAsync()).IsEqualTo(1);
        await Assert.That(await Scalar($"SELECT COUNT_BIG(*) FROM {SqlServerOutboxSchema.DeadTable(data.Options)} WHERE last_error LIKE '%missing%'")).IsEqualTo(1L);
    }

    private SqlServerMaintenanceWorker Worker(StoreData data, Transport transport) => new(
        new SqlConnectionStringBuilder(database.ConnectionString) { InitialCatalog = "master", MaxPoolSize = 2 }.ConnectionString,
        new() { TablePrefix = data.Options.TablePrefix, MaxDispatchAttempts = data.Options.MaxDispatchAttempts, BatchSize = 4, RecoveryDelay = TimeSpan.Zero },
        [transport], NullLogger<SqlServerMaintenanceWorker>.Instance);

    private async Task<long> Scalar(string sql)
    {
        await using var connection = new SqlConnection(database.ConnectionString);
        await connection.OpenAsync();
        await using var command = new SqlCommand(sql, connection);
        return (long)(await command.ExecuteScalarAsync())!;
    }

    private sealed class Transport(Func<CancellationToken, ValueTask>? send = null) : ITransport
    {
        public int Sent { get; private set; }
        public string Name => "events";
        public TransportCapabilities GetCapabilities(string endpoint) => new() { MaxBatchSize = 1 };
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
    public async Task Named_options_bind_immutable_snapshots_and_validate_before_connecting()
    {
        var config = new ConfigurationBuilder().AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["BatchSize"] = "7", ["DeadRetention"] = "infinite", ["InboxRetention"] = "2.00:00:00"
        }).Build();
        var services = new ServiceCollection().AddLogging();
        services.AddRelayerSqlServerMaintenance("a", "Server=unused;Database=orders", config);
        config["BatchSize"] = "99";
        await using var provider = services.BuildServiceProvider();
        var options = provider.GetRequiredService<IOptionsMonitor<SqlServerMaintenanceOptions>>().Get("a");
        await Assert.That(options.BatchSize).IsEqualTo(7);
        await Assert.That(options.DeadRetention).IsNull();
        await Assert.That(options.InboxRetention).IsEqualTo(TimeSpan.FromDays(2));
        await Assert.That(provider.GetServices<IHostedService>().Count()).IsEqualTo(1);
        await Assert.That(() => services.AddRelayerSqlServerMaintenance("a", "Server=unused;Database=orders")).Throws<InvalidOperationException>();
        await Assert.That(() => new SqlServerMaintenanceOptions { BatchSize = 0 }.Validate()).Throws<ArgumentOutOfRangeException>();
        await Assert.That(() => new SqlServerMaintenanceOptions { RecoveryDelay = TimeSpan.FromSeconds(-1) }.Validate()).Throws<ArgumentOutOfRangeException>();
        await Assert.That(() => new SqlServerMaintenanceOptions { ClaimDuration = TimeSpan.Zero }.Validate()).Throws<ArgumentOutOfRangeException>();
    }
}
