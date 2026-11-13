using Relayer.Transports;
using System.Buffers;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Npgsql;
using Relayer.TestFixtures;

namespace Relayer.Outbox.PostgreSql.Tests;

[InheritsTests]
public sealed class SharedShutdownContractTests : OutboxShutdownContractTests
{
    protected override async Task<ShutdownFixture> CreateFixtureAsync(string worker, ShutdownTransport transport, ShutdownLogs logs)
    {
        var fixture = new Fixture(worker, transport, logs);
        try { await fixture.PrepareAsync(); return fixture; }
        catch { await fixture.DisposeAsync(); throw; }
    }

    [Test]
    public async Task Capture_rejects_errors_shutdown_cancellation_and_commands_after_disposal()
    {
        var logs = new ShutdownLogs { DatabaseCommandCategory = "Npgsql.Command" };
        var worker = logs.CreateLogger("worker");
        worker.LogError("failed work");
        worker.LogCritical("critical work");
        worker.LogWarning(new OperationCanceledException(), "shutdown failed");
        logs.DatabaseClosed = true;
        logs.CreateLogger("Npgsql.Command").LogDebug("Executing command");
        await Assert.That(logs.Failures.Count).IsEqualTo(4);
        await Assert.That(logs.DatabaseCommands.Count).IsEqualTo(1);
    }

    [Test]
    public async Task Sender_probe_rejects_early_close_and_records_sends_after_close()
    {
        var sender = new ShutdownTransport(false);
        var send = sender.SendAsync(default, default).AsTask();
        try
        {
            await sender.Entered.Task;
            await Assert.ThrowsAsync<InvalidOperationException>(() => sender.StopAsync(default));
        }
        finally { sender.Release.TrySetResult(); await send; }
        await sender.StopAsync(default);
        await sender.SendAsync(default, default);
        await sender.DisposeAsync();
        await Assert.That(sender.SendsAfterClose).IsEqualTo(1);
    }

    private sealed class Fixture(string kind, ShutdownTransport transport, ShutdownLogs logs) : ShutdownFixture
    {
        private readonly PostgreSqlFixture _database = new();
        private readonly Guid _first = Guid.CreateVersion7();
        private readonly Guid _second = Guid.CreateVersion7();
        private readonly Guid _future = Guid.CreateVersion7();
        private readonly string _application = "shutdown-" + Guid.NewGuid().ToString("N");
        private NpgsqlDataSource? _observer;
        private IHost? _host;
        private Resources? _resources;
        private BackgroundService? _worker;
        private bool _disposed;

        public async Task PrepareAsync()
        {
            logs.DatabaseCommandCategory = "Npgsql.Command";
            await _database.StartAsync();
            _observer = NpgsqlDataSource.Create(_database.ConnectionString);
            await PostgreSqlOutboxStoreTests.CreateSchema(_observer);
            await Sql("INSERT INTO relayer_leases VALUES ('foreign', 'other-owner', clock_timestamp() + interval '1 day')");
            await Insert(_first, false);
            await Insert(_second, false);
            await Insert(_future, true);

            var builder = Host.CreateApplicationBuilder();
            builder.Logging.ClearProviders().SetMinimumLevel(LogLevel.Trace).AddProvider(logs);
            builder.Services.AddSingleton(provider => new Resources(
                new NpgsqlDataSourceBuilder(_database.ConnectionString + ";Application Name=" + _application)
                    .UseLoggerFactory(provider.GetRequiredService<ILoggerFactory>()).Build(), logs));
            builder.Services.AddSingleton<IHostedService>(_ => new SenderLifetime(transport));
            builder.Services.AddSingleton<IHostedService>(provider =>
            {
                _resources = provider.GetRequiredService<Resources>();
                var source = _resources.Source;
                _worker = kind switch
                {
                    "direct" => new PostgreSqlDirectDispatcher(source, new(), new() { BatchSize = 2 }, [transport],
                        provider.GetRequiredService<ILogger<PostgreSqlDirectDispatcher>>()),
                    "ordered" => new PostgreSqlOrderedDispatcher(source, new() { Lanes = 1 }, [transport],
                        provider.GetRequiredService<ILogger<PostgreSqlOrderedDispatcher>>()),
                    "maintenance" => new PostgreSqlMaintenanceWorker(source, new() { BatchSize = 2, RecoveryDelay = TimeSpan.Zero },
                        [transport], provider.GetRequiredService<ILogger<PostgreSqlMaintenanceWorker>>(),
                        provider.GetRequiredService<ILogger<PostgreSqlDirectDispatcher>>()),
                    _ => throw new ArgumentOutOfRangeException(nameof(kind))
                };
                _resources.Worker = _worker;
                return _worker;
            });
            _host = builder.Build();
            if (kind == "direct")
                _host.Services.GetRequiredService<IHostApplicationLifetime>().ApplicationStopping.Register(
                    () => transport.Stopping.TrySetResult());
        }

        public override async Task StartAsync()
        {
            // Persist an owned cleanup lease before the sweeper enters its uncommitted send transaction.
            // This makes the shutdown check prove release, even when the in-flight lease rolls back.
            _ = _host!.Services.GetServices<IHostedService>().ToArray();
            if (_worker is PostgreSqlMaintenanceWorker maintenance) await maintenance.CleanupAsync(default);
            await _host.StartAsync();
            if (_worker is PostgreSqlDirectDispatcher direct)
            {
                // Exercise the real post-commit handoff with rows already committed by native PostgreSQL.
                var rows = new List<BufferedOutboxMessage> { new(), new(), new() };
                rows[0].Copy(Record(_first, false), 1);
                rows[1].Copy(Record(_second, false), 1);
                rows[2].Copy(Record(_future, true), 1);
                direct.Committed(rows, rows.Count);
            }
        }

        public override async Task VerifyInFlightAsync()
        {
            await Assert.That(transport.Active).IsEqualTo(1);
            await Assert.That(logs.DatabaseCommands.Count).IsGreaterThan(0);
            if (kind == "direct")
                await Assert.That(await Scalar("SELECT count(*) FROM relayer_outbox WHERE claim_token IS NOT NULL AND claim_expires_at IS NOT NULL")).IsEqualTo(2L);
            else
            {
                await Assert.That(await Scalar("SELECT count(*) FROM relayer_leases")).IsGreaterThan(1L);
                // The lease insert and claim are uncommitted. Observe the native transaction's lease-table lock
                // rather than expecting those rows to be visible through the observer connection.
                await using var command = _observer!.CreateCommand("""
                    SELECT count(*) FROM pg_locks l JOIN pg_stat_activity a ON a.pid = l.pid
                    WHERE a.application_name = @app AND l.relation = 'relayer_leases'::regclass
                      AND l.mode = 'RowExclusiveLock' AND l.granted
                    """);
                command.Parameters.AddWithValue("app", _application);
                await Assert.That((long)(await command.ExecuteScalarAsync())!).IsGreaterThan(0L);
            }
        }

        public override Task StopAsync(CancellationToken token) => _host!.StopAsync(token);

        public override async Task VerifyDurableStateAsync()
        {
            await Assert.That(_worker!.ExecuteTask!.IsCompletedSuccessfully).IsTrue();
            await Assert.That(transport.Delivered).IsEqualTo(kind == "direct" ? 2 : 0);
            await Assert.That(await Scalar("SELECT count(*) FROM relayer_outbox")).IsEqualTo(kind == "direct" ? 1L : 3L);
            await Assert.That(await Scalar("SELECT count(*) FROM relayer_outbox WHERE attempts <> 0 OR last_error IS NOT NULL OR claim_token IS NOT NULL OR claim_expires_at IS NOT NULL")).IsEqualTo(0L);
            await Assert.That(await Scalar("SELECT count(*) FROM relayer_outbox_dead")).IsEqualTo(0L);
            await Assert.That(await Scalar("SELECT count(*) FROM relayer_leases")).IsEqualTo(1L);
            await Assert.That(await Scalar("SELECT count(*) FROM relayer_leases WHERE name = 'foreign' AND owner = 'other-owner' AND expires_at > clock_timestamp()")).IsEqualTo(1L);
            await using var rows = _observer!.CreateCommand("SELECT id, body FROM relayer_outbox ORDER BY id");
            await using var reader = await rows.ExecuteReaderAsync();
            var ids = new List<Guid>();
            while (await reader.ReadAsync())
            {
                ids.Add(reader.GetGuid(0));
                await Assert.That(reader.GetFieldValue<byte[]>(1)).IsEquivalentTo(new byte[] { 7, 8, 9 });
            }
            await Assert.That(ids).IsEquivalentTo(kind == "direct" ? new[] { _future } : new[] { _first, _second, _future });
        }

        public override async ValueTask DisposeHostAsync()
        {
            if (_disposed || _host is null) return;
            _disposed = true;
            await ((IAsyncDisposable)_host).DisposeAsync();
        }

        public override async Task VerifyClosedAsync()
        {
            await Assert.That(_resources!.Closed).IsTrue();
            await Assert.That(_worker!.ExecuteTask!.IsCompletedSuccessfully).IsTrue();
            // Pooling is disabled: no worker connection may outlive host resource disposal.
            await using var command = _observer!.CreateCommand("SELECT count(*) FROM pg_stat_activity WHERE application_name = @app");
            command.Parameters.AddWithValue("app", _application);
            await Assert.That((long)(await command.ExecuteScalarAsync())!).IsEqualTo(0L);
        }

        public override async ValueTask DisposeAsync()
        {
            transport.Release.TrySetResult();
            try
            {
                if (!_disposed && _host is not null)
                {
                    using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(30));
                    try { await _host.StopAsync(deadline.Token); }
                    finally { await DisposeHostAsync(); }
                }
            }
            finally
            {
                try { if (_observer is not null) await _observer.DisposeAsync(); }
                finally { await _database.DisposeAsync(); }
            }
        }

        private OutboxRecord Record(Guid id, bool future) => new(id, "events", "shutdown.probe;v=1",
            new("orders", new byte[] { 7, 8, 9 }, new KeyValuePair<string, WireString>[] { new("ce_id", new(id.ToString())) },
                kind == "ordered" ? new WireString("customer") : null,
                future ? DateTimeOffset.UtcNow.AddDays(1) : null), DateTimeOffset.UtcNow.AddDays(-1));

        private async Task Insert(Guid id, bool future)
        {
            var record = Record(id, future);
            var headers = new ArrayBufferWriter<byte>();
            OutboxHeaderCodec.Write(headers, record.Message.Headers.Span);
            await using var command = _observer!.CreateCommand("""
                INSERT INTO relayer_outbox (id, transport, destination, message_type, headers, body, created_at, lane, ordering_key, deliver_at)
                VALUES (@id, 'events', 'orders', 'shutdown.probe;v=1', @headers, @body, clock_timestamp() - interval '1 day',
                  @lane, @key, clock_timestamp() + @due)
                """);
            command.Parameters.AddWithValue("id", id);
            command.Parameters.AddWithValue("headers", headers.WrittenMemory.ToArray());
            command.Parameters.AddWithValue("body", new byte[] { 7, 8, 9 });
            command.Parameters.AddWithValue("lane", NpgsqlTypes.NpgsqlDbType.Integer, kind == "ordered" ? 0 : DBNull.Value);
            command.Parameters.AddWithValue("key", NpgsqlTypes.NpgsqlDbType.Text, kind == "ordered" ? "customer" : DBNull.Value);
            command.Parameters.AddWithValue("due", future ? TimeSpan.FromDays(1) : TimeSpan.Zero);
            await command.ExecuteNonQueryAsync();
        }

        private async Task Sql(string sql)
        {
            await using var command = _observer!.CreateCommand(sql);
            await command.ExecuteNonQueryAsync();
        }
        private async Task<long> Scalar(string sql)
        {
            await using var command = _observer!.CreateCommand(sql);
            return (long)(await command.ExecuteScalarAsync())!;
        }
    }

    private sealed class Resources(NpgsqlDataSource source, ShutdownLogs logs) : IAsyncDisposable
    {
        public NpgsqlDataSource Source => source;
        public BackgroundService? Worker { get; set; }
        public bool Closed { get; private set; }
        public async ValueTask DisposeAsync()
        {
            if (Worker?.ExecuteTask?.IsCompletedSuccessfully != true)
                throw new InvalidOperationException("Database disposed before its worker completed.");
            await source.DisposeAsync();
            Closed = logs.DatabaseClosed = true;
        }
    }

    // Registered before the worker so Generic Host stops the worker before closing its native sender.
    private sealed class SenderLifetime(ShutdownTransport sender) : IHostedService, IAsyncDisposable
    {
        public Task StartAsync(CancellationToken token) => sender.StartAsync(token);
        public Task StopAsync(CancellationToken token) => sender.StopAsync(token);
        public ValueTask DisposeAsync() => sender.DisposeAsync();
    }
}
