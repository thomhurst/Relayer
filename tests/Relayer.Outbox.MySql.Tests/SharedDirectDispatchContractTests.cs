using System.Data.Common;
using MySqlConnector;
using Microsoft.Extensions.DependencyInjection;
using Relayer.TestFixtures;
using Relayer.Transports;

namespace Relayer.Outbox.MySql.Tests;

[InheritsTests]
public sealed class SharedDirectDispatchContractTests : OutboxDirectDispatchContractTests
{
    [Test]
    [Arguments("throw")]
    [Arguments("zero")]
    [Arguments("negative")]
    [Arguments("missing")]
    public async Task Invalid_destination_does_not_claim_rows_or_stall_other_groups(string failure)
    {
        var transport = new RecordingTransport { CapabilityFailure = failure };
        await using var fixture = new Fixture(transport);
        await fixture.Start(2, 10);
        var invalid = failure == "missing" ? "absent" : "events";
        await fixture.CommitAsync(Record(1, "alpha") with { TransportName = invalid },
            Record(2, "alpha") with { TransportName = invalid }, Record(3, "beta"));
        await fixture.StopAsync();
        await Assert.That(transport.Received.Select(message => message.Body[0])).IsEquivalentTo(new byte[] { 3 });
        var rows = await fixture.ReadRowsAsync();
        await Assert.That(rows.Length).IsEqualTo(2);
        await Assert.That(rows.All(row => row.Attempts == 0 && row.Error is null && !row.HasClaim)).IsTrue();
    }

    [Test]
    public async Task Completion_failure_keeps_sent_rows_claimed_without_incrementing_attempts()
    {
        var transport = new RecordingTransport();
        await using var fixture = new Fixture(transport);
        await fixture.Start(10, 10);
        await fixture.Sql($"CREATE TRIGGER `fail_{Guid.NewGuid():N}` BEFORE DELETE ON {fixture.Table} FOR EACH ROW SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'completion unavailable';");
        var record = Record(1);
        await fixture.CommitAsync(record);
        await fixture.StopAsync();
        await Assert.That(transport.Received.Count).IsEqualTo(1);
        await Assert.That(await fixture.ReadRowsAsync()).IsEquivalentTo(new[] { new Row(record.Id, 0, null, true, true) });
    }

    [Test]
    public async Task Expired_completion_preserves_row_and_attempt_count_after_successful_send()
    {
        var transport = new RecordingTransport { Block = true };
        await using var fixture = new Fixture(transport);
        await fixture.Start(10, 10);
        var record = Record(1);
        await fixture.CommitAsync(record);
        await transport.Entered.Task.WaitAsync(TimeSpan.FromSeconds(30));
        await fixture.Sql($"UPDATE {fixture.Table} SET claim_expires_at = UTC_TIMESTAMP(6) - INTERVAL 1 SECOND");
        transport.Release.TrySetResult();
        await fixture.StopAsync();
        await Assert.That(transport.Received.Count).IsEqualTo(1);
        await Assert.That(await fixture.ReadRowsAsync()).IsEquivalentTo(new[] { new Row(record.Id, 0, null, true, true) });
    }

    [Test]
    public async Task Competing_claim_skips_only_its_row_and_stale_completion_cannot_delete_it()
    {
        var transport = new RecordingTransport();
        await using var fixture = new Fixture(transport);
        await fixture.Start(10, 10, startWorker: false);
        var claimed = Record(1);
        await fixture.CommitAsync(claimed, Record(2));
        using var store = new MySqlOutboxStore(fixture.Source, fixture.Options);
        var now = DateTimeOffset.UtcNow;
        var competitor = new OutboxClaim(claimed.Id, Guid.NewGuid());
        await Assert.That(await store.TryClaimAsync(competitor, now, now.AddMinutes(5), default)).IsTrue();
        await fixture.Worker!.StartAsync(default);
        await fixture.StopAsync();
        await Assert.That(transport.Received.Select(message => message.Body[0])).IsEquivalentTo(new byte[] { 2 });
        await Assert.That(await fixture.ReadRowsAsync()).IsEquivalentTo(new[] { new Row(claimed.Id, 0, null, true, true) });
        await Assert.That(await store.CompleteAsync(new(claimed.Id, Guid.NewGuid()), now, default)).IsFalse();
        await Assert.That(await store.CompleteAsync(competitor, now, default)).IsTrue();
    }

    [Test]
    public async Task Shutdown_cancels_blocked_send_and_later_commits_stay_durable()
    {
        var transport = new RecordingTransport { Block = true };
        await using var fixture = new Fixture(transport);
        await fixture.Start(10, 10);
        await fixture.CommitAsync(Record(1));
        await transport.Entered.Task.WaitAsync(TimeSpan.FromSeconds(30));
        using var canceled = new CancellationTokenSource();
        canceled.Cancel();
        await Assert.That(async () => await fixture.Worker!.StopAsync(canceled.Token)).Throws<OperationCanceledException>();
        await fixture.Worker!.ExecuteTask!.WaitAsync(TimeSpan.FromSeconds(30));
        await fixture.CommitAsync(Record(2));
        await Assert.That(transport.Received.Count).IsEqualTo(0);
        await Assert.That((await fixture.ReadRowsAsync()).Length).IsEqualTo(2);
    }

    [Test]
    public async Task Synchronous_commit_dispatches_and_scope_disposal_never_publishes_uncommitted_work()
    {
        var transport = new RecordingTransport();
        await using var fixture = new Fixture(transport);
        await fixture.Start(10, 10);
        await using (var scope = fixture.Provider!.CreateAsyncScope())
        {
            var outbox = scope.ServiceProvider.GetRequiredService<MySqlOutbox>();
            using var transaction = await outbox.BeginTransactionAsync(fixture.Source);
            await outbox.EnlistAsync(Record(1), default);
            transaction.Commit();
            await using var abandoned = await outbox.BeginTransactionAsync(fixture.Source);
            await outbox.EnlistAsync(Record(2), default);
        }
        await fixture.StopAsync();
        await Assert.That(transport.Received.Select(message => message.Body[0])).IsEquivalentTo(new byte[] { 1 });
        await Assert.That((await fixture.ReadRowsAsync()).Length).IsEqualTo(0);
    }

    private static OutboxRecord Record(byte value, string destination = "destination") => new(Guid.CreateVersion7(),
        "events", "dispatch.probe;v=1", new OutgoingMessage(destination, new byte[] { value }), DateTimeOffset.UtcNow);

    [Test]
    public async Task Native_batch_outcomes_fence_each_row_and_ignore_trigger_writes()
    {
        var transport = new RecordingTransport();
        await using var fixture = new Fixture(transport);
        await fixture.Start(10, 10, startWorker: false);
        var records = new[] { Record(1), Record(2), Record(3) };
        await fixture.CommitAsync(records);
        using var store = new MySqlOutboxStore(fixture.Source, fixture.Options);
        var now = DateTimeOffset.UtcNow;
        var claims = records.Select(row => new OutboxClaim(row.Id, Guid.NewGuid())).ToArray();
        var competitor = new OutboxClaim(records[1].Id, Guid.NewGuid());
        await Assert.That(await store.TryClaimAsync(competitor, now, now.AddMinutes(5), default)).IsTrue();
        var outcomes = new bool[3];
        await store.ClaimBatchAsync(claims, outcomes, now, now.AddMinutes(5), default);
        await Assert.That(outcomes).IsEquivalentTo(new[] { true, false, true });
        await fixture.Sql("CREATE TABLE audit_rows (value INT)");
        await fixture.Sql($"CREATE TRIGGER audit_delete AFTER DELETE ON {fixture.Table} FOR EACH ROW INSERT INTO audit_rows VALUES (1), (2)");
        await Assert.That(await store.CompleteBatchAsync(claims, now, default)).IsEqualTo(2L);
        await Assert.That(await store.CompleteBatchAsync(claims, now, default)).IsEqualTo(0L);
        await Assert.That(await store.CompleteBatchAsync(new[] { competitor }, now, default)).IsEqualTo(1L);
        await Assert.That((await fixture.ReadRowsAsync()).Length).IsEqualTo(0);
    }

    [Test]
    public async Task Changed_caller_database_is_rejected_before_beginning_native_transaction()
    {
        var transport = new RecordingTransport();
        await using var fixture = new Fixture(transport);
        await fixture.Start(10, 10);
        await using var scope = fixture.Provider!.CreateAsyncScope();
        var outbox = scope.ServiceProvider.GetRequiredService<MySqlOutbox>();
        await using var connection = await fixture.Source.OpenConnectionAsync();
        var database = connection.Database;
        await connection.ChangeDatabaseAsync("mysql");
        await Assert.That(async () => await outbox.BeginTransactionAsync(connection)).Throws<InvalidOperationException>();
        await Assert.That(outbox.IsActive).IsFalse();
        await connection.ChangeDatabaseAsync(database);
        await using var transaction = await outbox.BeginTransactionAsync(connection);
        await outbox.EnlistAsync(Record(1), default);
        await transaction.CommitAsync();
        await fixture.StopAsync();
        await Assert.That(transport.Received.Count).IsEqualTo(1);
    }

    protected override async Task<DispatchFixture> CreateFixtureAsync(RecordingTransport transport, int batchSize = 100,
        int queueCapacity = 256)
    {
        var fixture = new Fixture(transport);
        try { await fixture.Start(batchSize, queueCapacity); return fixture; }
        catch { await fixture.DisposeAsync(); throw; }
    }

    private sealed class Fixture(RecordingTransport transport) : DispatchFixture
    {
        private readonly MySqlOutboxOptions _options = new()
        {
            TablePrefix = "dispatch_" + Guid.NewGuid().ToString("N") + "_",
            DispatchStoreName = "store"
        };
        private readonly MySqlFixture _database = new();
        internal MySqlDataSource Source => _database.Source;
        private ServiceProvider? _provider;
        private MySqlDirectDispatcher? _worker;
        internal string Table => MySqlOutboxSchema.Table(_options);
        internal MySqlOutboxOptions Options => _options;
        internal ServiceProvider? Provider => _provider;
        internal MySqlDirectDispatcher? Worker => _worker;

        internal async Task Start(int batchSize, int queueCapacity, bool startWorker = true)
        {
            await _database.StartAsync();
            await using var connection = await Source.OpenConnectionAsync();
            await using var create = new MySqlCommand(MySqlOutboxSchema.CreateScript(_options), connection);
            await create.ExecuteNonQueryAsync();
            var services = new ServiceCollection();
            services.AddLogging();
            services.AddSingleton<ITransport>(transport);
            services.AddRelayerMySqlOutbox(_options);
            services.AddRelayerMySqlDirectDispatch("store", Source, _options,
                new() { BatchSize = batchSize, QueueCapacity = queueCapacity });
            _provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true });
            _worker = _provider.GetRequiredKeyedService<MySqlDirectDispatcher>("store");
            if (startWorker) await _worker.StartAsync(default);
        }

        internal async Task Sql(string sql)
        {
            await using var connection = await Source.OpenConnectionAsync();
            await using var command = new MySqlCommand(sql, connection);
            await command.ExecuteNonQueryAsync();
        }

        public override Task<DispatchSession> OpenSessionAsync() => Task.FromResult<DispatchSession>(
            new Session(_provider!.CreateAsyncScope(), Source));

        public override async Task<Row[]> ReadRowsAsync()
        {
            await using var connection = await Source.OpenConnectionAsync();
            await using var command = new MySqlCommand(
                $"SELECT id, attempts, last_error, claim_token, claim_expires_at FROM {Table} ORDER BY id", connection);
            await using var reader = await command.ExecuteReaderAsync();
            var rows = new List<Row>();
            while (await reader.ReadAsync())
                rows.Add(new(ReadId(reader), reader.GetInt32(1),
                    reader.IsDBNull(2) ? null : reader.GetString(2), !reader.IsDBNull(3), !reader.IsDBNull(4)));
            return rows.ToArray();
        }

        private static Guid ReadId(MySqlDataReader reader)
        {
            var bytes = new byte[16];
            reader.GetBytes(0, 0, bytes, 0, 16);
            return new(bytes, bigEndian: true);
        }

        public override async ValueTask RecoverAsync(OutboxRecord[] records, CancellationToken cancellationToken = default)
        {
            var rows = new List<BufferedOutboxMessage>();
            foreach (var record in records)
            {
                var row = new BufferedOutboxMessage();
                row.Copy(record, 1);
                rows.Add(row);
            }
            var batch = new OutboxDispatchBatch();
            batch.Take(rows, rows.Count, DateTimeOffset.UtcNow);
            await _worker!.DispatchRecoveredAsync(batch, cancellationToken);
        }

        public override async Task StopAsync()
        {
            if (_worker is null) return;
            using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30));
            await _worker.StopAsync(timeout.Token);
        }

        public override async ValueTask DisposeAsync()
        {
            transport.Release.TrySetResult();
            try { await StopAsync(); }
            finally
            {
                try { if (_provider is not null) await _provider.DisposeAsync(); }
                finally { await _database.DisposeAsync(); }
            }
        }
    }

    private sealed class Session(AsyncServiceScope scope, MySqlDataSource source) : DispatchSession
    {
        private readonly MySqlOutbox _outbox = scope.ServiceProvider.GetRequiredService<MySqlOutbox>();
        private DbTransaction? _transaction;
        public override async Task BeginTransactionAsync() => _transaction = await _outbox.BeginTransactionAsync(source);
        public override ValueTask EnlistAsync(OutboxRecord record) => _outbox.EnlistAsync(record, default);
        public override Task SaveAsync() => Task.CompletedTask;
        public override async Task CommitAsync()
        {
            await _transaction!.CommitAsync();
            await _transaction.DisposeAsync();
            _transaction = null;
        }
        public override async Task RollbackAsync()
        {
            await _transaction!.RollbackAsync();
            await _transaction.DisposeAsync();
            _transaction = null;
        }
        public override async ValueTask DisposeAsync()
        {
            try { if (_transaction is not null) await _transaction.DisposeAsync(); }
            finally { await scope.DisposeAsync(); }
        }
    }
}
