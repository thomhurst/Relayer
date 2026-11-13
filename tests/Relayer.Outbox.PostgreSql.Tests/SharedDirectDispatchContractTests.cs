using Relayer.Transports;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.Extensions.DependencyInjection;
using Relayer.Outbox.EntityFrameworkCore;
using Relayer.TestFixtures;

namespace Relayer.Outbox.PostgreSql.Tests;

[InheritsTests]
public sealed class SharedDirectDispatchContractTests : OutboxDirectDispatchContractTests
{
    protected override async Task<DispatchFixture> CreateFixtureAsync(RecordingTransport transport, int batchSize = 100,
        int queueCapacity = 256)
    {
        var fixture = new Fixture(transport);
        try { await fixture.Start(batchSize, queueCapacity); return fixture; }
        catch { await fixture.DisposeAsync(); throw; }
    }

    private sealed class Fixture(RecordingTransport transport) : DispatchFixture
    {
        private readonly DirectDispatchTests.Database _database = new();
        private ServiceProvider? _provider;
        private PostgreSqlDirectDispatcher? _worker;

        public async Task Start(int batchSize, int queueCapacity)
        {
            await _database.Start();
            _provider = _database.Services(transport, new() { BatchSize = batchSize, QueueCapacity = queueCapacity });
            _worker = _provider.GetRequiredKeyedService<PostgreSqlDirectDispatcher>("store");
            await _worker.StartAsync(default);
        }

        public override async Task<DispatchSession> OpenSessionAsync()
        {
            var scope = _provider!.CreateAsyncScope();
            try { return new Session(scope); }
            catch { await scope.DisposeAsync(); throw; }
        }

        public override async Task<Row[]> ReadRowsAsync()
        {
            await using var command = _database.Source.CreateCommand(
                $"SELECT id, attempts, last_error, claim_token, claim_expires_at FROM {DirectDispatchTests.Database.Table} ORDER BY id");
            await using var reader = await command.ExecuteReaderAsync();
            var rows = new List<Row>();
            while (await reader.ReadAsync())
                rows.Add(new(reader.GetGuid(0), reader.GetInt32(1), reader.IsDBNull(2) ? null : reader.GetString(2),
                    !reader.IsDBNull(3), !reader.IsDBNull(4)));
            return rows.ToArray();
        }

        public override ValueTask RecoverAsync(OutboxRecord[] records, CancellationToken cancellationToken = default) => _worker!.DispatchRecoveredAsync(records, cancellationToken);

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

    private sealed class Session : DispatchSession
    {
        private readonly AsyncServiceScope _scope;
        private readonly DirectDispatchTests.TestContext _context;
        private readonly IDisposable _selection;
        private readonly IOutboxEnlistment _enlistment;
        private IDbContextTransaction? _transaction;

        public Session(AsyncServiceScope scope)
        {
            _scope = scope;
            _context = scope.ServiceProvider.GetRequiredService<DirectDispatchTests.TestContext>();
            _enlistment = scope.ServiceProvider.GetRequiredService<IOutboxEnlistment>();
            _selection = _context.EnlistRelayerOutbox();
        }

        public override async Task BeginTransactionAsync() => _transaction = await _context.Database.BeginTransactionAsync();
        public override ValueTask EnlistAsync(OutboxRecord record) => _enlistment.EnlistAsync(record, default);
        public override async Task SaveAsync() => await _context.SaveChangesAsync();

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
            finally
            {
                try { _selection.Dispose(); }
                finally { await _scope.DisposeAsync(); }
            }
        }
    }
}
