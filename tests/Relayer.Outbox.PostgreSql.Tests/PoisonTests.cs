using System.Diagnostics.Metrics;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using Relayer.Testing;
using Relayer.Transports;

namespace Relayer.Outbox.PostgreSql.Tests;

public class PoisonTests
{
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task Attempt_limit_is_fenced_durable_and_redrive_resets_attempts(bool ordered)
    {
        await using var fixture = new PostgreSqlFixture();
        await fixture.StartAsync();
        await using var source = NpgsqlDataSource.Create(fixture.ConnectionString);
        await PostgreSqlOutboxStoreTests.CreateSchema(source);
        var options = new PostgreSqlOutboxOptions { MaxDispatchAttempts = 2 };
        var id = Guid.CreateVersion7();
        await PostgreSqlOutboxStoreTests.Insert(source, options, id, DateTimeOffset.UtcNow, lane: ordered ? 0 : null);
        var store = new PostgreSqlOutboxStore(source, options);

        for (var attempt = 1; attempt <= 2; attempt++)
        {
            var claim = new OutboxClaim(id, Guid.NewGuid());
            var now = DateTimeOffset.UtcNow;
            await Assert.That(await store.TryClaimAsync(claim, now, now.AddMinutes(1), default)).IsTrue();
            await Assert.That(await store.RecordDispatchFailureAsync(new(id, Guid.NewGuid()), "stale", now, default)).IsEqualTo(0);
            await Assert.That(await store.RecordDispatchFailureAsync(claim, "broker failed", now, default)).IsEqualTo(attempt);
        }

        var services = new ServiceCollection().AddRelayerPostgreSqlDeadLetters("store", source, options);
        await using var provider = services.BuildServiceProvider();
        var manager = provider.GetRequiredKeyedService<IDeadLetterManager>("store");
        var page = await manager.PeekAsync("store");
        await Assert.That(page.Messages.Count).IsEqualTo(1);
        var details = page.Messages[0].Features.GetRequired<PostgreSqlDeadLetterDetails>();
        await Assert.That(details.Record.Id).IsEqualTo(id);
        await Assert.That(details.Record.Attempts).IsEqualTo(2);
        await Assert.That(details.Record.LastError).IsEqualTo("broker failed");
        await Assert.That(details.Record.TenantId).IsEqualTo("tenant");
        await Assert.That(page.Messages[0].Body.ToArray()).IsEquivalentTo(new byte[] { 0, 255, 1 });
        await Assert.That(await manager.RedriveAsync("store", _ => false)).IsEqualTo(0);
        await Assert.That(async () => await manager.RedriveAsync("store", _ => throw new InvalidOperationException("filter"))).Throws<InvalidOperationException>();
        await Assert.That((await manager.PeekAsync("store")).Messages.Count).IsEqualTo(1);
        var competing = new PostgreSqlDeadLetterManager("store", source, options);
        var redrives = await Task.WhenAll(manager.RedriveAsync("store").AsTask(), competing.RedriveAsync("store").AsTask());
        await Assert.That(redrives.Sum()).IsEqualTo(1);
        var rows = new OutboxRecord[1];
        await Assert.That(await store.ReadPendingAsync(new(ordered ? 0 : null, DateTimeOffset.UtcNow), rows, default)).IsEqualTo(1);
        await Assert.That(rows[0].Id).IsEqualTo(id);
        await Assert.That(rows[0].Attempts).IsEqualTo(0);
        await Assert.That((await manager.PeekAsync("store")).Messages.Count).IsEqualTo(0);
    }

    [Test]
    public async Task Rolled_back_failure_can_be_replayed_without_losing_payload_or_consuming_attempt()
    {
        await using var fixture = new PostgreSqlFixture();
        await fixture.StartAsync();
        await using var source = NpgsqlDataSource.Create(fixture.ConnectionString);
        await PostgreSqlOutboxStoreTests.CreateSchema(source);
        var options = new PostgreSqlOutboxOptions { MaxDispatchAttempts = 1 };
        var id = Guid.CreateVersion7();
        await PostgreSqlOutboxStoreTests.Insert(source, options, id, DateTimeOffset.UtcNow);
        var store = new PostgreSqlOutboxStore(source, options);

        await using (var connection = await source.OpenConnectionAsync())
        await using (var transaction = await connection.BeginTransactionAsync())
        {
            store.UseDispatchTransaction(connection, transaction);
            var now = DateTimeOffset.UtcNow;
            var claim = new OutboxClaim(id, Guid.NewGuid());
            await store.TryClaimAsync(claim, now, now.AddMinutes(1), default);
            await Assert.That(await store.RecordDispatchFailureAsync(claim, "rollback", now, default)).IsEqualTo(2);
            await transaction.RollbackAsync();
            store.UseDispatchTransaction(null, null);
        }

        await using (var unordered = source.CreateCommand("UPDATE relayer_outbox SET ordering_key = NULL"))
        {
            await unordered.ExecuteNonQueryAsync();
        }

        var transport = new FailingTransport();
        using var dispatcher = new PostgreSqlDirectDispatcher(source, options, new(), [transport], NullLogger<PostgreSqlDirectDispatcher>.Instance);
        var rows = new OutboxRecord[1];
        await store.ReadPendingAsync(new(null, DateTimeOffset.UtcNow), rows, default);
        await Assert.That(rows[0].Attempts).IsEqualTo(0);
        await dispatcher.DispatchRecoveredAsync(rows, default);
        // A restarted recovery worker sees no pending poison and cannot send it again.
        await Assert.That(await store.ReadPendingAsync(new(null, DateTimeOffset.UtcNow), rows, default)).IsEqualTo(0);
        var manager = new PostgreSqlDeadLetterManager("store", source, options);
        await Assert.That((await manager.PeekAsync("store")).Messages[0].Features.GetRequired<PostgreSqlDeadLetterDetails>().Record.Attempts).IsEqualTo(1);
        await Assert.That(await manager.PurgeAsync("store")).IsEqualTo(1);
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task Direct_dispatch_poison_settles_pending_and_harness_work_once_committed(bool recoveryTransaction)
    {
        await using var fixture = new PostgreSqlFixture();
        await fixture.StartAsync();
        await using var source = NpgsqlDataSource.Create(fixture.ConnectionString);
        await PostgreSqlOutboxStoreTests.CreateSchema(source);
        var options = new PostgreSqlOutboxOptions { MaxDispatchAttempts = 1 };
        await PostgreSqlOutboxStoreTests.Insert(source, options, Guid.CreateVersion7(), DateTimeOffset.UtcNow);

        await using (var unordered = source.CreateCommand("UPDATE relayer_outbox SET ordering_key = NULL"))
        {
            await unordered.ExecuteNonQueryAsync();
        }

        using var instrumentation = new RelayerInstrumentation();
        long pending = 0, poisoned = 0;
        using var listener = new MeterListener();
        listener.InstrumentPublished = (instrument, meterListener) =>
        {
            if (instrument == instrumentation.OutboxPending || instrument == instrumentation.OutboxPoison)
            {
                meterListener.EnableMeasurementEvents(instrument);
            }
        };
        listener.SetMeasurementEventCallback<long>((instrument, value, _, _) =>
            Interlocked.Add(ref instrument == instrumentation.OutboxPending ? ref pending : ref poisoned, value));
        listener.Start();
        var harness = new RelayerHarness();
        instrumentation.Observer = harness;
        // Stands for the commit that wrote the row in this provider.
        instrumentation.AddOutboxPending(1);
        var idle = harness.WaitForIdleAsync();
        var store = new PostgreSqlOutboxStore(source, options);
        using var dispatcher = new PostgreSqlDirectDispatcher(source, options, new(), [new FailingTransport()],
            NullLogger<PostgreSqlDirectDispatcher>.Instance, store, instrumentation);
        var rows = new OutboxRecord[1];
        await store.ReadPendingAsync(new(null, DateTimeOffset.UtcNow), rows, default);
        await dispatcher.DispatchRecoveredAsync(rows, default, propagateDatabaseFailures: recoveryTransaction);

        if (recoveryTransaction)
        {
            // Recovery reports poison only after its transaction commits.
            await Assert.That(Interlocked.Read(ref pending)).IsEqualTo(1L);
            await Assert.That(idle.IsCompleted).IsFalse();
            dispatcher.FinishRecovery(committed: true);
        }

        await Assert.That(Interlocked.Read(ref pending)).IsEqualTo(0L);
        await Assert.That(Interlocked.Read(ref poisoned)).IsEqualTo(1L);
        await idle.WaitAsync(TimeSpan.FromSeconds(30));
        await Assert.That(await store.ReadPendingAsync(new(null, DateTimeOffset.UtcNow), rows, default)).IsEqualTo(0);
    }

    [Test]
    public async Task Recovery_transaction_settles_sent_rows_only_after_it_commits()
    {
        await using var fixture = new PostgreSqlFixture();
        await fixture.StartAsync();
        await using var source = NpgsqlDataSource.Create(fixture.ConnectionString);
        await PostgreSqlOutboxStoreTests.CreateSchema(source);
        var options = new PostgreSqlOutboxOptions();
        await PostgreSqlOutboxStoreTests.Insert(source, options, Guid.CreateVersion7(), DateTimeOffset.UtcNow);

        await using (var unordered = source.CreateCommand("UPDATE relayer_outbox SET ordering_key = NULL"))
        {
            await unordered.ExecuteNonQueryAsync();
        }

        using var instrumentation = new RelayerInstrumentation();
        var harness = new RelayerHarness();
        instrumentation.Observer = harness;
        // Stands for the commit that wrote the row in this provider.
        instrumentation.AddOutboxPending(1);
        var idle = harness.WaitForIdleAsync();
        var store = new PostgreSqlOutboxStore(source, options);
        using var dispatcher = new PostgreSqlDirectDispatcher(source, options, new(), [new SucceedingTransport()],
            NullLogger<PostgreSqlDirectDispatcher>.Instance, store, instrumentation);
        var rows = new OutboxRecord[1];

        foreach (var commit in new[] { false, true })
        {
            await store.ReadPendingAsync(new(null, DateTimeOffset.UtcNow), rows, default);
            await using var connection = await source.OpenConnectionAsync();
            await using var transaction = await connection.BeginTransactionAsync();
            store.UseDispatchTransaction(connection, transaction);
            await dispatcher.DispatchRecoveredAsync(rows, default, propagateDatabaseFailures: true);
            store.UseDispatchTransaction(null, null);
            await Assert.That(idle.IsCompleted).IsFalse();

            if (!commit)
            {
                // The deletion rolls back, so the row is still outstanding.
                await transaction.RollbackAsync();
                dispatcher.FinishRecovery(committed: false);
                // A later pass that commits without dispatching must not report the rolled-back rows.
                dispatcher.FinishRecovery(committed: true);
                await Assert.That(idle.IsCompleted).IsFalse();
                continue;
            }

            await transaction.CommitAsync();
            dispatcher.FinishRecovery(committed: true);
        }

        await idle.WaitAsync(TimeSpan.FromSeconds(30));
        await Assert.That(await store.ReadPendingAsync(new(null, DateTimeOffset.UtcNow), rows, default)).IsEqualTo(0);
    }

    [Test]
    public async Task Filtered_redrive_bounds_scanned_rows_and_requires_a_larger_prefix_for_later_matches()
    {
        await using var fixture = new PostgreSqlFixture();
        await fixture.StartAsync();
        await using var source = NpgsqlDataSource.Create(fixture.ConnectionString);
        await PostgreSqlOutboxStoreTests.CreateSchema(source);
        var options = new PostgreSqlOutboxOptions { MaxDispatchAttempts = 1 };
        var first = Guid.Parse("00000000-0000-0000-0000-000000000001");
        var second = Guid.Parse("00000000-0000-0000-0000-000000000002");
        await MakeDead(source, options, first);
        await MakeDead(source, options, second);
        var manager = new PostgreSqlDeadLetterManager("store", source, options);
        var scanned = 0;

        bool Match(DeadLetterMessage message)
        {
            scanned++;
            return message.MessageId == second.ToString("D");
        }

        await Assert.That(await manager.RedriveAsync("store", Match, 1)).IsEqualTo(0);
        await Assert.That(await manager.RedriveAsync("store", Match, 1)).IsEqualTo(0);
        await Assert.That(scanned).IsEqualTo(2);
        var page = await manager.PeekAsync("store", 1);
        await Assert.That((await manager.PeekAsync("store", 1, page.NextCursor)).Messages[0].MessageId).IsEqualTo(second.ToString("D"));
        await Assert.That(await manager.RedriveAsync("store", Match, 2)).IsEqualTo(1);
        await Assert.That((await manager.PeekAsync("store")).Messages[0].MessageId).IsEqualTo(first.ToString("D"));
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task Redrive_makes_rows_outstanding_again_and_purge_removes_parked_rows_from_pending(bool ordered)
    {
        await using var fixture = new PostgreSqlFixture();
        await fixture.StartAsync();
        await using var source = NpgsqlDataSource.Create(fixture.ConnectionString);
        await PostgreSqlOutboxStoreTests.CreateSchema(source);
        var options = new PostgreSqlOutboxOptions { MaxDispatchAttempts = 1 };
        var redriven = Guid.Parse("00000000-0000-0000-0000-000000000001");
        var purged = Guid.Parse("00000000-0000-0000-0000-000000000002");
        int? lane = ordered ? 0 : null;
        await MakeDead(source, options, redriven, lane);
        using var instrumentation = new RelayerInstrumentation();
        long pending = 0;
        using var listener = new MeterListener();
        listener.InstrumentPublished = (instrument, meterListener) =>
        {
            if (instrument == instrumentation.OutboxPending)
            {
                meterListener.EnableMeasurementEvents(instrument);
            }
        };
        listener.SetMeasurementEventCallback<long>((_, value, _, _) => Interlocked.Add(ref pending, value));
        listener.Start();
        var harness = new RelayerHarness();
        instrumentation.Observer = harness;
        var manager = new PostgreSqlDeadLetterManager("store", source, options, instrumentation);
        await Assert.That(await manager.RedriveAsync("store")).IsEqualTo(1);
        // An unordered row re-enters the table; a parked row never left it.
        await Assert.That(Interlocked.Read(ref pending)).IsEqualTo(ordered ? 0L : 1L);
        var idle = harness.WaitForIdleAsync();
        await Assert.That(idle.IsCompleted).IsFalse();
        // Stands for the dispatcher settling the redriven row.
        instrumentation.AddOutboxPending(-1);
        await idle.WaitAsync(TimeSpan.FromSeconds(30));

        await using (var sent = source.CreateCommand("DELETE FROM relayer_outbox"))
        {
            await sent.ExecuteNonQueryAsync();
        }

        Interlocked.Exchange(ref pending, 0);
        await MakeDead(source, options, purged, lane);
        await Assert.That(await manager.PurgeAsync("store")).IsEqualTo(1);
        await Assert.That(Interlocked.Read(ref pending)).IsEqualTo(ordered ? -1L : 0L);
        await Assert.That(harness.WaitForIdleAsync().IsCompleted).IsTrue();
    }

    [Test]
    public async Task Missing_ordered_barrier_reports_identity_and_rolls_back_earlier_redrives()
    {
        await using var fixture = new PostgreSqlFixture();
        await fixture.StartAsync();
        await using var source = NpgsqlDataSource.Create(fixture.ConnectionString);
        await PostgreSqlOutboxStoreTests.CreateSchema(source);
        var options = new PostgreSqlOutboxOptions { MaxDispatchAttempts = 1 };
        var first = Guid.Parse("00000000-0000-0000-0000-000000000001");
        var broken = Guid.Parse("00000000-0000-0000-0000-000000000002");
        await MakeDead(source, options, first);
        await MakeDead(source, options, broken, 0);

        await using (var command = source.CreateCommand("DELETE FROM relayer_outbox WHERE id = @id"))
        {
            command.Parameters.AddWithValue("id", broken);
            await command.ExecuteNonQueryAsync();
        }

        var manager = new PostgreSqlDeadLetterManager("store", source, options);

        try
        {
            await manager.RedriveAsync("store");
            throw new Exception("Expected inconsistent barrier to fail redrive.");
        }
        catch (InvalidOperationException exception)
        {
            await Assert.That(exception.Message).Contains(broken.ToString("D"));
            await Assert.That(exception.Message).Contains("rolled back");
        }

        await Assert.That((await manager.PeekAsync("store")).Messages.Count).IsEqualTo(2);
        var rows = new OutboxRecord[2];
        await Assert.That(await new PostgreSqlOutboxStore(source, options).ReadPendingAsync(new(null, DateTimeOffset.UtcNow), rows, default)).IsEqualTo(0);
        // Explicitly discard the damaged prefix; purge does not need to invent a sequence.
        await Assert.That(await manager.PurgeAsync("store", 2)).IsEqualTo(2);
        await Assert.That((await manager.PeekAsync("store")).Messages.Count).IsEqualTo(0);
    }

    private static async Task MakeDead(NpgsqlDataSource source, PostgreSqlOutboxOptions options, Guid id, int? lane = null)
    {
        var now = DateTimeOffset.UtcNow;
        await PostgreSqlOutboxStoreTests.Insert(source, options, id, now, lane: lane);
        var store = new PostgreSqlOutboxStore(source, options);
        var claim = new OutboxClaim(id, Guid.NewGuid());
        await Assert.That(await store.TryClaimAsync(claim, now, now.AddMinutes(1), default)).IsTrue();
        await store.RecordDispatchFailureAsync(claim, "broker failed", now, default);
    }

    private sealed class SucceedingTransport : ITransport
    {
        public string Name => "events";

        public TransportCapabilities GetCapabilities(string endpoint)
        {
            return new();
        }

        public ValueTask SendAsync(OutgoingMessage message, CancellationToken cancellationToken)
        {
            return ValueTask.CompletedTask;
        }

        public ValueTask SendBatchAsync(ReadOnlyMemory<OutgoingMessage> messages, CancellationToken cancellationToken)
        {
            return ValueTask.CompletedTask;
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

    private sealed class FailingTransport : ITransport
    {
        public string Name => "events";

        public TransportCapabilities GetCapabilities(string endpoint)
        {
            return new();
        }

        public ValueTask SendAsync(OutgoingMessage message, CancellationToken cancellationToken)
        {
            throw new PostgreSqlLocalFailure(new InvalidOperationException("broker failed"), FailureAction.DeadLetter);
        }

        public ValueTask SendBatchAsync(ReadOnlyMemory<OutgoingMessage> messages, CancellationToken cancellationToken)
        {
            throw new PostgreSqlLocalFailure(new InvalidOperationException("broker failed"), FailureAction.DeadLetter);
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
