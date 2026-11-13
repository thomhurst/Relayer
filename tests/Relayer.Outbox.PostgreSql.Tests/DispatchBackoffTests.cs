using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.DependencyInjection;
using Npgsql;
using Relayer.Transports;

namespace Relayer.Outbox.PostgreSql.Tests;

public class DispatchBackoffTests
{
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task Outage_exceeding_poison_budget_persists_backoff_and_recovers_after_restart(bool ordered)
    {
        await using var fixture = new PostgreSqlFixture();
        await fixture.StartAsync();
        await using var source = NpgsqlDataSource.Create(fixture.ConnectionString);
        await PostgreSqlOutboxStoreTests.CreateSchema(source);
        var options = new PostgreSqlOutboxOptions { Lanes = 1, MaxDispatchAttempts = 1 };
        var id = Guid.CreateVersion7();
        await PostgreSqlOutboxStoreTests.Insert(source, options, id, DateTimeOffset.UtcNow, lane: ordered ? 0 : null);
        if (!ordered)
        {
            await using var unordered = source.CreateCommand("UPDATE relayer_outbox SET ordering_key = NULL");
            await unordered.ExecuteNonQueryAsync();
        }
        var transport = new OutageTransport();
        var rows = new OutboxRecord[1];
        for (var attempt = 0; attempt < 12; attempt++)
        {
            // Advance database eligibility rather than sleeping. Each attempt uses a fresh worker,
            // proving that retries and ordered head barriers survive worker/process replacement.
            await using (var ready = source.CreateCommand("UPDATE relayer_outbox SET deliver_at = clock_timestamp(); UPDATE relayer_leases SET expires_at = clock_timestamp() - interval '1 second'"))
                await ready.ExecuteNonQueryAsync();
            await using var clock = source.CreateCommand("SELECT clock_timestamp()");
            var before = (DateTime)(await clock.ExecuteScalarAsync())!;
            await Dispatch();
            await using var state = source.CreateCommand("SELECT attempts, status, deliver_at, clock_timestamp(), claim_token IS NULL, (SELECT count(*) FROM relayer_outbox_dead) FROM relayer_outbox");
            await using var reader = await state.ExecuteReaderAsync();
            await Assert.That(await reader.ReadAsync()).IsTrue();
            await Assert.That(reader.GetInt32(0)).IsEqualTo(attempt + 1);
            await Assert.That(reader.GetString(1)).IsEqualTo("pending");
            var due = reader.GetDateTime(2);
            var after = reader.GetDateTime(3);
            var expected = Math.Min(300, Math.Pow(2, Math.Min(attempt, 9)));
            await Assert.That(due >= before.AddSeconds(expected) && due <= after.AddSeconds(expected)).IsTrue();
            await Assert.That(reader.GetBoolean(4)).IsTrue();
            await Assert.That(reader.GetInt64(5)).IsEqualTo(0L);
            await reader.DisposeAsync();
            // Test due-time fencing independently of test-runner scheduling pauses.
            await using (var future = source.CreateCommand("UPDATE relayer_outbox SET deliver_at = clock_timestamp() + interval '1 day'"))
                await future.ExecuteNonQueryAsync();
            await Dispatch();
            await Assert.That(transport.Attempts).IsEqualTo(attempt + 1);
        }
        transport.Failing = false;
        await using (var ready = source.CreateCommand("UPDATE relayer_outbox SET deliver_at = clock_timestamp(); UPDATE relayer_leases SET expires_at = clock_timestamp() - interval '1 second'"))
            await ready.ExecuteNonQueryAsync();
        await Dispatch();
        await using var remaining = source.CreateCommand("SELECT count(*) FROM relayer_outbox");
        await Assert.That((long)(await remaining.ExecuteScalarAsync())!).IsEqualTo(0L);
        await Assert.That(transport.Attempts).IsEqualTo(13);

        async Task Dispatch()
        {
            if (ordered)
            {
                using var worker = new PostgreSqlOrderedDispatcher(source, new() { Lanes = 1, MaxDispatchAttempts = 1 }, [transport], NullLogger<PostgreSqlOrderedDispatcher>.Instance);
                await worker.DispatchLaneAsync(0, default);
            }
            else
            {
                var store = new PostgreSqlOutboxStore(source, options);
                var count = await store.ReadPendingAsync(new(null, DateTimeOffset.UtcNow), rows, default);
                using var worker = new PostgreSqlDirectDispatcher(source, options, new(), [transport], NullLogger<PostgreSqlDirectDispatcher>.Instance);
                await worker.DispatchRecoveredAsync(rows.AsMemory(0, count), default);
            }
        }
    }

    [Test]
    public async Task Retry_is_fenced_rolls_back_and_saturates_without_poisoning()
    {
        await using var fixture = new PostgreSqlFixture();
        await fixture.StartAsync();
        await using var source = NpgsqlDataSource.Create(fixture.ConnectionString);
        await PostgreSqlOutboxStoreTests.CreateSchema(source);
        var options = new PostgreSqlOutboxOptions { MaxDispatchAttempts = 1 };
        var id = Guid.CreateVersion7();
        await PostgreSqlOutboxStoreTests.Insert(source, options, id, DateTimeOffset.UtcNow);
        await using (var seed = source.CreateCommand("UPDATE relayer_outbox SET attempts = 30"))
            await seed.ExecuteNonQueryAsync();
        var store = new PostgreSqlOutboxStore(source, options);
        var claim = new OutboxClaim(id, Guid.NewGuid());
        var now = DateTimeOffset.UtcNow;
        await Assert.That(await store.TryClaimAsync(claim, now, now.AddMinutes(1), default)).IsTrue();
        await Assert.That(await store.RecordDispatchFailureAsync(new(id, Guid.NewGuid()), "stale", now, default, retryable: true)).IsEqualTo(0);
        await Assert.That(await store.RecordDispatchFailureAsync(claim, "expired", now.AddMinutes(2), default, retryable: true)).IsEqualTo(0);
        await using (var connection = await source.OpenConnectionAsync())
        await using (var transaction = await connection.BeginTransactionAsync())
        {
            store.UseDispatchTransaction(connection, transaction);
            await Assert.That(await store.RecordDispatchFailureAsync(claim, "rollback", now, default, retryable: true)).IsEqualTo(1);
            await transaction.RollbackAsync();
            store.UseDispatchTransaction(null, null);
        }
        await Assert.That(await store.RecordDispatchFailureAsync(claim, "retry", now, default, retryable: true)).IsEqualTo(1);
        await Assert.That(await store.RecordDispatchFailureAsync(claim, "replay", now, default, retryable: true)).IsEqualTo(0);
        var rows = new OutboxRecord[1];
        await Assert.That(await store.ReadPendingAsync(new(null, now.AddSeconds(299)), rows, default)).IsEqualTo(0);
        await Assert.That(await store.ReadPendingAsync(new(null, now.AddSeconds(301)), rows, default)).IsEqualTo(1);
        await Assert.That(rows[0].Attempts).IsEqualTo(30);
        await Assert.That(rows[0].LastError).IsEqualTo("retry");
    }

    [Test]
    [Arguments(false, FailureAction.DeadLetter)]
    [Arguments(true, FailureAction.DeadLetter)]
    [Arguments(false, FailureAction.Discard)]
    [Arguments(true, FailureAction.Discard)]
    [Arguments(false, FailureAction.Retry)]
    [Arguments(true, FailureAction.Retry)]
    public async Task Native_failures_use_scoped_classifier_and_fenced_outcome(bool ordered, FailureAction action)
    {
        await using var fixture = new PostgreSqlFixture();
        await fixture.StartAsync();
        await using var source = NpgsqlDataSource.Create(fixture.ConnectionString);
        await PostgreSqlOutboxStoreTests.CreateSchema(source);
        var options = new PostgreSqlOutboxOptions { Lanes = 1, MaxDispatchAttempts = 1 };
        var id = Guid.CreateVersion7();
        await PostgreSqlOutboxStoreTests.Insert(source, options, id, DateTimeOffset.UtcNow, lane: ordered ? 0 : null);
        await using (var setup = source.CreateCommand("UPDATE relayer_outbox SET message_type = 'orders;v=3', ordering_key = CASE WHEN lane IS NULL THEN NULL ELSE ordering_key END"))
            await setup.ExecuteNonQueryAsync();
        var observation = new ClassificationObservation();
        var transport = new OutageTransport();
        var services = new ServiceCollection().AddLogging().AddSingleton<ITransport>(transport)
            .AddScoped<IFailureClassifier>(scope => new DispatchClassifier(observation, action, scope));
        if (ordered) services.AddRelayerPostgreSqlOrderedDispatch("classified", source, new PostgreSqlOrderedDispatchOptions { Lanes = 1, MaxDispatchAttempts = 1 });
        else services.AddRelayerPostgreSqlDirectDispatch("classified", source, options);
        await using var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true });
        if (ordered)
        {
            var worker = provider.GetRequiredKeyedService<PostgreSqlOrderedDispatcher>("classified");
            await worker.DispatchLaneAsync(0, default);
        }
        else
        {
            var store = new PostgreSqlOutboxStore(source, options);
            var rows = new OutboxRecord[1];
            var count = await store.ReadPendingAsync(new(null, DateTimeOffset.UtcNow), rows, default);
            var worker = provider.GetRequiredKeyedService<PostgreSqlDirectDispatcher>("classified");
            await worker.DispatchRecoveredAsync(rows.AsMemory(0, count), default);
        }
        await Assert.That(observation.Id).IsEqualTo(id.ToString());
        await Assert.That(observation.Type).IsEqualTo(new MessageType("orders", 3));
        await Assert.That(observation.Attempt).IsEqualTo(1);
        await Assert.That(observation.ScopeMatches).IsTrue();
        await Assert.That(observation.Disposed).IsTrue();
        await using var state = source.CreateCommand("SELECT (SELECT count(*) FROM relayer_outbox), (SELECT count(*) FROM relayer_outbox_dead), (SELECT status FROM relayer_outbox LIMIT 1)");
        await using var reader = await state.ExecuteReaderAsync();
        await reader.ReadAsync();
        await Assert.That(reader.GetInt64(0)).IsEqualTo(action == FailureAction.Retry || (ordered && action == FailureAction.DeadLetter) ? 1L : 0L);
        await Assert.That(reader.GetInt64(1)).IsEqualTo(action == FailureAction.DeadLetter ? 1L : 0L);
        if (action == FailureAction.Retry || (ordered && action == FailureAction.DeadLetter))
            await Assert.That(reader.GetString(2)).IsEqualTo(action == FailureAction.Retry ? "pending" : "parked");
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task Failed_native_batch_isolates_rejected_row_and_completes_healthy_row(bool partiallySent)
    {
        await using var fixture = new PostgreSqlFixture();
        await fixture.StartAsync();
        await using var source = NpgsqlDataSource.Create(fixture.ConnectionString);
        await PostgreSqlOutboxStoreTests.CreateSchema(source);
        var rejected = Guid.CreateVersion7();
        var healthy = Guid.CreateVersion7();
        await PostgreSqlOutboxStoreTests.Insert(source, new(), rejected, DateTimeOffset.UtcNow);
        await PostgreSqlOutboxStoreTests.Insert(source, new(), healthy, DateTimeOffset.UtcNow);
        await using (var setup = source.CreateCommand("UPDATE relayer_outbox SET ordering_key = NULL, body = CASE WHEN id = @bad THEN decode('00', 'hex') ELSE decode('01', 'hex') END"))
        {
            setup.Parameters.AddWithValue("bad", rejected);
            await setup.ExecuteNonQueryAsync();
        }
        await using var provider = new ServiceCollection().AddSingleton<IFailureClassifier>(new FailureClassifierBuilder()
            .On<RejectedSendException>(FailureAction.DeadLetter).Build()).BuildServiceProvider();
        var transport = new MixedBatchTransport(partiallySent);
        var store = new PostgreSqlOutboxStore(source);
        var rows = new OutboxRecord[2];
        var count = await store.ReadPendingAsync(new(null, DateTimeOffset.UtcNow), rows, default);
        using var worker = new PostgreSqlDirectDispatcher(source, new(), new() { BatchSize = 2 }, [transport],
            NullLogger<PostgreSqlDirectDispatcher>.Instance, scopes: provider.GetRequiredService<IServiceScopeFactory>());
        await worker.DispatchRecoveredAsync(rows.AsMemory(0, count), default);
        await Assert.That(transport.Batches).IsEqualTo(1);
        await Assert.That(transport.Singles).IsEqualTo(2);
        await Assert.That(transport.HealthySends).IsEqualTo(partiallySent ? 2 : 1);
        await using var state = source.CreateCommand("SELECT (SELECT count(*) FROM relayer_outbox), (SELECT id FROM relayer_outbox_dead)");
        await using var reader = await state.ExecuteReaderAsync();
        await reader.ReadAsync();
        await Assert.That(reader.GetInt64(0)).IsEqualTo(0L);
        await Assert.That(reader.GetGuid(1)).IsEqualTo(rejected);
    }

    [Test]
    public async Task Batch_outage_stops_isolation_after_first_retry_and_backs_off_every_row()
    {
        await using var fixture = new PostgreSqlFixture();
        await fixture.StartAsync();
        await using var source = NpgsqlDataSource.Create(fixture.ConnectionString);
        await PostgreSqlOutboxStoreTests.CreateSchema(source);
        for (var i = 0; i < 4; i++)
            await PostgreSqlOutboxStoreTests.Insert(source, new(), Guid.CreateVersion7(), DateTimeOffset.UtcNow);
        await using (var setup = source.CreateCommand("UPDATE relayer_outbox SET ordering_key = NULL"))
            await setup.ExecuteNonQueryAsync();
        var transport = new OutageTransport();
        var store = new PostgreSqlOutboxStore(source);
        var rows = new OutboxRecord[4];
        var count = await store.ReadPendingAsync(new(null, DateTimeOffset.UtcNow), rows, default);
        await Assert.That(count).IsEqualTo(4);
        using var worker = new PostgreSqlDirectDispatcher(source, new(), new() { BatchSize = 4 }, [transport],
            NullLogger<PostgreSqlDirectDispatcher>.Instance);
        var before = DateTimeOffset.UtcNow;
        await worker.DispatchRecoveredAsync(rows.AsMemory(0, count), default);
        await Assert.That(transport.Attempts).IsEqualTo(2); // One batch and one single native send.
        await using var state = source.CreateCommand("SELECT count(*), bool_and(attempts = 1 AND status = 'pending' AND deliver_at >= @before + interval '1 second' AND claim_token IS NULL), (SELECT count(*) FROM relayer_outbox_dead) FROM relayer_outbox");
        state.Parameters.AddWithValue("before", before);
        await using var reader = await state.ExecuteReaderAsync();
        await reader.ReadAsync();
        await Assert.That(reader.GetInt64(0)).IsEqualTo(4L);
        await Assert.That(reader.GetBoolean(1)).IsTrue();
        await Assert.That(reader.GetInt64(2)).IsEqualTo(0L);
    }

    private sealed class ClassificationObservation
    {
        internal string? Id;
        internal MessageType Type;
        internal int Attempt;
        internal bool ScopeMatches;
        internal bool Disposed;
    }

    private sealed class DispatchClassifier(ClassificationObservation observation, FailureAction action, IServiceProvider services)
        : IFailureClassifier, IDisposable
    {
        public FailureAction Classify(Exception exception, MessageContext context)
        {
            observation.Id = context.MessageId;
            observation.Type = context.MessageType;
            observation.Attempt = context.Attempt;
            observation.ScopeMatches = ReferenceEquals(context.Services, services);
            return action;
        }
        public void Dispose() => observation.Disposed = true;
    }

    private sealed class RejectedSendException : Exception;

    private sealed class MixedBatchTransport(bool partiallySent) : ITransport
    {
        internal int Batches;
        internal int Singles;
        internal int HealthySends;
        public string Name => "events";
        public TransportCapabilities GetCapabilities(string endpoint) => new() { MaxBatchSize = 2 };
        public ValueTask SendAsync(OutgoingMessage message, CancellationToken cancellationToken)
        {
            Singles++;
            if (message.Body.Span[0] == 0) throw new RejectedSendException();
            HealthySends++;
            return ValueTask.CompletedTask;
        }
        public ValueTask SendBatchAsync(ReadOnlyMemory<OutgoingMessage> messages, CancellationToken cancellationToken)
        {
            Batches++;
            if (partiallySent) HealthySends++;
            throw new RejectedSendException();
        }
        public ValueTask<TopologyReport> ApplyTopologyAsync(TopologyMode mode, CancellationToken cancellationToken) => new(new TopologyReport([]));
        public Task StartAsync(CancellationToken cancellationToken) => Task.CompletedTask;
        public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    private sealed class OutageTransport : ITransport
    {
        public bool Failing { get; set; } = true;
        public int Attempts { get; private set; }
        public string Name => "events";
        public TransportCapabilities GetCapabilities(string endpoint) => new() { OrderedByKey = true, MaxBatchSize = 4 };
        public ValueTask SendAsync(OutgoingMessage message, CancellationToken cancellationToken)
        {
            Attempts++;
            if (Failing) throw new IOException("broker unavailable");
            return ValueTask.CompletedTask;
        }
        public ValueTask SendBatchAsync(ReadOnlyMemory<OutgoingMessage> messages, CancellationToken cancellationToken) => SendAsync(messages.Span[0], cancellationToken);
        public ValueTask<TopologyReport> ApplyTopologyAsync(TopologyMode mode, CancellationToken cancellationToken) => new(new TopologyReport([]));
        public Task StartAsync(CancellationToken cancellationToken) => Task.CompletedTask;
        public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }
}
