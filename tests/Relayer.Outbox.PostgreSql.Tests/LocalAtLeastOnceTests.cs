using System.Collections.Concurrent;
using System.Diagnostics.Metrics;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using Relayer.Transports;
using static Relayer.Outbox.PostgreSql.Tests.LocalTransportTests;

namespace Relayer.Outbox.PostgreSql.Tests;

[ClassDataSource<PostgreSqlLocalSessionFixture>(Shared = SharedType.PerTestSession)]
public class LocalAtLeastOnceTests(PostgreSqlLocalSessionFixture server)
{
    [Test]
    public async Task Hosted_direct_dispatch_settles_an_independent_commit_while_the_first_handler_is_blocked()
    {
        await using var database = server.CreateDatabase();
        await database.StartAsync();
        // The blocked handler owns no connection. The second claim, handler and completion can all use this pool.
        await using var source = NpgsqlDataSource.Create(database.ConnectionString + ";Maximum Pool Size=1;Timeout=5");
        await PostgreSqlOutboxStoreTests.CreateSchema(source);

        await using (var command = source.CreateCommand("CREATE TABLE local_business (value integer PRIMARY KEY)"))
        {
            await command.ExecuteNonQueryAsync();
        }

        await using var provider = Provider(source, directHandoff: true, concurrency: 2, consumerOptions: options =>
        options.Completion = PostgreSqlLocalCompletion.AtLeastOnce);
        var services = provider.GetServices<IHostedService>().ToArray();
        var probe = provider.GetRequiredService<Probe>();
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        probe.BeforeObserve = async (value, token) =>
        {
            if (value != 1)
            {
                return;
            }

            entered.TrySetResult();
            await release.Task.WaitAsync(token);
        };
        using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(15));

        foreach (var service in services)
        {
            await service.StartAsync(deadline.Token);
        }

        try
        {
            await Enqueue(provider, source, new(1), true);
            await entered.Task.WaitAsync(deadline.Token);
            await Enqueue(provider, source, new(2), true);
            // A committed delete is observable independently of handler entry or return.
            while (await Count(source, "relayer_outbox") != 1)
            {
                await Task.Delay(10, deadline.Token);
            }

            await Assert.That(await Count(source, "local_business WHERE value = 2")).IsEqualTo(1L);
            await Assert.That(await Count(source, "local_business WHERE value = 1")).IsEqualTo(0L);
        }
        finally
        {
            release.TrySetResult();

            for (var index = services.Length - 1; index >= 0; index--)
            {
                await services[index].StopAsync(default);
            }
        }

        await Assert.That(await Count(source, "relayer_outbox")).IsEqualTo(0L);
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task Saturated_direct_destination_resumes_when_one_slot_opens(bool competingFailure)
    {
        await using var database = server.CreateDatabase();
        await database.StartAsync();
        await using var source = NpgsqlDataSource.Create(database.ConnectionString);
        await PostgreSqlOutboxStoreTests.CreateSchema(source);
        var options = new PostgreSqlOutboxOptions();
        await using var transport = new PostgreSqlLocalTransport("local", source, options, ["work", "work-other"]);
        using var worker = new PostgreSqlDirectDispatcher(source, options, new(), [transport],
            NullLogger<PostgreSqlDirectDispatcher>.Instance);
        transport.Dispatcher = worker;

        var firstEntered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var secondEntered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseFirst = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseSecond = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var resumed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var otherSettled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var finalBarrier = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var retriedAttempt = 0;
        var expectedValue = competingFailure ? 5 : 3;
        transport.Register("work", async (delivery, token) =>
        {
            var value = delivery.Message.Body.Span[0];

            if (value == 1)
            {
                firstEntered.SetResult();
                await releaseFirst.Task.WaitAsync(token);
            }
            else if (value == 2)
            {
                secondEntered.SetResult();
                await releaseSecond.Task.WaitAsync(token);
            }

            if (competingFailure && value == 3)
            {
                retriedAttempt = delivery.Attempt;
            }

            await transport.SettleAsync(delivery, Disposition.Complete, token);

            if (value == expectedValue)
            {
                resumed.SetResult();
            }

            return Disposition.SettledByHandler;
        }, concurrency: 2, atLeastOnce: true);
        transport.Register("work-other", async (delivery, token) =>
        {
            await transport.SettleAsync(delivery, Disposition.Complete, token);
            (delivery.Message.Body.Span[0] == 4 ? otherSettled : finalBarrier).SetResult();
            return Disposition.SettledByHandler;
        }, atLeastOnce: true);

        using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(15));
        await worker.StartAsync(deadline.Token);

        try
        {
            await SendAsync("work", 1);
            await firstEntered.Task.WaitAsync(deadline.Token);
            await SendAsync("work", 2);
            await secondEntered.Task.WaitAsync(deadline.Token);
            await SendAsync("work", 3);
            await SendAsync("work-other", 4);
            // This destination is visited after "work", even if both commits are coalesced.
            await otherSettled.Task.WaitAsync(deadline.Token);
            await Assert.That(resumed.Task.IsCompleted).IsFalse();
            await Assert.That(await Count(source, "relayer_outbox WHERE claim_token IS NULL")).IsEqualTo(1L);

            if (competingFailure)
            {
                // Simulate another process failing the parked row with a retry due immediately.
                // Its current attempt must be read by recovery instead of using the retained commit snapshot.
                await using var failure = source.CreateCommand("UPDATE relayer_outbox SET attempts = 1 WHERE claim_token IS NULL");
                await Assert.That(await failure.ExecuteNonQueryAsync(deadline.Token)).IsEqualTo(1);
                await SendAsync("work", 5);
                await SendAsync("work-other", 6);
                await finalBarrier.Task.WaitAsync(deadline.Token);
            }

            releaseFirst.SetResult();
            await resumed.Task.WaitAsync(deadline.Token);
            await Assert.That(await Count(source, "relayer_outbox")).IsEqualTo(competingFailure ? 2L : 1L);
        }
        finally
        {
            releaseFirst.TrySetResult();
            releaseSecond.TrySetResult();
            await worker.StopAsync(default);
        }

        await Assert.That(await Count(source, "relayer_outbox")).IsEqualTo(competingFailure ? 1L : 0L);
        await Assert.That(await Count(source, "relayer_outbox WHERE attempts = 1 AND claim_token IS NULL"))
            .IsEqualTo(competingFailure ? 1L : 0L);

        if (competingFailure)
        {
            using var maintenance = new PostgreSqlMaintenanceWorker(source,
                new()
                {
                    RecoveryDelay = TimeSpan.Zero
                }, [transport], NullLogger<PostgreSqlMaintenanceWorker>.Instance);
            await maintenance.SweepAsync(deadline.Token);
            await Assert.That(retriedAttempt).IsEqualTo(2);
            await Assert.That(await Count(source, "relayer_outbox")).IsEqualTo(0L);
        }

        ValueTask SendAsync(string destination, byte value)
        {
            KeyValuePair<string, WireString>[] headers =
                [new("ce_id", new(Guid.CreateVersion7().ToString())), new("ce_type", new("local.work;v=1"))];
            return transport.SendAsync(new(destination, new byte[] { value }, headers), deadline.Token);
        }
    }

    [Test]
    public async Task Hosted_direct_burst_shares_admission_with_recovery_and_retains_retry_deadlines()
    {
        await using var database = server.CreateDatabase();
        await database.StartAsync();
        await using var source = NpgsqlDataSource.Create(database.ConnectionString);
        await PostgreSqlOutboxStoreTests.CreateSchema(source);

        await using (var command = source.CreateCommand("CREATE TABLE local_business (value integer PRIMARY KEY)"))
        {
            await command.ExecuteNonQueryAsync();
        }

        await using var provider = Provider(source, directHandoff: true, concurrency: 2, maxAttempts: 3,
            failureAction: FailureAction.Retry, consumerOptions: options =>
            {
                options.Completion = PostgreSqlLocalCompletion.AtLeastOnce;
                options.RedeliveryDelays = [TimeSpan.FromHours(1)];
            });
        var services = provider.GetServices<IHostedService>().ToArray();
        var worker = provider.GetRequiredKeyedService<PostgreSqlDirectDispatcher>("local");
        var transport = provider.GetServices<ITransport>().Single();
        using var maintenance = new PostgreSqlMaintenanceWorker(source,
            new()
            {
                RecoveryDelay = TimeSpan.Zero,
                MaxDispatchAttempts = 3
            }, [transport],
            NullLogger<PostgreSqlMaintenanceWorker>.Instance);
        var firstEntered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var retryEntered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var entered = new ConcurrentQueue<int>();
        provider.GetRequiredService<Probe>().BeforeObserve = async (value, token) =>
        {
            entered.Enqueue(value);

            if (value is not (1 or -1))
            {
                return;
            }

            (value == 1 ? firstEntered : retryEntered).TrySetResult();
            await release.Task.WaitAsync(token);
        };

        using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(15));

        foreach (var service in services)
        {
            await service.StartAsync(deadline.Token);
        }

        const int Burst = 16;

        try
        {
            await Enqueue(provider, source, new(1), true);
            await firstEntered.Task.WaitAsync(deadline.Token);
            await Enqueue(provider, source, new(-1), true);
            await retryEntered.Task.WaitAsync(deadline.Token);

            for (var value = 2; value < Burst + 2; value++)
            {
                await Enqueue(provider, source, new(value), true);
            }

            // Both direct attempts still hold admission. An explicit recovery pass cannot claim the burst either.
            await maintenance.SweepAsync(deadline.Token);
            await Assert.That(entered.ToArray()).IsEquivalentTo([1, -1], TUnit.Assertions.Enums.CollectionOrdering.Matching);
            await Assert.That(await Count(source, "relayer_outbox WHERE claim_token IS NOT NULL")).IsEqualTo(2L);

            var draining = worker.StopAsync(deadline.Token);
            await Assert.That(draining.IsCompleted).IsFalse();
            release.TrySetResult();
            await draining;

            var settled = await Count(source, "local_business WHERE value > 0");
            var recoveredLater = await Count(source, "relayer_outbox WHERE status = 'pending' AND attempts = 0 AND claim_token IS NULL");
            await Assert.That(settled + recoveredLater).IsEqualTo(Burst + 1L);
            await Assert.That(await Count(source, "relayer_outbox WHERE status = 'pending' AND attempts = 1 AND claim_token IS NULL AND deliver_at > clock_timestamp()")).IsEqualTo(1L);
            await Assert.That(await Count(source, "relayer_outbox")).IsEqualTo(recoveredLater + 1L);
        }
        finally
        {
            release.TrySetResult();

            for (var index = services.Length - 1; index >= 0; index--)
            {
                await services[index].StopAsync(default);
            }
        }
    }

    [Test]
    [Arguments("direct")]
    [Arguments("recovered")]
    [Arguments("ordered")]
    public async Task Handler_runs_after_the_claim_commits_and_a_fenced_statement_completes_the_row(string mode)
    {
        await using var database = server.CreateDatabase();
        await using var harness = await Harness.StartAsync(database, mode, maxAttempts: null);

        foreach (var value in new[] { 7, 8, 9 })
        {
            await Enqueue(harness.Provider, harness.Source, new(value), true);
        }

        await harness.DispatchAsync();

        await Assert.That(await Count(harness.Source, "local_business")).IsEqualTo(3L);
        await Assert.That(await Count(harness.Source, "relayer_outbox")).IsEqualTo(0L);
        var observed = harness.Probe.Observations.ToArray();
        await Assert.That(observed.Length).IsEqualTo(3);

        foreach (var observation in observed)
        {
            await Assert.That(observation.TransactionRejected).IsTrue();
            // Another connection sees the committed claim and can lock the row and the lane or sweeper lease at once.
            await Assert.That(observation.ClaimCommittedAndUnlocked).IsTrue();
        }
        // One lane turn handles every row, re-acquiring its lease between detached attempts, in key order.
        if (mode == "ordered")
        {
            await Assert.That(observed.Select(item => item.Value)).IsEquivalentTo([7, 8, 9], TUnit.Assertions.Enums.CollectionOrdering.Matching);
        }
    }

    [Test]
    [Arguments("direct")]
    [Arguments("recovered")]
    [Arguments("ordered")]
    public async Task Retry_and_poison_transitions_run_after_the_handler_without_rolling_back_its_work(string mode)
    {
        await using var database = server.CreateDatabase();
        await using var harness = await Harness.StartAsync(database, mode, maxAttempts: 2);
        await Enqueue(harness.Provider, harness.Source, new(-1), true);

        await harness.DispatchAsync();

        await using (var pending = harness.Source.CreateCommand("SELECT attempts FROM relayer_outbox WHERE status = 'pending'"))
        {
            await Assert.That((int)(await pending.ExecuteScalarAsync())!).IsEqualTo(1);
        }

        await harness.DispatchAsync();

        // The handler's own write committed before it failed; at-least-once does not roll it back.
        await Assert.That(await Count(harness.Source, "local_business")).IsEqualTo(1L);

        if (mode == "ordered")
        {
            await Assert.That(await Count(harness.Source, "relayer_outbox WHERE status = 'parked'")).IsEqualTo(1L);
        }
        else
        {
            await Assert.That(await Count(harness.Source, "relayer_outbox_dead")).IsEqualTo(1L);
            await Assert.That(await Count(harness.Source, "relayer_outbox")).IsEqualTo(0L);
        }
    }

    [Test]
    [Arguments("direct")]
    [Arguments("recovered")]
    [Arguments("ordered")]
    public async Task Discard_completes_the_row_without_rolling_back_the_handlers_work(string mode)
    {
        await using var database = server.CreateDatabase();
        await using var harness = await Harness.StartAsync(database, mode, maxAttempts: null, FailureAction.Discard);
        await Enqueue(harness.Provider, harness.Source, new(-1), true);

        await harness.DispatchAsync();

        // No delivery transaction or savepoint exists, so the handler's committed write stays and the fenced completion deletes the row.
        await Assert.That(await Count(harness.Source, "local_business")).IsEqualTo(1L);
        await Assert.That(await Count(harness.Source, "relayer_outbox")).IsEqualTo(0L);
        await Assert.That(await Count(harness.Source, "relayer_outbox_dead")).IsEqualTo(0L);
    }

    [Test]
    [Arguments("direct")]
    [Arguments("recovered")]
    [Arguments("ordered")]
    public async Task Lost_claim_on_completion_is_reported_and_the_row_stays_for_redelivery(string mode)
    {
        await using var database = server.CreateDatabase();
        await using var harness = await Harness.StartAsync(database, mode, maxAttempts: null);
        // The handler expires its own claim, as a run longer than the claim duration would.
        await Enqueue(harness.Provider, harness.Source, new(-2), true);

        // The ordered lane stops with the failure; direct dispatch logs it and continues.
        var reported = new List<Exception>();

        if (mode == "ordered")
        {
            reported.Add((await Assert.That(harness.DispatchAsync).Throws<InvalidOperationException>())!);
        }
        else
        {
            await harness.DispatchAsync();
            reported.AddRange(harness.Errors);
        }

        await Assert.That(await Count(harness.Source, "local_business")).IsEqualTo(1L);
        await Assert.That(await Count(harness.Source, "relayer_outbox WHERE status = 'pending'")).IsEqualTo(1L);
        var errors = reported.SelectMany(Chain).Select(error => error.Message);
        await Assert.That(errors.Any(message => message.Contains("lost before completion", StringComparison.Ordinal))).IsTrue();
    }

    [Test]
    public async Task Change_capture_traversal_surfaces_database_failures_from_at_least_once_rows()
    {
        await using var database = server.CreateDatabase();
        await using var harness = await Harness.StartAsync(database, "recovered", maxAttempts: null);
        await Enqueue(harness.Provider, harness.Source, new(7), true);
        // The row's own claim fails in the database after the traversal's pass has committed.
        await using (var command = harness.Source.CreateCommand("""
            CREATE FUNCTION reject_claim() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'claim rejected'; END $$;
            CREATE TRIGGER reject_claim BEFORE UPDATE ON relayer_outbox FOR EACH ROW EXECUTE FUNCTION reject_claim();
            """))
        {
            await command.ExecuteNonQueryAsync();
        }

        // CDC checkpoints only after this returns, so a database failure must not be swallowed.
        var error = await Assert.That(harness.DispatchCommittedAsync).Throws<PostgresException>();

        await Assert.That(error!.MessageText).IsEqualTo("claim rejected");
        await Assert.That(await Count(harness.Source, "local_business")).IsEqualTo(0L);
        await Assert.That(await Count(harness.Source, "relayer_outbox WHERE status = 'pending'")).IsEqualTo(1L);
    }

    [Test]
    [Arguments("direct")]
    [Arguments("recovered")]
    [Arguments("ordered")]
    public async Task Completion_database_failure_does_not_consume_an_attempt(string mode)
    {
        await using var database = server.CreateDatabase();
        // Attempts remain, so a misrecorded failure would be a retry UPDATE that the trigger allows.
        await using var harness = await Harness.StartAsync(database, mode, maxAttempts: 3);
        await Enqueue(harness.Provider, harness.Source, new(7), true);
        // Only the completion fails; the claim and failure transitions update the row.
        await using (var command = harness.Source.CreateCommand("""
            CREATE FUNCTION reject_completion() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'completion rejected'; END $$;
            CREATE TRIGGER reject_completion BEFORE DELETE ON relayer_outbox FOR EACH ROW EXECUTE FUNCTION reject_completion();
            """))
        {
            await command.ExecuteNonQueryAsync();
        }

        // Recovery surfaces the database error so CDC does not checkpoint; the ordered lane stops; direct dispatch logs it.
        if (mode == "recovered")
        {
            await Assert.That((await Assert.That(harness.DispatchCommittedAsync).Throws<PostgresException>())!.MessageText).IsEqualTo("completion rejected");
        }
        else if (mode == "ordered")
        {
            await Assert.That(harness.DispatchAsync).Throws<InvalidOperationException>();
        }
        else
        {
            await harness.DispatchAsync();
            await Assert.That(harness.Errors.SelectMany(Chain).OfType<PostgresException>().Any(error => error.MessageText == "completion rejected")).IsTrue();
        }

        await Assert.That(await Count(harness.Source, "local_business")).IsEqualTo(1L);
        await Assert.That(await Count(harness.Source, "relayer_outbox WHERE status = 'pending' AND attempts = 0")).IsEqualTo(1L);
    }

    [Test]
    public async Task Ordered_lane_reports_a_committed_completion_when_the_next_row_fails()
    {
        await using var database = server.CreateDatabase();
        await using var harness = await Harness.StartAsync(database, "ordered", maxAttempts: null);

        foreach (var value in new[] { 7, 8 })
        {
            await Enqueue(harness.Provider, harness.Source, new(value), true);
        }
        // The first row's claim is the only update allowed; claiming the second row fails the turn.
        await using (var command = harness.Source.CreateCommand("""
            CREATE SEQUENCE claim_updates;
            CREATE FUNCTION reject_second_claim() RETURNS trigger LANGUAGE plpgsql AS $$
            BEGIN IF nextval('claim_updates') > 1 THEN RAISE EXCEPTION 'claim rejected'; END IF; RETURN NEW; END $$;
            CREATE TRIGGER reject_second_claim BEFORE UPDATE ON relayer_outbox FOR EACH ROW EXECUTE FUNCTION reject_second_claim();
            """))
        {
            await command.ExecuteNonQueryAsync();
        }

        var instrumentation = harness.Provider.GetRequiredService<RelayerInstrumentation>();
        var pending = new ConcurrentQueue<long>();
        using var listener = new MeterListener();
        listener.InstrumentPublished = (instrument, meters) =>
        {
            if (ReferenceEquals(instrument.Meter.Scope, instrumentation) && instrument.Name == "relayer.outbox.pending")
            {
                meters.EnableMeasurementEvents(instrument);
            }
        };
        listener.SetMeasurementEventCallback<long>((_, value, _, _) => pending.Enqueue(value));
        listener.Start();

        await Assert.That((await Assert.That(harness.DispatchAsync).Throws<PostgresException>())!.MessageText).IsEqualTo("claim rejected");

        // The first row's fenced delete committed, so the gauge drops even though the turn failed.
        await Assert.That(await Count(harness.Source, "relayer_outbox")).IsEqualTo(1L);
        await Assert.That(pending.Sum()).IsEqualTo(-1L);
    }

    [Test]
    public async Task Undefined_completion_mode_fails_startup()
    {
        await using var source = NpgsqlDataSource.Create("Host=unused");
        await using var provider = Provider(source, consumerOptions: options => options.Completion = (PostgreSqlLocalCompletion)7);
        await Assert.That(async () =>
        {
            foreach (var starter in provider.GetServices<IHostedService>())
            {
                if (starter is not PostgreSqlDirectDispatcher)
                {
                    await starter.StartAsync(default);
                }
            }
        }).Throws<ArgumentOutOfRangeException>();
    }

    private static IEnumerable<Exception> Chain(Exception error)
    {
        for (Exception? current = error; current is not null; current = current.InnerException)
        {
            yield return current;
        }
    }

    // Records what an at-least-once handler can see from outside its attempt.
    public sealed class Probe
    {
        internal Func<int, CancellationToken, ValueTask>? BeforeObserve
        {
            get; set;
        }

        internal ConcurrentQueue<(int Value, bool TransactionRejected, bool ClaimCommittedAndUnlocked)> Observations
        {
            get;
        } = new();

        internal async Task ObserveAsync(NpgsqlDataSource source, int value, PostgreSqlLocalDelivery delivery, CancellationToken cancellationToken)
        {
            if (BeforeObserve is { } before)
            {
                await before(value, cancellationToken);
            }

            var rejected = false;

            try
            {
                _ = delivery.Transaction;
            }
            catch (InvalidOperationException error) when (error.Message.Contains("AtLeastOnce", StringComparison.Ordinal))
            {
                rejected = true;
            }

            await using var connection = await source.OpenConnectionAsync(cancellationToken);
            await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
            bool unlocked;

            try
            {
                await using var row = new NpgsqlCommand("SELECT claim_token FROM relayer_outbox WHERE id = @id FOR UPDATE NOWAIT", connection, transaction);
                row.Parameters.AddWithValue("id", delivery.Claim.MessageId);
                var token = await row.ExecuteScalarAsync(cancellationToken);
                await using var lease = new NpgsqlCommand("SELECT name FROM relayer_leases WHERE name LIKE 'lane:%' OR name = 'sweeper' FOR UPDATE NOWAIT", connection, transaction);

                await using (var reader = await lease.ExecuteReaderAsync(cancellationToken))
                {
                    while (await reader.ReadAsync(cancellationToken))
                    {
                    }
                }

                unlocked = token is Guid claimed && claimed == delivery.Claim.Token;
            }
            catch (PostgresException error) when (error.SqlState == PostgresErrorCodes.LockNotAvailable)
            {
                unlocked = false;
            }

            await transaction.RollbackAsync(cancellationToken);
            Observations.Enqueue((value, rejected, unlocked));
        }
    }

    private sealed class Harness : IAsyncDisposable
    {
        private readonly string _mode;
        private readonly ErrorLog<PostgreSqlDirectDispatcher> _directLog = new();
        private readonly ErrorLog<PostgreSqlOrderedDispatcher> _orderedLog = new();
        private PostgreSqlOrderedDispatcher? _ordered;
        private PostgreSqlMaintenanceWorker? _maintenance;

        private Harness(string mode, NpgsqlDataSource source, ServiceProvider provider)
        {
            _mode = mode;
            Source = source;
            Provider = provider;
        }

        internal NpgsqlDataSource Source
        {
            get;
        }

        internal ServiceProvider Provider
        {
            get;
        }

        internal Probe Probe => Provider.GetRequiredService<Probe>();

        internal IEnumerable<Exception> Errors => _directLog.Errors;

        internal static async Task<Harness> StartAsync(PostgreSqlLocalSessionFixture.Database database, string mode, int? maxAttempts,
            FailureAction failureAction = FailureAction.Retry)
        {
            await database.StartAsync();
            // One pooled connection: a recovery pass that kept its connection during the handler could not complete.
            var source = NpgsqlDataSource.Create(mode == "recovered" ? database.ConnectionString + ";Maximum Pool Size=1;Timeout=5" : database.ConnectionString);
            await PostgreSqlOutboxStoreTests.CreateSchema(source);

            await using (var command = source.CreateCommand("CREATE TABLE local_business (value integer PRIMARY KEY)"))
            {
                await command.ExecuteNonQueryAsync();
            }

            var provider = Provider(source, ordered: mode == "ordered", failureAction: failureAction, consumerOptions: options =>
            {
                options.Completion = PostgreSqlLocalCompletion.AtLeastOnce;
                options.MaxAttempts = maxAttempts;
            });

            foreach (var starter in provider.GetServices<IHostedService>())
            {
                if (starter is not PostgreSqlDirectDispatcher)
                {
                    await starter.StartAsync(default);
                }
            }

            return new(mode, source, provider);
        }

        internal async Task DispatchAsync()
        {
            var transport = Provider.GetServices<ITransport>().Single();

            if (_mode == "ordered")
            {
                // One worker keeps its lane lease across attempts.
                _ordered ??= new(Source, new()
                {
                    Lanes = 1,
                    MaxDispatchAttempts = 1
                }, [transport], _orderedLog,
                    instrumentation: Provider.GetRequiredService<RelayerInstrumentation>());
                await _ordered.DispatchLaneAsync(0, default);
                return;
            }

            if (_mode == "recovered")
            {
                // The sweeper's pass commits and returns its connection before at-least-once rows claim and run.
                _maintenance ??= new(Source, new()
                {
                    RecoveryDelay = TimeSpan.Zero,
                    MaxDispatchAttempts = 1
                }, [transport],
                    NullLogger<PostgreSqlMaintenanceWorker>.Instance, _directLog);
                // The pass after the last row wraps the boundary, so the next call starts from the oldest due row.
                while (await _maintenance.SweepAsync(default) > 0)
                {
                }

                return;
            }

            var store = new PostgreSqlOutboxStore(Source, new()
            {
                MaxDispatchAttempts = 1
            });
            var rows = new OutboxRecord[4];
            var count = await store.ReadPendingAsync(new(null, DateTimeOffset.UtcNow), rows, default);
            using var worker = new PostgreSqlDirectDispatcher(Source, new()
            {
                MaxDispatchAttempts = 1
            }, new(), [transport], _directLog);
            await worker.DispatchRecoveredAsync(rows.AsMemory(0, count), default);
        }

        internal async Task DispatchCommittedAsync()
        {
            _maintenance ??= new(Source, new()
            {
                RecoveryDelay = TimeSpan.Zero,
                MaxDispatchAttempts = 1
            }, [Provider.GetServices<ITransport>().Single()],
                NullLogger<PostgreSqlMaintenanceWorker>.Instance, _directLog);
            await _maintenance.DispatchCommittedAsync(default);
        }

        public async ValueTask DisposeAsync()
        {
            _ordered?.Dispose();
            _maintenance?.Dispose();
            await Provider.DisposeAsync();
            await Source.DisposeAsync();
        }
    }

    private sealed class ErrorLog<T> : ILogger<T>
    {
        internal ConcurrentQueue<Exception> Errors
        {
            get;
        } = new();

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull
        {
            return null;
        }

        public bool IsEnabled(LogLevel logLevel)
        {
            return true;
        }

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
        {
            if (exception is not null)
            {
                Errors.Enqueue(exception);
            }
        }
    }
}
