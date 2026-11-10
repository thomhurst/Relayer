using System.Collections.Concurrent;
using System.Diagnostics.Metrics;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using Relayer.Transports;

namespace Relayer.Outbox.PostgreSql.Tests;

[ClassDataSource<PostgreSqlLocalSessionFixture>(Shared = SharedType.PerTestSession)]
public class LocalTransportTests(PostgreSqlLocalSessionFixture server)
{
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task Local_completion_commits_business_work_and_deletion_together(bool recovered)
    {
        await using var database = server.CreateDatabase();
        await database.StartAsync();
        await using var source = NpgsqlDataSource.Create(database.ConnectionString);
        await PostgreSqlOutboxStoreTests.CreateSchema(source);
        await using (var command = source.CreateCommand("CREATE TABLE local_business (value integer PRIMARY KEY)")) await command.ExecuteNonQueryAsync();
        await using var provider = Provider(source);
        var instrumentation = provider.GetRequiredService<RelayerInstrumentation>();
        var measurements = new ConcurrentQueue<(string Name, Dictionary<string, object?> Tags)>();
        using var listener = new MeterListener();
        listener.InstrumentPublished = (instrument, capture) =>
        {
            if (ReferenceEquals(instrument.Meter.Scope, instrumentation)) capture.EnableMeasurementEvents(instrument);
        };
        listener.SetMeasurementEventCallback<long>((instrument, _, tags, _) => measurements.Enqueue((instrument.Name, tags.ToArray().ToDictionary(pair => pair.Key, pair => pair.Value))));
        listener.SetMeasurementEventCallback<double>((instrument, _, tags, _) => measurements.Enqueue((instrument.Name, tags.ToArray().ToDictionary(pair => pair.Key, pair => pair.Value))));
        listener.Start();
        var starters = provider.GetServices<IHostedService>().ToArray();
        // Compose the consumer; drive the same dispatcher deterministically below.
        foreach (var starter in starters) if (starter is not PostgreSqlDirectDispatcher) await starter.StartAsync(default);
        var transport = provider.GetServices<ITransport>().Single();
        await Enqueue(provider, source, new(7), commit: true);
        var store = new PostgreSqlOutboxStore(source);
        var rows = new OutboxRecord[4];
        var count = await store.ReadPendingAsync(new(null, DateTimeOffset.UtcNow), rows, default);
        using var worker = new PostgreSqlDirectDispatcher(source, new() { MaxDispatchAttempts = 1 }, new(), [transport], provider.GetRequiredService<Microsoft.Extensions.Logging.ILogger<PostgreSqlDirectDispatcher>>());
        if (recovered)
        {
            await using var connection = await source.OpenConnectionAsync();
            await using var transaction = await connection.BeginTransactionAsync();
            store.UseDispatchTransaction(connection, transaction);
            using var recovery = new PostgreSqlDirectDispatcher(source, new() { MaxDispatchAttempts = 1 }, new(), [transport], provider.GetRequiredService<Microsoft.Extensions.Logging.ILogger<PostgreSqlDirectDispatcher>>(), store);
            await recovery.DispatchRecoveredAsync(rows.AsMemory(0, count), default, propagateDatabaseFailures: true);
            await Assert.That(await Count(source, "local_business")).IsEqualTo(0L);
            await Assert.That(await Count(source, "relayer_outbox")).IsEqualTo(1L);
            await transaction.CommitAsync();
        }
        else await worker.DispatchRecoveredAsync(rows.AsMemory(0, count), default);
        await Assert.That(await Count(source, "local_business")).IsEqualTo(1L);
        await Assert.That(await Count(source, "relayer_outbox")).IsEqualTo(0L);
        foreach (var metric in new[] { "messaging.client.consumed.messages", "messaging.process.duration", "relayer.critical_time" })
        {
            var measurement = measurements.Single(item => item.Name == metric);
            await Assert.That(measurement.Tags["messaging.system"]).IsEqualTo("postgresql");
            await Assert.That(measurement.Tags["messaging.destination.name"]).IsEqualTo("work");
            await Assert.That(measurement.Tags["messaging.operation.name"]).IsEqualTo("process");
        }
    }

    [Test]
    public async Task Idle_maintenance_stops_without_waiting_for_sweep_interval()
    {
        await using var database = server.CreateDatabase();
        await database.StartAsync();
        await using var source = NpgsqlDataSource.Create(database.ConnectionString);
        await PostgreSqlOutboxStoreTests.CreateSchema(source);
        await using var transport = new PostgreSqlLocalTransport("local", source, new(), ["work"]);
        using var worker = new PostgreSqlMaintenanceWorker(source, new() { SweepInterval = TimeSpan.FromHours(1) },
            [transport], NullLogger<PostgreSqlMaintenanceWorker>.Instance);
        using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        await worker.StartAsync(deadline.Token);
        try
        {
            // Cleanup's committed lease proves the worker passed the stop check and reached its idle boundary.
            while (true)
            {
                await using var command = source.CreateCommand("SELECT count(*) FROM relayer_leases WHERE name = 'cleanup'");
                if ((long)(await command.ExecuteScalarAsync(deadline.Token))! == 1) break;
                await Task.Yield();
            }
            using var stopDeadline = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            await worker.StopAsync(stopDeadline.Token);
            await Assert.That(worker.ExecuteTask!.IsCompletedSuccessfully).IsTrue();
            await Assert.That(await Count(source, "relayer_leases")).IsEqualTo(0L);
        }
        finally { await worker.StopAsync(deadline.Token); }
    }

    [Test]
    public async Task Idle_ordered_dispatch_stops_without_waiting_for_poll_interval()
    {
        await using var database = server.CreateDatabase();
        await database.StartAsync();
        await using var source = NpgsqlDataSource.Create(database.ConnectionString);
        await PostgreSqlOutboxStoreTests.CreateSchema(source);
        await using var transport = new PostgreSqlLocalTransport("local", source, new(), ["work"]);
        var clock = new IdlePollClock();
        using var worker = new PostgreSqlOrderedDispatcher(source,
            new() { Lanes = 1, PollInterval = TimeSpan.FromMinutes(1), LeaseDuration = TimeSpan.FromMinutes(10) },
            [transport], NullLogger<PostgreSqlOrderedDispatcher>.Instance, clock);
        using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        await worker.StartAsync(deadline.Token);
        try
        {
            // The timer never fires: only shutdown cancellation can complete this idle wait.
            await clock.FirstPoll.Task.WaitAsync(deadline.Token);
            using var stopDeadline = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            await worker.StopAsync(stopDeadline.Token);
            await Assert.That(worker.ExecuteTask!.IsCompletedSuccessfully).IsTrue();
            await Assert.That(await Count(source, "relayer_leases")).IsEqualTo(0L);
        }
        finally { await worker.StopAsync(deadline.Token); }
    }

    private sealed class IdlePollClock : TimeProvider
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

    [Test]
    [Arguments(-10, false)]
    [Arguments(-11, false)]
    [Arguments(10, false)]
    [Arguments(-10, true)]
    [Arguments(-11, true)]
    [Arguments(10, true)]
    public async Task Follow_up_sends_require_successful_handler_and_scope_disposal(int value, bool ordered)
    {
        await using var database = server.CreateDatabase();
        await database.StartAsync();
        await using var source = NpgsqlDataSource.Create(database.ConnectionString);
        await PostgreSqlOutboxStoreTests.CreateSchema(source);
        await using (var command = source.CreateCommand("CREATE TABLE local_business (value integer PRIMARY KEY)")) await command.ExecuteNonQueryAsync();
        await using var provider = Provider(source, ordered);
        foreach (var starter in provider.GetServices<IHostedService>()) if (starter is not PostgreSqlDirectDispatcher) await starter.StartAsync(default);
        var transport = provider.GetServices<ITransport>().Single();
        await Enqueue(provider, source, new(value), true);
        await using var original = source.CreateCommand("SELECT id FROM relayer_outbox");
        var parentId = (Guid)(await original.ExecuteScalarAsync())!;
        if (ordered)
        {
            using var worker = new PostgreSqlOrderedDispatcher(source, new() { Lanes = 1, MaxDispatchAttempts = 1 }, [transport], provider.GetRequiredService<ILogger<PostgreSqlOrderedDispatcher>>());
            await worker.DispatchLaneAsync(0, default);
        }
        else
        {
            var rows = new OutboxRecord[1];
            var count = await new PostgreSqlOutboxStore(source).ReadPendingAsync(new(null, DateTimeOffset.UtcNow), rows, default);
            using var worker = new PostgreSqlDirectDispatcher(source, new() { MaxDispatchAttempts = 1 }, new(), [transport], provider.GetRequiredService<ILogger<PostgreSqlDirectDispatcher>>());
            await worker.DispatchRecoveredAsync(rows.AsMemory(0, count), default);
        }
        await Assert.That(await Count(source, "local_business")).IsEqualTo(value > 0 ? 1L : 0L);
        await Assert.That(await Count(source, "relayer_outbox_dead")).IsEqualTo(value > 0 ? 0L : 1L);
        // Failed attempts must not leave child rows, including sends made during scope disposal.
        await using var children = source.CreateCommand("SELECT count(*) FROM relayer_outbox WHERE id <> @parent");
        children.Parameters.AddWithValue("parent", parentId);
        await Assert.That((long)(await children.ExecuteScalarAsync())!).IsEqualTo(value > 0 ? 1L : 0L);
    }

    [Test]
    public async Task Enqueue_rollback_never_invokes_the_consumer()
    {
        await using var database = server.CreateDatabase();
        await database.StartAsync();
        await using var source = NpgsqlDataSource.Create(database.ConnectionString);
        await PostgreSqlOutboxStoreTests.CreateSchema(source);
        await using var provider = Provider(source);
        await Enqueue(provider, source, new(1), commit: false);
        await Assert.That(await Count(source, "relayer_outbox")).IsEqualTo(0L);
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task Failed_business_changes_roll_back_and_poison_does_not_block_other_keys(bool ordered)
    {
        await using var database = server.CreateDatabase();
        await database.StartAsync();
        await using var source = NpgsqlDataSource.Create(database.ConnectionString);
        await PostgreSqlOutboxStoreTests.CreateSchema(source);
        await using (var command = source.CreateCommand("CREATE TABLE local_business (value integer PRIMARY KEY)")) await command.ExecuteNonQueryAsync();
        await using var provider = Provider(source, ordered);
        foreach (var starter in provider.GetServices<IHostedService>()) if (starter is not PostgreSqlDirectDispatcher) await starter.StartAsync(default);
        var transport = provider.GetServices<ITransport>().Single();
        await Enqueue(provider, source, new(-1), true);
        await Enqueue(provider, source, new(9), true);
        if (ordered)
        {
            using var worker = new PostgreSqlOrderedDispatcher(source, new() { Lanes = 1, MaxDispatchAttempts = 1 }, [transport], provider.GetRequiredService<Microsoft.Extensions.Logging.ILogger<PostgreSqlOrderedDispatcher>>());
            await worker.DispatchLaneAsync(0, default);
            await worker.DispatchLaneAsync(0, default);
        }
        else
        {
            var store = new PostgreSqlOutboxStore(source, new() { MaxDispatchAttempts = 1 });
            var rows = new OutboxRecord[4];
            var count = await store.ReadPendingAsync(new(null, DateTimeOffset.UtcNow), rows, default);
            using var worker = new PostgreSqlDirectDispatcher(source, new() { MaxDispatchAttempts = 1 }, new(), [transport], provider.GetRequiredService<Microsoft.Extensions.Logging.ILogger<PostgreSqlDirectDispatcher>>());
            await worker.DispatchRecoveredAsync(rows.AsMemory(0, count), default);
        }
        await Assert.That(await Count(source, "local_business")).IsEqualTo(1L);
        await Assert.That(await Count(source, "relayer_outbox_dead")).IsEqualTo(1L);
        await using var check = source.CreateCommand("SELECT value FROM local_business");
        await Assert.That((int)(await check.ExecuteScalarAsync())!).IsEqualTo(9);
    }

    [Test]
    public async Task Killed_consumer_rolls_back_business_work_and_another_process_completes_once()
    {
        await using var database = server.CreateDatabase();
        await database.StartAsync();
        await using var source = NpgsqlDataSource.Create(database.ConnectionString);
        await PostgreSqlOutboxStoreTests.CreateSchema(source);
        await using (var command = source.CreateCommand("CREATE TABLE local_business (value integer PRIMARY KEY)")) await command.ExecuteNonQueryAsync();
        await using var provider = Provider(source);
        await Enqueue(provider, source, new(42), true);
        using (var killed = StartProbe(database.ConnectionString, "block"))
        {
            var errors = killed.StandardError.ReadToEndAsync();
            try
            {
                var signal = await killed.StandardOutput.ReadLineAsync().WaitAsync(TimeSpan.FromSeconds(30));
                if (signal != "LOCAL_BUSINESS_WRITTEN")
                {
                    if (!killed.HasExited) killed.Kill(entireProcessTree: true);
                    await killed.WaitForExitAsync();
                    throw new InvalidOperationException($"Local crash probe did not enter handler: {signal}; {await errors}");
                }
                killed.Kill(entireProcessTree: true);
                await killed.WaitForExitAsync();
            }
            finally { if (!killed.HasExited) { killed.Kill(entireProcessTree: true); await killed.WaitForExitAsync(); } }
        }
        await Assert.That(await Count(source, "local_business")).IsEqualTo(0L);
        await Assert.That(await Count(source, "relayer_outbox")).IsEqualTo(1L);
        await using (var expire = source.CreateCommand("UPDATE relayer_outbox SET claim_expires_at = clock_timestamp() - interval '1 second' WHERE claim_token IS NOT NULL; UPDATE relayer_leases SET expires_at = clock_timestamp() - interval '1 second'")) await expire.ExecuteNonQueryAsync();
        using var recovered = StartProbe(database.ConnectionString, "complete");
        var recoveredErrors = recovered.StandardError.ReadToEndAsync();
        try
        {
            using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(40));
            var output = recovered.StandardOutput.ReadToEndAsync(deadline.Token);
            await recovered.WaitForExitAsync(deadline.Token);
            await Assert.That(recovered.ExitCode).IsEqualTo(0);
            await Assert.That((await output).Contains("LOCAL_COMPLETED", StringComparison.Ordinal)).IsTrue();
            await Assert.That(await Count(source, "local_business")).IsEqualTo(1L);
            await Assert.That(await Count(source, "relayer_outbox")).IsEqualTo(0L);
        }
        catch (Exception error) { if (!recovered.HasExited) { recovered.Kill(entireProcessTree: true); await recovered.WaitForExitAsync(); } error.Data["ProbeError"] = await recoveredErrors; throw; }
        finally { if (!recovered.HasExited) { recovered.Kill(entireProcessTree: true); await recovered.WaitForExitAsync(); } }
    }
    private static System.Diagnostics.Process StartProbe(string connectionString, string mode)
    {
        var start = new System.Diagnostics.ProcessStartInfo("dotnet") { RedirectStandardOutput = true, RedirectStandardError = true, CreateNoWindow = true };
        start.ArgumentList.Add("exec");
        start.ArgumentList.Add("--runtimeconfig");
        start.ArgumentList.Add(Path.ChangeExtension(typeof(LocalTransportTests).Assembly.Location, ".runtimeconfig.json"));
        start.ArgumentList.Add("--depsfile");
        start.ArgumentList.Add(Path.ChangeExtension(typeof(LocalTransportTests).Assembly.Location, ".deps.json"));
        start.ArgumentList.Add(typeof(CrashProbeMarker).Assembly.Location);
        start.ArgumentList.Add("local"); start.ArgumentList.Add(connectionString); start.ArgumentList.Add(mode);
        return System.Diagnostics.Process.Start(start)!;
    }

    [Test]
    public async Task Native_outbox_commit_hands_off_to_the_shared_direct_worker()
    {
        await using var database = server.CreateDatabase();
        await database.StartAsync();
        await using var source = NpgsqlDataSource.Create(database.ConnectionString);
        await PostgreSqlOutboxStoreTests.CreateSchema(source);
        await using (var command = source.CreateCommand("CREATE TABLE local_business (value integer PRIMARY KEY)")) await command.ExecuteNonQueryAsync();
        await using var provider = Provider(source, directHandoff: true);
        var services = provider.GetServices<IHostedService>().ToArray();
        foreach (var service in services) await service.StartAsync(default);
        try
        {
            await Enqueue(provider, source, new(77), true);
            // Stop drains the handoff queue: no sleeps or recovery polls make this test pass.
            await provider.GetRequiredKeyedService<PostgreSqlDirectDispatcher>("local").StopAsync(default);
            await Assert.That(await Count(source, "local_business")).IsEqualTo(1L);
            await Assert.That(await Count(source, "relayer_outbox")).IsEqualTo(0L);
        }
        finally { for (var index = services.Length - 1; index >= 0; index--) await services[index].StopAsync(default); }
    }

    [Test]
    [Arguments("expired")]
    [Arguments("stale")]
    public async Task Lost_claim_rolls_back_handler_business_work(string kind)
    {
        await using var database = server.CreateDatabase();
        await database.StartAsync();
        await using var source = NpgsqlDataSource.Create(database.ConnectionString);
        await PostgreSqlOutboxStoreTests.CreateSchema(source);
        await using (var command = source.CreateCommand("CREATE TABLE local_business (value integer PRIMARY KEY)")) await command.ExecuteNonQueryAsync();
        await using var provider = Provider(source);
        foreach (var service in provider.GetServices<IHostedService>()) if (service is not PostgreSqlDirectDispatcher) await service.StartAsync(default);
        await Enqueue(provider, source, new(kind == "expired" ? -2 : -3), true);
        var store = new PostgreSqlOutboxStore(source);
        var rows = new OutboxRecord[1];
        await store.ReadPendingAsync(new(null, DateTimeOffset.UtcNow), rows, default);
        using var worker = new PostgreSqlDirectDispatcher(source, new() { MaxDispatchAttempts = 1 }, new(), provider.GetServices<ITransport>(), provider.GetRequiredService<ILogger<PostgreSqlDirectDispatcher>>());
        await worker.DispatchRecoveredAsync(rows, default);
        await Assert.That(await Count(source, "local_business")).IsEqualTo(0L);
        await Assert.That(await Count(source, "relayer_outbox")).IsEqualTo(1L);
        await Assert.That(await Count(source, "relayer_outbox_dead")).IsEqualTo(0L);
    }

    [Test]
    public async Task Unsupported_envelopes_batch_handlers_and_missing_consumers_fail_at_startup()
    {
        await using var source = NpgsqlDataSource.Create("Host=unused");
        foreach (var kind in new[] { "raw", "batch", "missing" })
        {
            var services = new ServiceCollection().AddLogging();
            services.AddRelayer(builder =>
            {
                builder.AddMessageTypesFromThisAssembly();
                builder.PostgreSqlLocal("local", source, transport =>
                {
                    transport.Send<LocalWork>("work", route => { if (kind == "raw") route.SendOptions = new() { EnvelopeMode = MessageEnvelopeMode.Raw }; });
                    if (kind != "missing") transport.Consume("work", consumer => { consumer.Handler<LocalHandler>(); if (kind == "batch") consumer.Batch(2, TimeSpan.FromSeconds(1)); });
                });
            });
            services.AddSingleton(source);
        services.AddScoped<FailingCleanup>();
            services.AddRelayerPostgreSqlDirectDispatch("local", source);
            await using var provider = services.BuildServiceProvider();
            await Assert.ThrowsAsync<Exception>(async () =>
            {
                foreach (var service in provider.GetServices<IHostedService>()) if (service is not PostgreSqlDirectDispatcher) await service.StartAsync(default);
            });
        }
    }

    [Test]
    [Arguments(false, FailureAction.Discard)]
    [Arguments(true, FailureAction.Discard)]
    [Arguments(false, FailureAction.DeadLetter)]
    [Arguments(true, FailureAction.DeadLetter)]
    public async Task Failure_classifier_discard_and_permanent_failure_roll_back_business_changes(bool ordered, FailureAction action)
    {
        await using var database = server.CreateDatabase();
        await database.StartAsync();
        await using var source = NpgsqlDataSource.Create(database.ConnectionString);
        await PostgreSqlOutboxStoreTests.CreateSchema(source);
        await using (var command = source.CreateCommand("CREATE TABLE local_business (value integer PRIMARY KEY)")) await command.ExecuteNonQueryAsync();
        await using var provider = Provider(source, ordered, maxAttempts: 10, failureAction: action);
        foreach (var service in provider.GetServices<IHostedService>()) if (service is not PostgreSqlDirectDispatcher) await service.StartAsync(default);
        await Enqueue(provider, source, new(-1), true);
        if (ordered)
        {
            using var worker = new PostgreSqlOrderedDispatcher(source, new() { Lanes = 1, MaxDispatchAttempts = 10 }, provider.GetServices<ITransport>(), provider.GetRequiredService<ILogger<PostgreSqlOrderedDispatcher>>());
            await worker.DispatchLaneAsync(0, default);
        }
        else
        {
            var rows = new OutboxRecord[1];
            await new PostgreSqlOutboxStore(source).ReadPendingAsync(new(null, DateTimeOffset.UtcNow), rows, default);
            using var worker = new PostgreSqlDirectDispatcher(source, new(), new(), provider.GetServices<ITransport>(), provider.GetRequiredService<ILogger<PostgreSqlDirectDispatcher>>());
            await worker.DispatchRecoveredAsync(rows, default);
        }
        await Assert.That(await Count(source, "local_business")).IsEqualTo(0L);
        await Assert.That(await Count(source, "relayer_outbox_dead")).IsEqualTo(action == FailureAction.DeadLetter ? 1L : 0L);
        await Assert.That(await Count(source, "relayer_outbox")).IsEqualTo(action == FailureAction.DeadLetter && ordered ? 1L : 0L);
    }
    private sealed class FixedClassifier(FailureAction action) : IFailureClassifier
    {
        public FailureAction Classify(Exception exception, MessageContext context) => action;
    }

    [Test]
    public async Task Admission_bounds_attempts_without_claiming_or_queueing_work()
    {
        await using var source = NpgsqlDataSource.Create("Host=unused");
        await using var transport = new PostgreSqlLocalTransport("local", source, new(), ["work"]);
        transport.Register("work", static (_, _) => new(Disposition.SettledByHandler), concurrency: 2);
        var first = transport.TryEnter("work");
        var second = transport.TryEnter("work");
        await Assert.That(first is not null && second is not null).IsTrue();
        await Assert.That(transport.TryEnter("work") is null).IsTrue();
        first!.Exit();
        var replacement = transport.TryEnter("work");
        await Assert.That(replacement is not null).IsTrue();
        replacement!.Exit(); second!.Exit();
    }

    [Test]
    public async Task Concurrent_send_waves_beyond_initial_capacity_keep_rows_distinct()
    {
        await using var database = server.CreateDatabase();
        await database.StartAsync();
        var connectionString = new NpgsqlConnectionStringBuilder(database.ConnectionString) { MaxPoolSize = 1 }.ConnectionString;
        await using var source = NpgsqlDataSource.Create(connectionString);
        await PostgreSqlOutboxStoreTests.CreateSchema(source);
        await using var transport = new PostgreSqlLocalTransport("local", source, new(), ["work"]);
        const int concurrency = 33;
        for (var wave = 0; wave < 2; wave++)
        {
            // Holding the only native connection forces every send to rent its own state
            // before any of them can finish; there is no scheduler timing assumption.
            var blocker = await source.OpenConnectionAsync();
            var sends = new Task[concurrency];
            try
            {
                for (var i = 0; i < concurrency; i++)
                {
                    KeyValuePair<string, WireString>[] headers = [new("ce_id", new(Guid.CreateVersion7().ToString())), new("ce_type", new("local.work;v=1"))];
                    sends[i] = transport.SendAsync(new("work", new byte[] { (byte)(wave * concurrency + i) }, headers), default).AsTask();
                }
            }
            finally { await blocker.DisposeAsync(); }
            await Task.WhenAll(sends);
        }
        await using var check = source.CreateCommand("SELECT count(DISTINCT body) FROM relayer_outbox");
        await Assert.That((long)(await check.ExecuteScalarAsync())!).IsEqualTo(2L * concurrency);
    }

    [Test]
    public async Task Completion_saturation_reuses_stores_after_all_borrowed_deliveries_finish()
    {
        await using var database = server.CreateDatabase();
        await database.StartAsync();
        await using var source = NpgsqlDataSource.Create(database.ConnectionString);
        await using var transport = new PostgreSqlLocalTransport("local", source, new(), ["work"]);
        const int concurrency = 65;
        var stores = new HashSet<PostgreSqlOutboxStore>();
        var firstWave = new HashSet<PostgreSqlOutboxStore>();
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var count = 0;
        transport.Register("work", async (delivery, _) =>
        {
            lock (stores)
            {
                if (!stores.Add(delivery.CompletionStore)) throw new InvalidOperationException("A live completion store was shared.");
                if (++count == concurrency) entered.SetResult();
            }
            await release.Task;
            return Disposition.SettledByHandler;
        }, concurrency);
        for (var wave = 0; wave < 2; wave++)
        {
            var attempts = new Task[concurrency];
            for (var i = 0; i < concurrency; i++)
                attempts[i] = transport.ConsumeAsync(new("work", default), new(Guid.CreateVersion7(), Guid.NewGuid()), 1, null, default).AsTask();
            try { await entered.Task.WaitAsync(TimeSpan.FromSeconds(30)); }
            finally { release.TrySetResult(); await Task.WhenAll(attempts); }
            if (wave == 0) firstWave.UnionWith(stores);
            else await Assert.That(stores.SetEquals(firstWave)).IsTrue();
            stores.Clear(); count = 0;
            release = new(TaskCreationOptions.RunContinuationsAsynchronously);
            entered = new(TaskCreationOptions.RunContinuationsAsynchronously);
        }
    }

    internal static ServiceProvider Provider(NpgsqlDataSource source, bool ordered = false, bool directHandoff = false, int maxAttempts = 1, FailureAction? failureAction = null)
    {
        var services = new ServiceCollection().AddLogging(logging => logging.AddConsole());
        services.AddSingleton(source);
        services.AddScoped<FailingCleanup>();
        if (failureAction is { } action) services.AddSingleton<IFailureClassifier>(new FixedClassifier(action));
        services.AddRelayer(builder =>
        {
            builder.AddMessageTypesFromThisAssembly();
            builder.PostgreSqlLocal("local", source, transport =>
            {
                transport.Send<LocalWork>("work", route => { if (ordered) route.OrderingKey(message => message.Value.ToString()); });
                transport.Consume("work", consumer => consumer.Handler<LocalHandler>());
            }, new() { Lanes = 1, MaxDispatchAttempts = maxAttempts });
        });
        if (directHandoff) services.AddRelayerPostgreSqlOutbox("local", new() { Lanes = 1, MaxDispatchAttempts = maxAttempts });
        else services.AddRelayerPostgreSqlOutbox(new PostgreSqlOutboxOptions() { Lanes = 1, MaxDispatchAttempts = maxAttempts });
        services.AddRelayerPostgreSqlDirectDispatch("local", source, new PostgreSqlOutboxOptions() { MaxDispatchAttempts = maxAttempts });
        return services.BuildServiceProvider();
    }
    internal static async Task Enqueue(ServiceProvider provider, NpgsqlDataSource source, LocalWork message, bool commit)
    {
        await using var scope = provider.CreateAsyncScope();
        var outbox = scope.ServiceProvider.GetRequiredService<PostgreSqlOutbox>();
        await using var transaction = await outbox.BeginTransactionAsync(source);
        await scope.ServiceProvider.GetRequiredService<ISender>().SendAsync(message);
        if (commit) await transaction.CommitAsync();
        else await transaction.RollbackAsync();
    }
    internal static async Task<long> Count(NpgsqlDataSource source, string table)
    {
        await using var command = source.CreateCommand("SELECT count(*) FROM " + table);
        return (long)(await command.ExecuteScalarAsync())!;
    }
    public sealed class FailingCleanup(ISender sender) : IAsyncDisposable
    {
        public async ValueTask DisposeAsync()
        {
            await sender.SendAsync(new LocalWork(2000));
            throw new InvalidOperationException("scope cleanup failed");
        }
    }
    [Message("local.work")]
    public sealed record LocalWork(int Value);
    public sealed class LocalHandler(NpgsqlDataSource source, ISender sender) : IHandler<LocalWork>
    {
        public async ValueTask HandleAsync(LocalWork message, MessageContext context, CancellationToken cancellationToken)
        {
            var delivery = context.Features.GetRequired<NativeMessage<PostgreSqlLocalDelivery>>().Message;
            await using var command = new NpgsqlCommand("INSERT INTO local_business VALUES (@value)", delivery.Transaction.Connection, delivery.Transaction);
            command.Parameters.AddWithValue("value", message.Value);
            await command.ExecuteNonQueryAsync(context.CancellationToken);
            if (message.Value is 10 or -10 or -11)
            {
                await sender.SendAsync(new LocalWork(1000), cancellationToken);
                if (message.Value == -11) context.Services.GetRequiredService<FailingCleanup>();
            }
            if (message.Value is -2 or -3)
            {
                await using var fence = source.CreateCommand(message.Value == -2
                    ? "UPDATE relayer_outbox SET claim_expires_at = clock_timestamp() WHERE id = @id"
                    : "UPDATE relayer_outbox SET claim_token = @token WHERE id = @id");
                fence.Parameters.AddWithValue("id", delivery.Claim.MessageId);
                if (message.Value == -3) fence.Parameters.AddWithValue("token", Guid.NewGuid());
                await fence.ExecuteNonQueryAsync(cancellationToken);
            }
            else if (message.Value < 0 && message.Value != -11) throw new InvalidOperationException("poison");
        }
    }
}
