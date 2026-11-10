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

    internal static ServiceProvider Provider(NpgsqlDataSource source, bool ordered = false, bool directHandoff = false, int maxAttempts = 1, FailureAction? failureAction = null)
    {
        var services = new ServiceCollection().AddLogging(logging => logging.AddConsole());
        services.AddSingleton(source);
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
    [Message("local.work")]
    public sealed record LocalWork(int Value);
    public sealed class LocalHandler(NpgsqlDataSource source) : IHandler<LocalWork>
    {
        public async ValueTask HandleAsync(LocalWork message, MessageContext context, CancellationToken cancellationToken)
        {
            var delivery = context.Features.GetRequired<NativeMessage<PostgreSqlLocalDelivery>>().Message;
            await using var command = new NpgsqlCommand("INSERT INTO local_business VALUES (@value)", delivery.Transaction.Connection, delivery.Transaction);
            command.Parameters.AddWithValue("value", message.Value);
            await command.ExecuteNonQueryAsync(context.CancellationToken);
            if (message.Value is -2 or -3)
            {
                await using var fence = source.CreateCommand(message.Value == -2
                    ? "UPDATE relayer_outbox SET claim_expires_at = clock_timestamp() WHERE id = @id"
                    : "UPDATE relayer_outbox SET claim_token = @token WHERE id = @id");
                fence.Parameters.AddWithValue("id", delivery.Claim.MessageId);
                if (message.Value == -3) fence.Parameters.AddWithValue("token", Guid.NewGuid());
                await fence.ExecuteNonQueryAsync(cancellationToken);
            }
            else if (message.Value < 0) throw new InvalidOperationException("poison");
        }
    }
}
