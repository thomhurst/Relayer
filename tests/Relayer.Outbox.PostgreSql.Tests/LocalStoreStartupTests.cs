using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Npgsql;
using Relayer.Outbox.EntityFrameworkCore;

namespace Relayer.Outbox.PostgreSql.Tests;

[ClassDataSource<PostgreSqlLocalSessionFixture>(Shared = SharedType.PerTestSession)]
public class LocalStoreStartupTests(PostgreSqlLocalSessionFixture server)
{
    [Test]
    [Arguments("ordered", "MaxDispatchAttempts")]
    [Arguments("maintenance", "MaxDispatchAttempts")]
    [Arguments("ordered", "Lanes")]
    [Arguments("ordered", nameof(NpgsqlDataSource))]
    [Arguments("maintenance", nameof(NpgsqlDataSource))]
    [Arguments("writer", "Lanes")]
    public async Task Incompatible_store_options_fail_before_any_worker_starts(string worker, string setting)
    {
        await using var database = server.CreateDatabase();
        await database.StartAsync();
        await using var source = NpgsqlDataSource.Create(database.ConnectionString);
        await PostgreSqlOutboxStoreTests.CreateSchema(source);
        var workerConnection = new NpgsqlConnectionStringBuilder(database.ConnectionString) { ApplicationName = worker };
        await using var workerSource = NpgsqlDataSource.Create(workerConnection.ConnectionString);
        using var host = BuildHost(source, ordered: true, worker, setting, namedWriter: true, workerSource: workerSource);

        using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        var error = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
        {
            await host.StartAsync(deadline.Token);
            await host.StopAsync(deadline.Token);
        });
        await Assert.That(error!.Message).Contains("local").And.Contains(setting);
        await Assert.That(host.Services.GetRequiredKeyedService<PostgreSqlDirectDispatcher>("direct").ExecuteTask is null).IsTrue();
        await Assert.That(host.Services.GetRequiredKeyedService<PostgreSqlOrderedDispatcher>("ordered").ExecuteTask is null).IsTrue();
        await Assert.That(host.Services.GetRequiredKeyedService<PostgreSqlMaintenanceWorker>("maintenance").ExecuteTask is null).IsTrue();
    }

    [Test]
    [Arguments(true)]
    [Arguments(false)]
    public async Task Matching_store_dispatches_standalone_and_transactional_work(bool ordered)
    {
        await using var database = server.CreateDatabase();
        await database.StartAsync();
        await using var source = NpgsqlDataSource.Create(database.ConnectionString);
        await PostgreSqlOutboxStoreTests.CreateSchema(source, new()
        {
            Lanes = 2
        });

        await using (var create = source.CreateCommand("CREATE TABLE local_business (value integer PRIMARY KEY)"))
        {
            await create.ExecuteNonQueryAsync();
        }

        // Unordered transactional sends bypass direct notification and exercise maintenance recovery.
        using var host = BuildHost(source, ordered, namedWriter: ordered);
        using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        await host.StartAsync(deadline.Token);

        try
        {
            await using var scope = host.Services.CreateAsyncScope();
            var sender = scope.ServiceProvider.GetRequiredService<ISender>();
            var outbox = scope.ServiceProvider.GetRequiredService<PostgreSqlOutbox>();
            // The key "8" hashes to lane 1 when the store has two lanes.
            await sender.SendWithoutOutboxAsync(new LocalTransportTests.LocalWork(8), deadline.Token);

            await using var transaction = await outbox.BeginTransactionAsync(source, cancellationToken: deadline.Token);
            await sender.SendAsync(new LocalTransportTests.LocalWork(7), cancellationToken: deadline.Token);
            await transaction.CommitAsync(deadline.Token);

            while (await LocalTransportTests.Count(source, "local_business", deadline.Token) != 2)
            {
                await Task.Delay(20, deadline.Token);
            }

            await Assert.That(await LocalTransportTests.Count(source, "relayer_outbox", deadline.Token)).IsEqualTo(0L);
            await Assert.That(await LocalTransportTests.Count(source, "relayer_outbox_dead", deadline.Token)).IsEqualTo(0L);
        }
        finally
        {
            using var stop = new CancellationTokenSource(TimeSpan.FromSeconds(30));
            await host.StopAsync(stop.Token);
        }
    }

    [Test]
    public async Task Unnamed_ado_writer_validates_lanes_when_the_database_is_selected()
    {
        await using var database = server.CreateDatabase();
        await database.StartAsync();
        await using var source = NpgsqlDataSource.Create(database.ConnectionString);
        await using var unrelatedDatabase = new PostgreSqlLocalSessionFixture.Database(
            TestContext.Current!.Isolation.GetIsolatedName("unrelated"));
        await unrelatedDatabase.StartAsync();
        await using var unrelatedSource = NpgsqlDataSource.Create(unrelatedDatabase.ConnectionString);
        using var host = BuildHost(source, ordered: true, mismatch: "writer");
        await using var scope = host.Services.CreateAsyncScope();
        var writer = scope.ServiceProvider.GetRequiredService<PostgreSqlOutbox>();

        await Assert.That(async () =>
        {
            await using var transaction = await writer.BeginTransactionAsync(source);
        }).Throws<InvalidOperationException>().WithMessageContaining("Lanes");

        await using var connection = await source.OpenConnectionAsync();

        await Assert.That(async () =>
        {
            await using var transaction = await writer.BeginTransactionAsync(connection);
        }).Throws<InvalidOperationException>().WithMessageContaining("Lanes");

        await using var unrelated = await writer.BeginTransactionAsync(unrelatedSource);
        await unrelated.RollbackAsync();
    }

    [Test]
    [Arguments(1)]
    [Arguments(2)]
    public async Task Entity_framework_writer_validates_lanes_when_the_context_enlists(int lanes)
    {
        await using var database = server.CreateDatabase();
        await database.StartAsync();
        await using var source = NpgsqlDataSource.Create(database.ConnectionString);
        using var host = BuildHost(source, ordered: true, efLanes: lanes);
        await using var scope = host.Services.CreateAsyncScope();
        var context = scope.ServiceProvider.GetRequiredService<OutboxRegistrationTests.OrdersContext>();

        if (lanes == 1)
        {
            await Assert.That(() => context.EnlistRelayerOutbox()).Throws<InvalidOperationException>().WithMessageContaining("Lanes");
        }
        else
        {
            using var selection = context.EnlistRelayerOutbox();
        }
    }

    private static IHost BuildHost(NpgsqlDataSource source, bool ordered, string? mismatch = null, string? setting = null,
        bool namedWriter = false, int? efLanes = null, NpgsqlDataSource? workerSource = null)
    {
        var builder = Host.CreateApplicationBuilder();
        builder.Logging.ClearProviders();
        var services = builder.Services;
        services.Configure<HostOptions>(options => options.ServicesStartConcurrently = true);
        services.AddSingleton(source);
        services.AddRelayer(relayer =>
        {
            relayer.AddMessageTypesFromThisAssembly();
            relayer.PostgreSqlLocal("local", source, transport =>
            {
                transport.Send<LocalTransportTests.LocalWork>("work", route =>
                {
                    if (ordered)
                    {
                        route.OrderingKey(message => message.Value.ToString());
                    }
                });
                transport.Consume("work", consumer => consumer.Handler<LocalTransportTests.LocalHandler>());
            }, new()
            {
                Lanes = 2
            }, dispatchStoreName: "direct");
        });

        var writerOptions = new PostgreSqlOutboxOptions { Lanes = mismatch == "writer" ? 1 : 2 };

        if (efLanes is { } lanes)
        {
            services.AddRelayerEntityFrameworkCoreOutbox();
            services.AddDbContext<OutboxRegistrationTests.OrdersContext>((provider, options) => options.UseNpgsql(source)
                .AddRelayerOutbox(provider, new()
                {
                    Lanes = lanes
                }));
        }
        else if (namedWriter)
        {
            services.AddRelayerPostgreSqlOutbox("direct", writerOptions);
        }
        else
        {
            services.AddRelayerPostgreSqlOutbox(writerOptions);
        }

        services.AddRelayerPostgreSqlDirectDispatch("direct", source);
        services.AddRelayerPostgreSqlMaintenance("maintenance", source, new PostgreSqlMaintenanceOptions
        {
            Schema = "public",
            MaxDispatchAttempts = mismatch == "maintenance" && setting == "MaxDispatchAttempts" ? 1 : 10,
            RecoveryDelay = TimeSpan.Zero,
            SweepInterval = TimeSpan.FromMilliseconds(50)
        });

        if (mismatch == "maintenance" && setting == nameof(NpgsqlDataSource))
        {
            // Keep a valid recovery worker so the failure identifies the separate pool for local work.
            services.AddRelayerPostgreSqlMaintenance("other-maintenance", workerSource!);
        }

        if (ordered)
        {
            var orderedSource = mismatch == "ordered" && setting == nameof(NpgsqlDataSource) ? workerSource! : source;
            services.AddRelayerPostgreSqlOrderedDispatch("ordered", orderedSource, new PostgreSqlOrderedDispatchOptions
            {
                Lanes = mismatch == "ordered" && setting == "Lanes" ? 1 : 2,
                MaxDispatchAttempts = mismatch == "ordered" && setting == "MaxDispatchAttempts" ? 1 : 10,
                PollInterval = TimeSpan.FromMilliseconds(50)
            });
        }

        return builder.Build();
    }
}
