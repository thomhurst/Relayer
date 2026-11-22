using System.Diagnostics.Metrics;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Relayer.Outbox.SqlServer;
using Relayer.Outbox.SqlServer.Tests;
using Relayer.RabbitMQ;
using Relayer.Transports;

namespace Relayer.Tests;

[ClassDataSource<SqlServerFixture>(Shared = SharedType.PerTestSession)]
public sealed class OutboxMaintenanceStartupTests(SqlServerFixture database)
{
    [Test]
    public async Task SqlServer_recovery_waits_for_RabbitMQ_startup_before_claiming_a_due_row()
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(3));
        var token = timeout.Token;
        var prefix = TestContext.Current!.Isolation.GetIsolatedName("startup") + "_";
        var storeOptions = new SqlServerOutboxOptions { TablePrefix = prefix, MaxDispatchAttempts = 1 };
        var connectionString = new SqlConnectionStringBuilder(database.ConnectionString) { InitialCatalog = "master" }.ConnectionString;
        await using var sql = new SqlConnection(connectionString);
        await sql.OpenAsync(token);
        await using var schema = new SqlCommand(SqlServerOutboxSchema.CreateScript(storeOptions), sql);
        await schema.ExecuteNonQueryAsync(token);

        await using var broker = await RabbitMqTopologyTests.Broker.StartAsync(token);
        var factory = broker.Factory();
        await using var connection = await factory.CreateConnectionAsync(token);
        await using var channel = await connection.CreateChannelAsync(cancellationToken: token);
        await channel.QueueDeclareAsync("orders", true, false, false, cancellationToken: token);
        using var host = new HostBuilder().ConfigureServices(services =>
        {
            services.AddRelayer(relayer =>
            {
                relayer.AddMessageTypesFromThisAssembly();
                relayer.RabbitMQ("rabbit", rabbit =>
                {
                    rabbit.TopologyMode = TopologyMode.None;
                    rabbit.Options.Configure(options => options.ConnectionFactory = factory);
                    rabbit.Send<OrderPlaced>("orders");
                });
            });
        }).Build();
        await using var hostLifetime = (IAsyncDisposable)host;

        // Start the registered maintenance service with the RabbitMQ host's lifetime held at pre-start.
        var startup = new ObservedLifetime(host.Services.GetRequiredService<IHostApplicationLifetime>());
        var services = new ServiceCollection().AddLogging();
        services.AddSingleton<IHostApplicationLifetime>(startup);
        services.AddSingleton(host.Services.GetRequiredService<ITransport>());
        services.AddSingleton<RelayerInstrumentation>();
        services.AddRelayerSqlServerOutbox(storeOptions);
        services.AddRelayerSqlServerMaintenance("orders", connectionString, new SqlServerMaintenanceOptions
        {
            TablePrefix = prefix,
            RecoveryDelay = TimeSpan.Zero,
            MaxDispatchAttempts = 1
        });
        await using var provider = services.BuildServiceProvider();
        await using var scope = provider.CreateAsyncScope();
        var outbox = scope.ServiceProvider.GetRequiredService<SqlServerOutbox>();

        await using (var transaction = await outbox.BeginTransactionAsync(sql, cancellationToken: token))
        {
            var row = new OutboxRecord(Guid.CreateVersion7(), "rabbit", "startup.order.placed.v1",
                new OutgoingMessage("orders", new byte[] { 42 }), DateTimeOffset.UtcNow.AddMinutes(-1));
            await outbox.EnlistAsync(row, token);
            await transaction.CommitAsync(token);
        }

        // A negative pending delta is emitted after the transaction commits, including dead-lettering.
        var settled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var instrumentation = provider.GetRequiredService<RelayerInstrumentation>();
        using var listener = new MeterListener
        {
            InstrumentPublished = (instrument, meterListener) =>
            {
                if (ReferenceEquals(instrument.Meter.Scope, instrumentation) && instrument.Name == "relayer.outbox.pending")
                {
                    meterListener.EnableMeasurementEvents(instrument);
                }
            }
        };
        listener.SetMeasurementEventCallback<long>((_, delta, _, _) =>
        {
            if (delta < 0)
            {
                settled.TrySetResult();
            }
        });
        listener.Start();
        var worker = provider.GetRequiredService<IHostedService>();
        await worker.StartAsync(token);

        try
        {
            await Task.WhenAny(startup.Observed, settled.Task).WaitAsync(token);
            await using var pending = new SqlCommand($"SELECT COUNT_BIG(*) FROM [dbo].[{prefix}outbox] WHERE attempts=0", sql);
            await Assert.That((long)(await pending.ExecuteScalarAsync(token))!).IsEqualTo(1L);

            await host.StartAsync(token);
            await settled.Task.WaitAsync(token);
            var received = await channel.BasicGetAsync("orders", true, token);
            await Assert.That(received?.Body.ToArray()).IsEquivalentTo(new byte[] { 42 });
        }
        finally
        {
            await worker.StopAsync(CancellationToken.None);
            await host.StopAsync(CancellationToken.None);
        }
    }

    [Message("startup.order.placed")]
    public sealed record OrderPlaced;

    private sealed class ObservedLifetime(IHostApplicationLifetime lifetime) : IHostApplicationLifetime
    {
        private readonly TaskCompletionSource _observed = new(TaskCreationOptions.RunContinuationsAsynchronously);

        internal Task Observed => _observed.Task;

        public CancellationToken ApplicationStarted
        {
            get
            {
                _observed.TrySetResult();
                return lifetime.ApplicationStarted;
            }
        }

        public CancellationToken ApplicationStopping => lifetime.ApplicationStopping;

        public CancellationToken ApplicationStopped => lifetime.ApplicationStopped;

        public void StopApplication()
        {
            lifetime.StopApplication();
        }
    }
}
