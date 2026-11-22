using Microsoft.Data.SqlClient;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Relayer.Outbox.SqlServer;
using Relayer.Outbox.SqlServer.Tests;
using Relayer.RabbitMQ;
using Relayer.Transports;

namespace Relayer.Tests;

[ClassDataSource<SqlServerFixture>(Shared = SharedType.PerTestSession)]
public sealed class OutboxDispatchShutdownTests(SqlServerFixture database)
{
    [Test]
    public async Task RabbitMQ_publishers_remain_available_until_SQL_Server_direct_dispatch_drains()
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(3));
        var token = timeout.Token;
        var prefix = TestContext.Current!.Isolation.GetIsolatedName("shutdown") + "_";
        var store = new SqlServerOutboxOptions { TablePrefix = prefix, DispatchStoreName = "orders", MaxDispatchAttempts = 1 };
        var connectionString = new SqlConnectionStringBuilder(database.ConnectionString) { InitialCatalog = "master" }.ConnectionString;
        await using var sql = new SqlConnection(connectionString);
        await sql.OpenAsync(token);
        await using var schema = new SqlCommand(SqlServerOutboxSchema.CreateScript(store), sql);
        await schema.ExecuteNonQueryAsync(token);

        await using var broker = await RabbitMqTopologyTests.Broker.StartAsync(token);
        var factory = broker.Factory();
        await using var connection = await factory.CreateConnectionAsync(token);
        await using var channel = await connection.CreateChannelAsync(cancellationToken: token);
        await channel.QueueDeclareAsync("orders", true, false, false, cancellationToken: token);

        var barrier = new DispatchBarrier();
        var host = new HostBuilder().ConfigureServices(services =>
        {
            services.AddRelayerSqlServerOutbox(store);
            services.AddRelayerSqlServerDirectDispatch("orders", connectionString, store);
            services.AddSingleton<ITransport>(barrier);
            // The host stops RabbitMQ, then this barrier, then the direct dispatcher.
            services.AddSingleton<IHostedService>(barrier);
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
        await host.StartAsync(token);

        try
        {
            await using var scope = host.Services.CreateAsyncScope();
            var outbox = scope.ServiceProvider.GetRequiredService<SqlServerOutbox>();

            await using (var transaction = await outbox.BeginTransactionAsync(sql, cancellationToken: token))
            {
                var row = new OutboxRecord(Guid.CreateVersion7(), barrier.Name, "shutdown.barrier.v1",
                    new OutgoingMessage("barrier", new byte[] { 1 }), DateTimeOffset.UtcNow);
                await outbox.EnlistAsync(row, token);
                await transaction.CommitAsync(token);
            }

            await barrier.Entered.Task.WaitAsync(token);

            await using (var transaction = await outbox.BeginTransactionAsync(sql, cancellationToken: token))
            {
                await scope.ServiceProvider.GetRequiredService<ISender>().SendAsync(new OrderPlaced(42), cancellationToken: token);
                await transaction.CommitAsync(token);
            }
        }
        finally
        {
            await host.StopAsync(token);
        }

        await using var dead = new SqlCommand($"SELECT COUNT_BIG(*) FROM [dbo].[{prefix}outbox_dead]", sql);
        await Assert.That((long)(await dead.ExecuteScalarAsync(token))!).IsEqualTo(0L);
        await using var pending = new SqlCommand($"SELECT COUNT_BIG(*) FROM [dbo].[{prefix}outbox]", sql);
        await Assert.That((long)(await pending.ExecuteScalarAsync(token))!).IsEqualTo(0L);
        var received = await channel.BasicGetAsync("orders", true, token);
        await Assert.That(received?.Body.ToArray()).IsEquivalentTo("""{"Id":42}"""u8.ToArray());

        var rabbit = host.Services.GetServices<ITransport>().Single(transport => transport.Name == "rabbit");
        await Assert.That(async () => await rabbit.SendAsync(new OutgoingMessage("orders", new byte[] { 2 }), token))
            .Throws<InvalidOperationException>();
    }

    [Message("shutdown.order.placed")]
    public sealed record OrderPlaced(int Id);

    private sealed class DispatchBarrier : ITransport, IHostedService
    {
        private readonly TaskCompletionSource _release = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public string Name => "shutdown-barrier";

        internal TaskCompletionSource Entered
        {
            get;
        } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public TransportCapabilities GetCapabilities(string endpoint)
        {
            return new();
        }

        public ValueTask<TopologyReport> ApplyTopologyAsync(TopologyMode mode, CancellationToken cancellationToken)
        {
            return new(new TopologyReport([]));
        }

        public async ValueTask SendAsync(OutgoingMessage message, CancellationToken cancellationToken)
        {
            Entered.TrySetResult();
            await _release.Task.WaitAsync(cancellationToken);
        }

        public ValueTask SendBatchAsync(ReadOnlyMemory<OutgoingMessage> messages, CancellationToken cancellationToken)
        {
            throw new NotSupportedException();
        }

        public Task StartAsync(CancellationToken cancellationToken)
        {
            return Task.CompletedTask;
        }

        public Task StopAsync(CancellationToken cancellationToken)
        {
            _release.TrySetResult();
            return Task.CompletedTask;
        }

        public ValueTask DisposeAsync()
        {
            return default;
        }
    }
}
