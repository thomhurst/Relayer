using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Npgsql;
using Microsoft.Data.SqlClient;
using Relayer.Outbox.SqlServer;
using Relayer;
using Relayer.Outbox.PostgreSql;
using Relayer.Transports;

if (args.Length > 1 && args[0] == "local")
{
    await using var localSource = NpgsqlDataSource.Create(args[1]);
    var localServices = new ServiceCollection().AddLogging();
    localServices.AddSingleton(new LocalCrashMode(args[2] == "block"));
    localServices.AddRelayer(builder =>
    {
        builder.AddMessageTypesFromThisAssembly();
        builder.PostgreSqlLocal("local", localSource, transport =>
            transport.Consume("work", consumer => consumer.Handler<LocalCrashHandler>()));
    });
    localServices.AddRelayerPostgreSqlDirectDispatch("local", localSource);
    localServices.AddRelayerPostgreSqlMaintenance("local", localSource, new PostgreSqlMaintenanceOptions
    {
        RecoveryDelay = TimeSpan.Zero, SweepInterval = TimeSpan.FromMilliseconds(20)
    });
    await using var localProvider = localServices.BuildServiceProvider();
    var workers = localProvider.GetServices<IHostedService>().ToArray();
    foreach (var worker in workers) await worker.StartAsync(default);
    if (args[2] == "block") await Task.Delay(Timeout.InfiniteTimeSpan);
    else
    {
        using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        while (true)
        {
            await using var check = localSource.CreateCommand("SELECT count(*) FROM relayer_outbox");
            if ((long)(await check.ExecuteScalarAsync(deadline.Token))! == 0) break;
            await Task.Delay(20, deadline.Token);
        }
        for (var i = workers.Length - 1; i >= 0; i--) await workers[i].StopAsync(deadline.Token);
        Console.WriteLine("LOCAL_COMPLETED");
    }
    return;
}

if (args.Length > 1 && args[0] == "sqlserver")
{
    var sqlServices = new ServiceCollection().AddLogging();
    sqlServices.AddSingleton<ITransport>(new SqlServerCrashTransport(args[1], args[2]));
    sqlServices.AddRelayerSqlServerMaintenance("crash", args[1], new SqlServerMaintenanceOptions
    {
        TablePrefix = args[2], BatchSize = 1, RecoveryDelay = TimeSpan.Zero
    });
    await using var sqlProvider = sqlServices.BuildServiceProvider();
    foreach (var worker in sqlProvider.GetServices<IHostedService>()) await worker.StartAsync(default);
    await Task.Delay(Timeout.InfiniteTimeSpan);
    return;
}

await using var source = NpgsqlDataSource.Create(args[0]);
var services = new ServiceCollection().AddLogging();
services.AddSingleton<ITransport>(new CrashTransport(source));
services.AddRelayerPostgreSqlMaintenance("crash", source, new PostgreSqlMaintenanceOptions
{
    BatchSize = 1,
    RecoveryDelay = TimeSpan.Zero
});
await using var provider = services.BuildServiceProvider();
foreach (var worker in provider.GetServices<IHostedService>()) await worker.StartAsync(default);
await Task.Delay(Timeout.InfiniteTimeSpan);

public sealed class CrashProbeMarker;

internal sealed class CrashTransport(NpgsqlDataSource source) : ITransport
{
    public string Name => "events";
    public TransportCapabilities GetCapabilities(string destination) => new() { MaxBatchSize = 1 };
    public async ValueTask SendAsync(OutgoingMessage message, CancellationToken cancellationToken)
    {
        await using var command = source.CreateCommand("INSERT INTO crash_deliveries (id, body) VALUES (@id, @body)");
        command.Parameters.AddWithValue("id", Guid.Parse(message.Headers.Span[0].Value.ToString()));
        command.Parameters.AddWithValue("body", message.Body.ToArray());
        await command.ExecuteNonQueryAsync(cancellationToken);
        Console.WriteLine("BROKER_ACCEPTED");
        // The parent kills this process after acceptance and before outbox completion.
        await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken);
    }
    public ValueTask SendBatchAsync(ReadOnlyMemory<OutgoingMessage> messages, CancellationToken cancellationToken) => throw new NotSupportedException();
    public ValueTask<TopologyReport> ApplyTopologyAsync(TopologyMode mode, CancellationToken cancellationToken) => new(new TopologyReport([]));
    public Task StartAsync(CancellationToken cancellationToken) => Task.CompletedTask;
    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
    public ValueTask DisposeAsync() => ValueTask.CompletedTask;
}

internal sealed class SqlServerCrashTransport(string connectionString, string prefix) : ITransport
{
    public string Name => "events";
    public TransportCapabilities GetCapabilities(string destination) => new() { MaxBatchSize = 1 };
    public async ValueTask SendAsync(OutgoingMessage message, CancellationToken cancellationToken)
    {
        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync(cancellationToken);
        await using var command = new SqlCommand($"INSERT INTO [dbo].[{prefix.Replace("]", "]]")}acceptances] (id) VALUES (@id)", connection);
        command.Parameters.AddWithValue("@id", Guid.Parse(message.Headers.Span[0].Value.ToString()));
        await command.ExecuteNonQueryAsync(cancellationToken);
        Console.WriteLine("BROKER_ACCEPTED");
        await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken);
    }
    public ValueTask SendBatchAsync(ReadOnlyMemory<OutgoingMessage> messages, CancellationToken cancellationToken) => throw new NotSupportedException();
    public ValueTask<TopologyReport> ApplyTopologyAsync(TopologyMode mode, CancellationToken cancellationToken) => new(new TopologyReport([]));
    public Task StartAsync(CancellationToken cancellationToken) => Task.CompletedTask;
    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
    public ValueTask DisposeAsync() => ValueTask.CompletedTask;
}

internal sealed record LocalCrashMode(bool Block);
[Message("local.work")]
public sealed record LocalCrashWork(int Value);
internal sealed class LocalCrashHandler(LocalCrashMode mode) : IHandler<LocalCrashWork>
{
    public async ValueTask HandleAsync(LocalCrashWork message, MessageContext context, CancellationToken cancellationToken)
    {
        var delivery = context.Features.GetRequired<NativeMessage<PostgreSqlLocalDelivery>>().Message;
        await using var command = new NpgsqlCommand("INSERT INTO local_business VALUES (@value)", delivery.Transaction.Connection, delivery.Transaction);
        command.Parameters.AddWithValue("value", message.Value);
        await command.ExecuteNonQueryAsync(cancellationToken);
        Console.WriteLine("LOCAL_BUSINESS_WRITTEN");
        if (mode.Block) await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken);
    }
}
