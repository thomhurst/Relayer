using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Npgsql;
using Relayer;
using Relayer.Outbox.PostgreSql;
using Relayer.Transports;

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
