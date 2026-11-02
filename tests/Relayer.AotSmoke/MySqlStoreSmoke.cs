using Microsoft.Extensions.DependencyInjection;
using MySqlConnector;
using Relayer.Outbox.MySql;

namespace Relayer;

internal static class MySqlStoreSmoke
{
    internal static async Task RunAsync()
    {
        await using var source = new MySqlDataSource("Server=127.0.0.1;Port=1;Connection Timeout=1;Database=relayer");
        if (!MySqlOutboxSchema.CreateScript().Contains("ENGINE=InnoDB", StringComparison.Ordinal))
            throw new InvalidOperationException("MySQL schema creation failed under Native AOT.");
        await using var provider = new ServiceCollection().AddRelayerMySqlOutboxStore("mysql-smoke", source)
            .BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true });
        await using var scope = provider.CreateAsyncScope();
        var store = scope.ServiceProvider.GetRequiredKeyedService<IOutboxStore>("mysql-smoke");
        if (await store.ReadPendingAsync(new(null, DateTimeOffset.UtcNow), Memory<OutboxRecord>.Empty, default) != 0)
            throw new InvalidOperationException("Empty MySQL reads failed under Native AOT.");
        try
        {
            await store.GetPendingCountAsync(DateTimeOffset.UtcNow, new CancellationToken(true));
            throw new InvalidOperationException("MySQL cancellation failed under Native AOT.");
        }
        catch (OperationCanceledException) { }
        Console.WriteLine("MySQL store registration, schema, empty reads and cancellation passed under Native AOT.");
    }
}
