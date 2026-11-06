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
        await using var provider = new ServiceCollection().AddLogging().AddRelayerMySqlDirectDispatch("mysql-smoke", source)
            .AddRelayerMySqlOutbox(new MySqlOutboxOptions { Lanes = 8, DispatchStoreName = "mysql-smoke" })
            .BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true });
        await using var scope = provider.CreateAsyncScope();
        var store = scope.ServiceProvider.GetRequiredKeyedService<IOutboxStore>("mysql-smoke");
        var outbox = scope.ServiceProvider.GetRequiredService<MySqlOutbox>();
        if (!ReferenceEquals(outbox, scope.ServiceProvider.GetRequiredService<IOutboxEnlistment>()) || outbox.IsActive)
            throw new InvalidOperationException("MySQL enlistment registration failed under Native AOT.");
        try
        {
            await outbox.BeginTransactionAsync(source, cancellationToken: new CancellationToken(true));
            throw new InvalidOperationException("MySQL transaction cancellation failed under Native AOT.");
        }
        catch (OperationCanceledException) { }
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
