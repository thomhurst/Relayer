using Microsoft.Extensions.DependencyInjection;

namespace Relayer.Outbox.SqlServer.Tests;

public sealed class SqlServerOutboxSchemaTests
{
    [Test]
    public async Task Invalid_identifiers_fail_before_database_access()
    {
        foreach (var schema in new[] { "", "bad\0name", new string('x', 129) })
            await Assert.ThrowsAsync<ArgumentException>(() => Task.FromResult(SqlServerOutboxSchema.CreateScript(new() { Schema = schema })));
        await Assert.ThrowsAsync<ArgumentException>(() => Task.FromResult(SqlServerOutboxSchema.CreateScript(new() { TablePrefix = new string('x', 128) })));
        await Assert.ThrowsAsync<ArgumentNullException>(() => Task.FromResult(SqlServerOutboxSchema.CreateScript(new() { TablePrefix = null! })));
    }

    [Test]
    public async Task Registration_rejects_invalid_native_configuration_and_names()
    {
        var services = new ServiceCollection();
        await Assert.ThrowsAsync<ArgumentException>(() => Task.FromResult(services.AddRelayerSqlServerOutboxStore("orders", "NotANativeKeyword=value")));
        await Assert.ThrowsAsync<ArgumentException>(() => Task.FromResult(services.AddRelayerSqlServerOutboxStore(" ", "Server=localhost")));
        await Assert.ThrowsAsync<ArgumentException>(() => Task.FromResult(services.AddRelayerSqlServerOutboxStore("orders", "Server=localhost", new() { Schema = "" })));
    }
}
