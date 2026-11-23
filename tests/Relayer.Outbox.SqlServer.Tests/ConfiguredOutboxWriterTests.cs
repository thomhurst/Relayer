using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Relayer.TestFixtures;

namespace Relayer.Outbox.SqlServer.Tests;

[InheritsTests]
public sealed class ConfiguredOutboxWriterTests : OutboxWriterConfigurationTests<SqlServerOutboxOptions>
{
    protected override void Register(IServiceCollection services, IConfiguration configuration)
    {
        services.AddRelayerSqlServerOutbox(configuration);
    }

    protected override void Register(IServiceCollection services, int maxDispatchAttempts)
    {
        services.AddRelayerSqlServerOutbox(new SqlServerOutboxOptions { MaxDispatchAttempts = maxDispatchAttempts });
    }

    protected override (int Attempts, int Lanes) ReadSettings(SqlServerOutboxOptions options)
    {
        return (options.MaxDispatchAttempts, options.Lanes);
    }

    protected override string ReadPrefix(SqlServerOutboxOptions options)
    {
        return options.TablePrefix;
    }
}
