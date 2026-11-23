using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Relayer.TestFixtures;

namespace Relayer.Outbox.PostgreSql.Tests;

[InheritsTests]
public sealed class ConfiguredOutboxWriterTests : OutboxWriterConfigurationTests<PostgreSqlOutboxOptions>
{
    protected override void Register(IServiceCollection services, IConfiguration configuration)
    {
        services.AddRelayerPostgreSqlOutbox(configuration);
    }

    protected override void Register(IServiceCollection services, int maxDispatchAttempts)
    {
        services.AddRelayerPostgreSqlOutbox(new PostgreSqlOutboxOptions { MaxDispatchAttempts = maxDispatchAttempts });
    }

    protected override (int Attempts, int Lanes) ReadSettings(PostgreSqlOutboxOptions options)
    {
        return (options.MaxDispatchAttempts, options.Lanes);
    }

    protected override string ReadPrefix(PostgreSqlOutboxOptions options)
    {
        return options.TablePrefix;
    }
}
