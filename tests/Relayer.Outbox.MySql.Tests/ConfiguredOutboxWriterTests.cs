using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Relayer.TestFixtures;

namespace Relayer.Outbox.MySql.Tests;

[InheritsTests]
public sealed class ConfiguredOutboxWriterTests : OutboxWriterConfigurationTests<MySqlOutboxOptions>
{
    protected override void Register(IServiceCollection services, IConfiguration configuration)
    {
        services.AddRelayerMySqlOutbox(configuration);
    }

    protected override void Register(IServiceCollection services, int maxDispatchAttempts)
    {
        services.AddRelayerMySqlOutbox(new MySqlOutboxOptions { MaxDispatchAttempts = maxDispatchAttempts });
    }

    protected override (int Attempts, int Lanes) ReadSettings(MySqlOutboxOptions options)
    {
        return (options.MaxDispatchAttempts, options.Lanes);
    }

    protected override string ReadPrefix(MySqlOutboxOptions options)
    {
        return options.TablePrefix;
    }
}
