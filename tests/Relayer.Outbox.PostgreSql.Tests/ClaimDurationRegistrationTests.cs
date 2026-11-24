using System.Globalization;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Npgsql;
using Npgsql.Replication.PgOutput;

namespace Relayer.Outbox.PostgreSql.Tests;

public sealed class ClaimDurationRegistrationTests
{
    [Test]
    [Arguments("direct", false)]
    [Arguments("direct", true)]
    [Arguments("maintenance", false)]
    [Arguments("maintenance", true)]
    [Arguments("cdc", false)]
    [Arguments("cdc", true)]
    public async Task Claim_duration_is_validated_at_registration(string worker, bool fromConfiguration)
    {
        await using var source = NpgsqlDataSource.Create("Host=unused");
        var maximum = TimeSpan.FromDays(1_000_000);
        var slot = new PgOutputReplicationSlot("orders");
        var replication = new PgOutputReplicationOptions("orders", PgOutputProtocolVersion.V1);

        Register(TimeSpan.FromSeconds(30));
        Register(maximum);

        foreach (var invalid in new[] { TimeSpan.MaxValue, maximum.Add(TimeSpan.FromTicks(1)), TimeSpan.Zero })
        {
            var exception = await Assert.That(() => Register(invalid)).Throws<ArgumentOutOfRangeException>();
            await Assert.That(exception!.ParamName).IsEqualTo("ClaimDuration");
            await Assert.That(exception.Message).Contains("positive and at most 1000000 days");
        }

        void Register(TimeSpan duration)
        {
            var services = new ServiceCollection();

            if (fromConfiguration)
            {
                var configuration = new ConfigurationBuilder().AddInMemoryCollection(new Dictionary<string, string?>
                {
                    ["ClaimDuration"] = duration.ToString("c", CultureInfo.InvariantCulture)
                }).Build();

                switch (worker)
                {
                    case "direct":
                        services.AddRelayerPostgreSqlDirectDispatch("orders", source, configuration);
                        break;
                    case "maintenance":
                        services.AddRelayerPostgreSqlMaintenance("orders", source, configuration);
                        break;
                    default:
                        services.AddRelayerPostgreSqlChangeCapture("orders", source, () => new("Host=unused"), slot, replication, configuration);
                        break;
                }

                return;
            }

            switch (worker)
            {
                case "direct":
                    services.AddRelayerPostgreSqlDirectDispatch("orders", source, dispatchOptions: new()
                    {
                        ClaimDuration = duration
                    });
                    break;
                case "maintenance":
                    services.AddRelayerPostgreSqlMaintenance("orders", source, new PostgreSqlMaintenanceOptions
                    {
                        ClaimDuration = duration
                    });
                    break;
                default:
                    services.AddRelayerPostgreSqlChangeCapture("orders", source, () => new("Host=unused"), new PostgreSqlChangeCaptureOptions
                    {
                        Slot = slot,
                        Replication = replication,
                        ClaimDuration = duration
                    });
                    break;
            }
        }
    }
}
