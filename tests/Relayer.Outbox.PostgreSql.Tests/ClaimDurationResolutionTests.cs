using System.Globalization;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Npgsql;
using Npgsql.Replication.PgOutput;
using NpgsqlTypes;

namespace Relayer.Outbox.PostgreSql.Tests;

public sealed class ClaimDurationResolutionTests
{
    [Test]
    [Arguments("direct", false)]
    [Arguments("direct", true)]
    [Arguments("maintenance", false)]
    [Arguments("maintenance", true)]
    [Arguments("cdc", false)]
    [Arguments("cdc", true)]
    public async Task Registration_requires_at_least_one_microsecond(string worker, bool fromConfiguration)
    {
        await using var source = NpgsqlDataSource.Create("Host=unused");
        var slot = new PgOutputReplicationSlot("orders");
        var replication = new PgOutputReplicationOptions("orders", PgOutputProtocolVersion.V1);
        var tooShort = TimeSpan.FromTicks(1);

        Register(TimeSpan.FromMicroseconds(1));
        var exception = await Assert.That(() => Register(tooShort)).Throws<ArgumentOutOfRangeException>();
        await Assert.That(exception!.ParamName).IsEqualTo("ClaimDuration");
        await Assert.That(exception.Message).Contains("at least 1 microsecond");

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

    [Test]
    [Arguments(1, false)]
    [Arguments(10, true)]
    public async Task Native_interval_is_positive_at_one_microsecond(int ticks, bool positive)
    {
        var session = await PostgreSqlSession.Current;
        await using var command = session.Admin.CreateCommand("SELECT $1 > interval '0'");
        command.Parameters.AddWithValue(NpgsqlDbType.Interval, TimeSpan.FromTicks(ticks));

        await Assert.That((bool)(await command.ExecuteScalarAsync())!).IsEqualTo(positive);
    }
}
