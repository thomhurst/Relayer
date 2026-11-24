using System.Globalization;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace Relayer.Outbox.SqlServer.Tests;

public sealed class ClaimDurationRegistrationTests
{
    private const string ConnectionString = "Server=localhost;Database=orders;Integrated Security=True;Encrypt=False";

    [Test]
    [Arguments("direct", false)]
    [Arguments("direct", true)]
    [Arguments("maintenance", false)]
    [Arguments("maintenance", true)]
    public async Task Claim_duration_is_validated_at_registration(string worker, bool fromConfiguration)
    {
        var maximum = TimeSpan.FromDays(1_000_000);

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

                if (worker == "direct")
                {
                    services.AddRelayerSqlServerDirectDispatch("orders", ConnectionString, configuration);
                }
                else
                {
                    services.AddRelayerSqlServerMaintenance("orders", ConnectionString, configuration);
                }
            }
            else if (worker == "direct")
            {
                services.AddRelayerSqlServerDirectDispatch("orders", ConnectionString, dispatchOptions: new()
                {
                    ClaimDuration = duration
                });
            }
            else
            {
                services.AddRelayerSqlServerMaintenance("orders", ConnectionString, new SqlServerMaintenanceOptions
                {
                    ClaimDuration = duration
                });
            }
        }
    }
}
