using System.Globalization;
using Dekaf;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Relayer.Kafka.Tests;

[ClassDataSource<KafkaHeaderSnapshotBroker>(Shared = SharedType.PerTestSession)]
public class KafkaStartupTests(KafkaHeaderSnapshotBroker broker)
{
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task The_same_host_rebuilds_a_rejected_native_producer_after_reload(bool concurrent)
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(2));
        var token = timeout.Token;
        await broker.Started.WaitAsync(token);
        var topic = TestContext.Current!.Isolation.GetIsolatedName("producer-reload").ToLowerInvariant();
        var builder = Host.CreateApplicationBuilder();
        builder.Logging.ClearProviders();
        builder.Configuration["Events:BootstrapServers:0"] = broker.Bootstrap;
        builder.Configuration["Producer:DeliveryTimeoutMs"] = "900";
        builder.Services.Configure<HostOptions>(options => options.ServicesStartConcurrently = concurrent);
        var builds = 0;
        builder.Services.AddRelayer(relayer =>
        {
            relayer.AddMessageTypesFromThisAssembly();
            relayer.Kafka("events", kafka =>
            {
                kafka.TopologyMode = TopologyMode.Provision;
                kafka.Options.Bind(builder.Configuration.GetSection("Events"));
                kafka.Options.Configure(options =>
                {
                    var deliveryTimeout = int.Parse(builder.Configuration["Producer:DeliveryTimeoutMs"]!, CultureInfo.InvariantCulture);
                    options.ConfigureProducer = producer =>
                    {
                        builds++;
                        producer.WithRequestTimeout(TimeSpan.FromMilliseconds(800))
                            .WithLinger(TimeSpan.FromMilliseconds(300))
                            .WithDeliveryTimeout(TimeSpan.FromMilliseconds(deliveryTimeout));
                    };
                });
                kafka.Send<Order>(topic);
            });
        });
        using var host = builder.Build();
        await Assert.That(() => host.StartAsync(token)).Throws<InvalidOperationException>()
            .WithMessageContaining("DeliveryTimeoutMs (900) must be greater than or equal to");
        await Assert.That(builds).IsEqualTo(1);

        builder.Configuration["Producer:DeliveryTimeoutMs"] = "1000";
        ((IConfigurationRoot)builder.Configuration).Reload();
        await Assert.That(() => host.StartAsync(token)).Throws<InvalidOperationException>()
            .WithMessageContaining("DeliveryTimeoutMs (1000) must be greater than or equal to");
        await Assert.That(builds).IsEqualTo(2);

        builder.Configuration["Producer:DeliveryTimeoutMs"] = "1500";
        ((IConfigurationRoot)builder.Configuration).Reload();
        await host.StartAsync(token);

        try
        {
            using var scope = host.Services.CreateScope();
            await scope.ServiceProvider.GetRequiredService<ISender>().SendAsync(new Order("reloaded", 1), token);
            await Assert.That(builds).IsEqualTo(3);
        }
        finally
        {
            await host.StopAsync(token);
        }
    }
}
