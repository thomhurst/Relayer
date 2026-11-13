using Relayer.Transports;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using OpenTelemetry;
using OpenTelemetry.Metrics;
using OpenTelemetry.Trace;
using Relayer.AzureServiceBus;
using Relayer.Kafka;

namespace Relayer.Aspire.Tests;

public class AspireRegistrationTests
{
    [Test]
    public async Task References_can_use_different_connection_and_transport_names_without_changing_native_options()
    {
        var configuration = Config(("kafka-resource", " first:9092, [::1]:9093 "), ("bus-resource", "native-connection"));
        var services = new ServiceCollection();
        Action<Dekaf.ConsumerBuilder<byte[], byte[]>> nativeConsumer = _ => { };
        var nativeOptions = new ServiceBusClientOptions { TransportType = ServiceBusTransportType.AmqpWebSockets };
        services.AddRelayer(relayer =>
        {
            relayer.Kafka("events", kafka =>
            {
                kafka.UseAspireConnection(configuration, "kafka-resource");
                kafka.Options.Configure(options => options.ConfigureConsumer = nativeConsumer);
            });
            relayer.ServiceBus("commands", bus =>
            {
                bus.UseAspireConnection(configuration, "bus-resource");
                bus.Options.Configure(options => options.ClientOptions = nativeOptions);
            });
        });
        await using var provider = services.BuildServiceProvider();
        var kafka = provider.GetRequiredService<IOptionsMonitor<KafkaTransportOptions>>().Get("events");
        await Assert.That(kafka.BootstrapServers.ToArray()).IsEquivalentTo(new[] { "first:9092", "[::1]:9093" });
        await Assert.That(kafka.ConfigureConsumer).IsSameReferenceAs(nativeConsumer);
        var bus = provider.GetRequiredService<IOptionsMonitor<AzureServiceBusTransportOptions>>().Get("commands");
        await Assert.That(bus.ConnectionString).IsEqualTo("native-connection");
        await Assert.That(bus.ClientOptions).IsSameReferenceAs(nativeOptions);
        await Assert.That(provider.GetRequiredService<IOptionsMonitor<KafkaTransportOptions>>().Get("other").BootstrapServers.Count).IsEqualTo(0);
    }

    [Test]
    public async Task Multiple_connections_and_later_native_configuration_remain_independent()
    {
        var services = new ServiceCollection();
        services.AddRelayer(relayer =>
        {
            relayer.Kafka("one", kafka => kafka.UseAspireConnection(Config(("one", "a:9092")), "one"));
            relayer.Kafka("two", kafka =>
            {
                kafka.UseAspireConnection(Config(("two", "b:9092")), "two");
                kafka.Options.Configure(options => options.BootstrapServers.Add("c:9092"));
            });
        });
        await using var provider = services.BuildServiceProvider();
        var options = provider.GetRequiredService<IOptionsMonitor<KafkaTransportOptions>>();
        await Assert.That(options.Get("one").BootstrapServers.ToArray()).IsEquivalentTo(new[] { "a:9092" });
        await Assert.That(options.Get("two").BootstrapServers.ToArray()).IsEquivalentTo(new[] { "b:9092", "c:9092" });
    }

    [Test]
    [Arguments(null)]
    [Arguments("")]
    [Arguments(" ")]
    public async Task Missing_connections_fail_at_registration_with_an_actionable_key(string? value)
    {
        var configuration = Config(("missing", value));
        await Assert.That(() => new ServiceCollection().AddRelayer(relayer => relayer.Kafka("events",
            kafka => kafka.UseAspireConnection(configuration, "missing"))))
            .Throws<InvalidOperationException>().WithMessageContaining("ConnectionStrings:missing");
        await Assert.That(() => new ServiceCollection().AddRelayer(relayer => relayer.ServiceBus("commands",
            bus => bus.UseAspireConnection(configuration, "missing"))))
            .Throws<InvalidOperationException>().WithMessageContaining("WithReference");
    }

    [Test]
    [Arguments("a:9092,")]
    [Arguments(",a:9092")]
    [Arguments("a:9092, ,b:9092")]
    public async Task Kafka_empty_entries_fail_before_creating_a_client(string value)
    {
        await Assert.That(() => new ServiceCollection().AddRelayer(relayer => relayer.Kafka("events",
            kafka => kafka.UseAspireConnection(Config(("broker", value)), "broker"))))
            .Throws<InvalidOperationException>().WithMessageContaining("without empty entries");
    }

    [Test]
    public async Task OpenTelemetry_observes_existing_source_and_named_meter_and_preserves_application_sampling()
    {
        var activities = new ConcurrentQueue<Activity>();
        var metrics = new ConcurrentQueue<string>();
        using var activityExporter = new ActivityCapture(activities);
        using var metricExporter = new MetricCapture(metrics);
        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddRelayer(_ => { });
        builder.Services.AddOpenTelemetry()
            .AddRelayerInstrumentation()
            .WithTracing(tracing => tracing.SetSampler(new AlwaysOnSampler())
                .AddProcessor(new SimpleActivityExportProcessor(activityExporter)))
            .WithMetrics(meter => meter.AddReader(new BaseExportingMetricReader(metricExporter)));
        using var host = builder.Build();
        await host.StartAsync();
        var source = host.Services.GetRequiredService<MessageTracing>().Source;
        using (var activity = source.StartActivity("aspire.registration.test"))
            await Assert.That(activity?.Recorded).IsTrue();
        using var meter = new Meter(RelayerTelemetry.MeterName);
        meter.CreateCounter<long>("aspire.registration.test.counter").Add(1);
        host.Services.GetRequiredService<MeterProvider>().ForceFlush();
        await Assert.That(activities.Any(activity => activity.OperationName == "aspire.registration.test")).IsTrue();
        await Assert.That(metrics.Contains("aspire.registration.test.counter")).IsTrue();
        await host.StopAsync();
    }

    private static IConfiguration Config(params (string Name, string? Value)[] connections) =>
        new ConfigurationBuilder().AddInMemoryCollection(connections.Select(connection =>
            new KeyValuePair<string, string?>("ConnectionStrings:" + connection.Name, connection.Value))).Build();

    private sealed class ActivityCapture(ConcurrentQueue<Activity> items) : BaseExporter<Activity>
    {
        public override ExportResult Export(in OpenTelemetry.Batch<Activity> batch)
        {
            foreach (var item in batch) items.Enqueue(item);
            return ExportResult.Success;
        }
    }

    private sealed class MetricCapture(ConcurrentQueue<string> items) : BaseExporter<Metric>
    {
        public override ExportResult Export(in OpenTelemetry.Batch<Metric> batch)
        {
            foreach (var item in batch) items.Enqueue(item.Name);
            return ExportResult.Success;
        }
    }
}
