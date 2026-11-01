using Dekaf;
using Dekaf.Serialization;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Relayer.Transports;

namespace Relayer.Kafka;

/// <summary>Registers named Kafka transports backed by Dekaf.</summary>
public static class KafkaBuilderExtensions
{
    /// <summary>Declares Kafka send routes. Consumer execution is not implemented yet.</summary>
    public static IRelayerBuilder Kafka(this IRelayerBuilder builder, string name,
        Action<TransportBuilder<KafkaTransportOptions, KafkaRouteOptions, KafkaConsumerOptions>> configure)
    {
        ArgumentNullException.ThrowIfNull(builder);
        builder.AddTransport(name, configure);
        builder.Services.AddSingleton<ITransport>(provider =>
        {
            var definition = provider.GetRequiredService<RelayerConfiguration>().Transports.Single(item => item.Name == name);
            if (definition.Consumers.Count != 0)
                throw new NotSupportedException($"Kafka transport '{name}': consumer execution is not implemented yet.");
            var options = provider.GetRequiredService<IOptionsMonitor<KafkaTransportOptions>>().Get(name);
            var loggerFactory = provider.GetService<ILoggerFactory>() ?? NullLoggerFactory.Instance;
            var native = new ProducerBuilder<WireString?, (ReadOnlyMemory<byte> Body, Headers Headers)>()
                .WithBootstrapServers(options.BootstrapServers.ToArray())
                .WithKeySerializer(new KafkaKeySerializer())
                .WithValueSerializer(new KafkaValueSerializer())
                .WithLoggerFactory(loggerFactory);
            options.ConfigureProducer?.Invoke(native);
            return new KafkaProducerTransport(name, native.Build(), definition.Routes.Select(route => route.Destination),
                provider.GetRequiredService<MessageTracing>(), loggerFactory);
        });
        builder.Services.AddSingleton<IHostedService>(provider => new KafkaProducerHostedService(provider, name));
        return builder;
    }
}

/// <summary>Kafka route configuration. Ordering keys use the common route builder.</summary>
public sealed class KafkaRouteOptions;

/// <summary>Reserved Kafka consumer configuration. Consumer execution is not implemented yet.</summary>
public sealed class KafkaConsumerOptions;

internal sealed class KafkaProducerHostedService(IServiceProvider provider, string name) : IHostedService
{
    private ITransport Transport => provider.GetServices<ITransport>().Single(transport => transport.Name == name);
    public Task StartAsync(CancellationToken cancellationToken) => Transport.StartAsync(cancellationToken);
    public Task StopAsync(CancellationToken cancellationToken) => Transport.StopAsync(cancellationToken);
}
