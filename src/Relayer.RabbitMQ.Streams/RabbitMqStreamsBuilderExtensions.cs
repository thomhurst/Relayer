using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using Relayer.Transports;

namespace Relayer.RabbitMQ.Streams;

/// <summary>Registers owned RabbitMQ Stream systems and explicit stream topology.</summary>
public static class RabbitMqStreamsBuilderExtensions
{
    /// <summary>Registers native connection and topology management. Message execution remains planned under #316.</summary>
    public static IRelayerBuilder RabbitMQStreams(this IRelayerBuilder builder, string name,
        Action<TransportBuilder<RabbitMqStreamsOptions, RabbitMqStreamsConsumerOptions>> configure)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        ArgumentNullException.ThrowIfNull(configure);
        builder.AddTransport(name, configure);
        builder.Services.AddSingleton<ITransport>(provider =>
        {
            var definition = provider.GetRequiredService<RelayerConfiguration>().Transports.Single(item => item.Name == name);
            if (definition.Routes.Count != 0 || definition.Consumers.Count != 0)
                throw new NotSupportedException("RabbitMQ Streams send and receive execution are not implemented (#316). This registration manages native connections and stream topology only; reliable consumer recovery cannot yet preserve failed processing offsets.");
            return new RabbitMqStreamsTransport(name, provider.GetRequiredService<IOptionsMonitor<RabbitMqStreamsOptions>>().Get(name));
        });
        builder.Services.AddSingleton<IHostedService>(provider => new RabbitMqStreamsHostedService(provider, name));
        return builder;
    }
}

/// <summary>Reserved for native RabbitMQ Streams receive composition.</summary>
public sealed class RabbitMqStreamsConsumerOptions;

internal sealed class RabbitMqStreamsHostedService(IServiceProvider provider, string name) : IHostedService
{
    private ITransport Transport => provider.GetServices<ITransport>().Single(transport => transport.Name == name);
    public Task StartAsync(CancellationToken cancellationToken) => Transport.StartAsync(cancellationToken);
    public Task StopAsync(CancellationToken cancellationToken) => Transport.StopAsync(cancellationToken);
}
