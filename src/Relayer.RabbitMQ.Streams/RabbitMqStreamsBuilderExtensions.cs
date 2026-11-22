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
            {
                throw new NotSupportedException("RabbitMQ Streams registration supports native connections and stream topology only. Remove send routes and consumers from this registration. Message execution (#316) awaits graceful shutdown in the native reliable consumer (rabbitmq/rabbitmq-stream-dotnet-client#498).");
            }

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

    public Task StartAsync(CancellationToken cancellationToken)
    {
        return Transport.StartAsync(cancellationToken);
    }

    public Task StopAsync(CancellationToken cancellationToken)
    {
        return Transport.StopAsync(cancellationToken);
    }
}
