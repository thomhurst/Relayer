using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using Relayer.Transports;

namespace Relayer.RabbitMQ;

/// <summary>Registers native RabbitMQ connection and topology management.</summary>
public static class RabbitMqBuilderExtensions
{
    /// <summary>Registers a named RabbitMQ connection. Send and receive execution remain planned.</summary>
    public static IRelayerBuilder RabbitMQ(this IRelayerBuilder builder, string name,
        Action<TransportBuilder<RabbitMqTransportOptions, RabbitMqRouteOptions, RabbitMqConsumerOptions>> configure)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        ArgumentNullException.ThrowIfNull(configure);
        builder.AddTransport<RabbitMqTransportOptions, RabbitMqRouteOptions, RabbitMqConsumerOptions>(name, transport =>
        {
            transport.SendDefaults = new() { HeaderNames = new("cloudEvents:") };
            configure(transport);
        });
        builder.Services.AddSingleton<ITransport>(provider =>
        {
            var definition = provider.GetRequiredService<RelayerConfiguration>().Transports.Single(item => item.Name == name);
            if (definition.Routes.Count != 0 || definition.Consumers.Count != 0)
                throw new NotSupportedException($"RabbitMQ transport '{name}': send and receive execution are not implemented yet. This registration manages native connections and topology only.");
            return new RabbitMqNativeTransport(name,
                provider.GetRequiredService<IOptionsMonitor<RabbitMqTransportOptions>>().Get(name));
        });
        builder.Services.AddSingleton<IHostedService>(provider => new RabbitMqHostedService(provider, name));
        return builder;
    }
}

/// <summary>Reserved for native RabbitMQ send composition.</summary>
public sealed class RabbitMqRouteOptions;

/// <summary>Reserved for native RabbitMQ receive composition.</summary>
public sealed class RabbitMqConsumerOptions;

internal sealed class RabbitMqHostedService(IServiceProvider provider, string name) : IHostedService
{
    private ITransport Transport => provider.GetServices<ITransport>().Single(transport => transport.Name == name);
    public Task StartAsync(CancellationToken cancellationToken) => Transport.StartAsync(cancellationToken);
    public Task StopAsync(CancellationToken cancellationToken) => Transport.StopAsync(cancellationToken);
}
