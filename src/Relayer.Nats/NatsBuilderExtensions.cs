using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using Relayer.Transports;

namespace Relayer.Nats;

/// <summary>Registers owned native NATS connections and explicit JetStream topology.</summary>
public static class NatsBuilderExtensions
{
    /// <summary>Registers connection and topology management. Send and receive execution remain planned.</summary>
    public static IRelayerBuilder Nats(this IRelayerBuilder builder, string name,
        Action<TransportBuilder<NatsTransportOptions, NatsRouteOptions, NatsConsumerOptions>> configure)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        ArgumentNullException.ThrowIfNull(configure);
        builder.AddTransport(name, configure);
        builder.Services.AddSingleton<ITransport>(provider =>
        {
            var definition = provider.GetRequiredService<RelayerConfiguration>().Transports.Single(item => item.Name == name);
            if (definition.Routes.Count != 0 || definition.Consumers.Count != 0)
                throw new NotSupportedException($"NATS transport '{name}': send and receive execution are not implemented yet (#313). This registration manages native connections and JetStream topology only.");
            return new NatsNativeTransport(name, provider.GetRequiredService<IOptionsMonitor<NatsTransportOptions>>().Get(name));
        });
        builder.Services.AddSingleton<IHostedService>(provider => new NatsHostedService(provider, name));
        return builder;
    }
}

/// <summary>Reserved for native NATS send composition.</summary>
public sealed class NatsRouteOptions;

/// <summary>Reserved for native NATS receive composition.</summary>
public sealed class NatsConsumerOptions;

internal sealed class NatsHostedService(IServiceProvider provider, string name) : IHostedService
{
    private ITransport Transport => provider.GetServices<ITransport>().Single(transport => transport.Name == name);
    public Task StartAsync(CancellationToken cancellationToken) => Transport.StartAsync(cancellationToken);
    public Task StopAsync(CancellationToken cancellationToken) => Transport.StopAsync(cancellationToken);
}
