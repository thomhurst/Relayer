using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using RabbitMQ.Client;
using Relayer.Transports;

namespace Relayer.RabbitMQ;

/// <summary>Registers native RabbitMQ connections, topology and confirmed outgoing sends.</summary>
public static class RabbitMqBuilderExtensions
{
    /// <summary>Configures native basic properties for one send. The object is borrowed until send completion.</summary>
    public static SendOptions RabbitMQ(this SendOptions options, Action<BasicProperties> configure) => options.ConfigureNative(configure);

    /// <summary>Registers a named RabbitMQ connection. Receive execution remains planned.</summary>
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
            if (definition.Consumers.Count != 0)
                throw new NotSupportedException($"RabbitMQ transport '{name}': receive execution is not implemented yet.");
            return new RabbitMqNativeTransport(name,
                provider.GetRequiredService<IOptionsMonitor<RabbitMqTransportOptions>>().Get(name),
                definition.Routes.Select(route => route.Destination), provider.GetRequiredService<MessageTracing>(), provider.GetRequiredService<RelayerInstrumentation>());
        });
        builder.Services.AddSingleton<IHostedService>(provider => new RabbitMqHostedService(provider, name));
        return builder;
    }
}

/// <summary>RabbitMQ route configuration. Destinations select the native exchange and routing key.</summary>
public sealed class RabbitMqRouteOptions;

/// <summary>Reserved for native RabbitMQ receive composition.</summary>
public sealed class RabbitMqConsumerOptions;

internal sealed class RabbitMqHostedService(IServiceProvider provider, string name) : IHostedService
{
    private ITransport Transport => provider.GetServices<ITransport>().Single(transport => transport.Name == name);
    public Task StartAsync(CancellationToken cancellationToken) => Transport.StartAsync(cancellationToken);
    public Task StopAsync(CancellationToken cancellationToken) => Transport.StopAsync(cancellationToken);
}
