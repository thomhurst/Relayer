using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using RabbitMQ.Client;
using Relayer.Transports;

namespace Relayer.RabbitMQ;

/// <summary>Registers native RabbitMQ connections, topology, confirmed sends and durable consumers.</summary>
public static class RabbitMqBuilderExtensions
{
    /// <summary>Configures native basic properties for one send. The object is borrowed until send completion.</summary>
    public static SendOptions RabbitMQ(this SendOptions options, Action<BasicProperties> configure) => options.ConfigureNative(configure);

    /// <summary>Registers a named RabbitMQ connection and explicitly declared consumers.</summary>
    public static IRelayerBuilder RabbitMQ(this IRelayerBuilder builder, string name,
        Action<TransportBuilder<RabbitMqTransportOptions, RabbitMqConsumerOptions>> configure)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        ArgumentNullException.ThrowIfNull(configure);
        builder.AddTransport<RabbitMqTransportOptions, RabbitMqConsumerOptions>(name, transport =>
        {
            transport.SendDefaults = new() { HeaderNames = new("cloudEvents:") };
            configure(transport);
        });
        builder.Services.AddSingleton<ITransport>(provider =>
        {
            var definition = provider.GetRequiredService<RelayerConfiguration>().Transports.Single(item => item.Name == name);
            foreach (var declaration in definition.Consumers)
                RabbitMqConsumer.Validate((ConsumerDefinition<RabbitMqConsumerOptions>)declaration);
            return new RabbitMqNativeTransport(name,
                provider.GetRequiredService<IOptionsMonitor<RabbitMqTransportOptions>>().Get(name),
                definition.Routes.Select(route => route.Destination).Concat(definition.Consumers.SelectMany(declaration =>
                    new[] { declaration.Source, declaration.Failure.DeadLetterEndpoint ?? ":" + declaration.Source + ".dead-letter" })), provider.GetRequiredService<MessageTracing>(), provider.GetRequiredService<RelayerInstrumentation>());
        });
        builder.Services.AddSingleton<IHostedService>(provider => new RabbitMqHostedService(provider, name));
        return builder;
    }
}

/// <summary>Native consumer channel, QoS and failure configuration.</summary>
public sealed class RabbitMqConsumerOptions
{
    /// <summary>Native channel options. ConsumerDispatchConcurrency must match Concurrency.</summary>
    public CreateChannelOptions ChannelOptions { get; set; } = new(false, false, consumerDispatchConcurrency: 1);
    /// <summary>Native per-consumer prefetch count; zero means unlimited.</summary>
    public ushort PrefetchCount { get; set; } = 1;
    /// <summary>Native exclusive-consumer flag.</summary>
    public bool Exclusive { get; set; }
    /// <summary>Native consume arguments passed unchanged.</summary>
    public IDictionary<string, object?>? Arguments { get; set; }
}
