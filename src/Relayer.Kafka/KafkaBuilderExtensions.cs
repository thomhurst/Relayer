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
    /// <summary>Declares Kafka send routes and serial consumers with durable dead-letter copies.</summary>
    public static IRelayerBuilder Kafka(this IRelayerBuilder builder, string name,
        Action<TransportBuilder<KafkaTransportOptions, KafkaRouteOptions, KafkaConsumerOptions>> configure)
    {
        ArgumentNullException.ThrowIfNull(builder);
        builder.AddTransport(name, configure);
        builder.Services.AddKeyedSingleton<KafkaProducerTransport>(name, (provider, _) =>
        {
            var definition = provider.GetRequiredService<RelayerConfiguration>().Transports.Single(item => item.Name == name);
            var endpoints = new HashSet<string>(definition.Routes.Select(route => route.Destination), StringComparer.Ordinal);
            foreach (var declaration in definition.Consumers)
            {
                var consumer = (ConsumerDefinition<KafkaConsumerOptions>)declaration;
                if (string.IsNullOrWhiteSpace(consumer.Options.Group))
                    throw new InvalidOperationException($"Kafka consumer '{consumer.Source}': set Options.Group to a consumer group name.");
                if (consumer.Concurrency != 1 || consumer.BatchSize != 1)
                    throw new NotSupportedException($"Kafka consumer '{consumer.Source}': use Concurrency(1) and single-message handlers. Parallel partition scheduling and batch handlers are not implemented yet (#22).");
                if (!consumer.Options.Failure.RedeliveryDelays.IsEmpty)
                    throw new NotSupportedException($"Kafka consumer '{consumer.Source}': durable redelivery cannot yet persist native tombstones and null-valued headers, even with IFailureScheduler. Use RetryDelays for in-process retries and dead-letter copies instead.");
                endpoints.Add(consumer.Source);
                endpoints.Add(consumer.Options.Failure.DeadLetterEndpoint ?? consumer.Source + ".dead-letter");
            }
            var options = provider.GetRequiredService<IOptionsMonitor<KafkaTransportOptions>>().Get(name);
            var loggerFactory = provider.GetService<ILoggerFactory>() ?? NullLoggerFactory.Instance;
            var native = new ProducerBuilder<WireString?, (ReadOnlyMemory<byte> Body, Headers Headers)?>()
                .WithBootstrapServers(options.BootstrapServers.ToArray())
                .WithKeySerializer(new KafkaKeySerializer())
                .WithValueSerializer(new KafkaValueSerializer())
                .WithLoggerFactory(loggerFactory);
            options.ConfigureProducer?.Invoke(native);
            return new KafkaProducerTransport(name, native.Build(), endpoints,
                provider.GetRequiredService<MessageTracing>(), loggerFactory);
        });
        builder.Services.AddSingleton<ITransport>(provider => provider.GetRequiredKeyedService<KafkaProducerTransport>(name));
        builder.Services.AddKeyedSingleton<IKafkaConsumerControl>(name, (provider, _) => new KafkaConsumerControl(provider, name));
        builder.Services.AddSingleton<IHostedService>(provider => new KafkaHostedService(provider, name));
        return builder;
    }
}

/// <summary>Kafka route configuration. Ordering keys use the common route builder.</summary>
public sealed class KafkaRouteOptions;

/// <summary>Kafka group identity and the common durable failure policy.</summary>
public sealed class KafkaConsumerOptions
{
    /// <summary>The required native Kafka consumer group.</summary>
    public string? Group { get; set; }
    /// <summary>Core retry and dead-letter policy. The default dead-letter topic is source + ".dead-letter".</summary>
    public FailureOptions Failure { get; set; } = new();
}
