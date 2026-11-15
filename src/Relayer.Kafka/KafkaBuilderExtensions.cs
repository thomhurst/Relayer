using Dekaf;
using Dekaf.Consumer;
using Dekaf.Producer;
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
    /// <summary>Customizes native Kafka headers immediately before single or batch sending.</summary>
    public static SendOptions Kafka(this SendOptions options, Action<Headers> configure) =>
        options.ConfigureNative(configure);

    /// <summary>Transforms Dekaf's immutable record immediately before sending, including partition and timestamp.</summary>
    /// <remarks>The record and its memory are borrowed until send completion. Do not retain them. Cannot be persisted in an outbox.</remarks>
    public static SendOptions KafkaRecord(this SendOptions options,
        Func<ProducerMessage<WireString?, (ReadOnlyMemory<byte> Body, Headers Headers)?>,
            ProducerMessage<WireString?, (ReadOnlyMemory<byte> Body, Headers Headers)?>> configure) =>
        options.TransformNative(configure);

    /// <summary>Declares Kafka send routes and key-ordered parallel consumers with durable dead-letter copies.</summary>
    public static IRelayerBuilder Kafka(this IRelayerBuilder builder, string name,
        Action<TransportBuilder<KafkaTransportOptions, KafkaConsumerOptions>> configure)
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
                if (consumer.BatchSize != 1)
                    throw new NotSupportedException($"Kafka consumer '{consumer.Source}': use single-message handlers. Kafka batch handlers are not implemented yet.");
                if (consumer.Options.Processing is { } processing) KafkaProcessing.Validate(consumer.Source, processing, consumer.Concurrency);
                if (consumer.Failure.RedeliveryDelays.Length != 0)
                    throw new NotSupportedException($"Kafka consumer '{consumer.Source}': durable redelivery cannot yet persist native tombstones and null-valued headers, even with IFailureScheduler. Use RetryDelays for in-process retries and dead-letter copies instead.");
                consumer.Options.CircuitBreaker?.Validate();
                endpoints.Add(consumer.Source);
                endpoints.Add(consumer.Failure.DeadLetterEndpoint ?? consumer.Source + ".dead-letter");
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
                provider.GetRequiredService<MessageTracing>(), loggerFactory, options);
        });
        builder.Services.AddKeyedSingleton<IDeadLetterManager>(name, (provider, _) =>
        {
            var definition = provider.GetRequiredService<RelayerConfiguration>().Transports.Single(item => item.Name == name);
            return new KafkaDeadLetterManager(name, provider.GetRequiredService<IOptionsMonitor<KafkaTransportOptions>>().Get(name), definition.Consumers);
        });
        builder.Services.AddSingleton<ITransport>(provider => provider.GetRequiredKeyedService<KafkaProducerTransport>(name));
        builder.Services.AddKeyedSingleton<IKafkaConsumerControl>(name, (provider, _) => new KafkaConsumerControl(provider, name));
        builder.Services.AddKeyedSingleton<KafkaHostedService>(name, (provider, _) => new KafkaHostedService(provider, name));
        builder.Services.AddKeyedSingleton<IConsumerControl>(name, (provider, key) => provider.GetRequiredKeyedService<KafkaHostedService>(key));
        builder.Services.AddSingleton<IHostedService>(provider => provider.GetRequiredKeyedService<KafkaHostedService>(name));
        return builder;
    }
}

/// <summary>Kafka group identity and the common durable failure policy.</summary>
public sealed class KafkaConsumerOptions
{
    /// <summary>The required native Kafka consumer group.</summary>
    public string? Group { get; set; }
    /// <summary>Optional failure-rate protection. Disabled when null.</summary>
    /// <remarks>While the circuit is open, rejected records wait in their key lane and are retried when it admits a probe.
    /// Native backpressure pauses their partitions once buffers fill. They are never marked processed while waiting.</remarks>
    public ConsumerCircuitOptions? CircuitBreaker { get; set; }

    /// <summary>Dekaf's partitioned runtime options, passed through unchanged.</summary>
    /// <remarks>
    /// When null, Relayer uses key ordering, <c>MaxConcurrentHandlersPerPartition</c> equal to <c>Concurrency</c>, periodic
    /// completed-offset commits and Dekaf's other defaults. <c>Concurrency</c> always sets the native consumer-wide
    /// <c>MaxConcurrentHandlers</c>. Relayer supplies <c>Monitor</c>, <c>Control</c> and <c>CommitObserver</c>, requires a
    /// runtime-managed <c>CommitPolicy</c>, <c>ErrorPolicy</c> <c>StopConsumer</c> and single-record handlers.
    /// </remarks>
    public PartitionedProcessingOptions? Processing { get; set; }
}
