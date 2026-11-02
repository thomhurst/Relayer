using Dekaf.Admin;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Relayer.HealthChecks;

namespace Relayer.Kafka;

/// <summary>Connects named Kafka transports to the optional Relayer health checks.</summary>
public static class KafkaHealthServiceCollectionExtensions
{
    /// <summary>Registers native connectivity and receiver-owned liveness under the exact transport name.</summary>
    /// <remarks>
    /// Also call AddRelayerHealthChecks. The admin client is reused until service-provider disposal.
    /// Configure its native security settings explicitly; producer and consumer callbacks do not configure it.
    /// Repeated calls preserve existing adapters for this name.
    /// </remarks>
    public static IServiceCollection AddRelayerKafkaHealthProviders(this IServiceCollection services, string transportName,
        Action<AdminClientBuilder>? configureAdmin = null)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentException.ThrowIfNullOrWhiteSpace(transportName);
        services.TryAddSingleton(TimeProvider.System);
        services.TryAddKeyedSingleton<KafkaConsumerHealth>(transportName, static (provider, _) =>
            new(provider.GetRequiredService<TimeProvider>()));
        services.TryAddKeyedSingleton<IConsumerHealthProvider>(transportName, static (provider, key) =>
            new KafkaConsumerHealthProvider(provider.GetRequiredKeyedService<KafkaConsumerHealth>(key)));
        services.TryAddKeyedSingleton<ITransportHealthProbe>(transportName, (provider, _) =>
        {
            var declaration = provider.GetRequiredService<RelayerConfiguration>().Transports
                .SingleOrDefault(item => item.Name == transportName);
            if (declaration?.OptionsType != typeof(KafkaTransportOptions))
                throw new InvalidOperationException($"Kafka health: no Kafka transport named '{transportName}' is declared. Use the exact case-sensitive transport name.");
            var options = provider.GetRequiredService<IOptionsMonitor<KafkaTransportOptions>>().Get(transportName);
            var native = new AdminClientBuilder().WithBootstrapServers(options.BootstrapServers.ToArray())
                .WithLoggerFactory(provider.GetService<ILoggerFactory>() ?? NullLoggerFactory.Instance);
            configureAdmin?.Invoke(native);
            return new KafkaConnectivityHealthProbe(native.Build());
        });
        return services;
    }
}

internal sealed class KafkaConsumerHealthProvider(KafkaConsumerHealth health) : IConsumerHealthProvider
{
    public ValueTask<ConsumerHealthState?> GetStateAsync(IConsumerDefinition consumer, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var state = health.GetState(consumer);
        return new(state is { } value ? new ConsumerHealthState(value.IsIdle, value.LastProgressAt) : null);
    }
}
