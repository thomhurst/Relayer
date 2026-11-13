using Relayer.Transports;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Hosting;
using Relayer.HealthChecks;

namespace Relayer.RabbitMQ;

/// <summary>Registers optional native health adapters without adding health dependencies to the transport.</summary>
public static class RabbitMqHealthServiceCollectionExtensions
{
    /// <summary>Registers native read-only connectivity and receiver liveness under the exact RabbitMQ transport name.</summary>
    /// <remarks>Also call AddRelayerHealthChecks to register the checks. Call before starting the application host.</remarks>
    public static IServiceCollection AddRelayerRabbitMqHealthProviders(this IServiceCollection services, string name)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        services.TryAddKeyedSingleton<ITransportHealthProbe>(name, (provider, _) =>
            new RabbitMqHealthProvider((RabbitMqNativeTransport)provider.GetServices<Relayer.Transports.ITransport>().Single(transport => transport.Name == name)));
        services.TryAddKeyedSingleton<IConsumerHealthProvider>(name, (provider, _) =>
            new RabbitMqHealthProvider((RabbitMqNativeTransport)provider.GetServices<Relayer.Transports.ITransport>().Single(transport => transport.Name == name)));
        if (!services.Any(descriptor => descriptor.ServiceType == typeof(HealthRegistration) && Equals(descriptor.ServiceKey, name)))
        {
            services.AddKeyedSingleton(name, new HealthRegistration());
            services.AddSingleton<IHostedService>(provider => new RabbitMqHealthStartup(provider, name));
        }
        return services;
    }

    private sealed class HealthRegistration;

    private sealed class RabbitMqHealthStartup(IServiceProvider provider, string name) : IHostedLifecycleService
    {
        public Task StartingAsync(CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var transport = (RabbitMqNativeTransport)provider.GetServices<Relayer.Transports.ITransport>().Single(transport => transport.Name == name);
            transport.HealthTracking = true;
            return Task.CompletedTask;
        }
        public Task StartAsync(CancellationToken cancellationToken) => Task.CompletedTask;
        public Task StartedAsync(CancellationToken cancellationToken) => Task.CompletedTask;
        public Task StoppingAsync(CancellationToken cancellationToken) => Task.CompletedTask;
        public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
        public Task StoppedAsync(CancellationToken cancellationToken) => Task.CompletedTask;
    }

    private sealed class RabbitMqHealthProvider(RabbitMqNativeTransport transport) : ITransportHealthProbe, IConsumerHealthProvider
    {
        public ValueTask<HealthCheckResult> CheckAsync(CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return ValueTask.FromResult(transport.IsConnected
                ? HealthCheckResult.Healthy("Native RabbitMQ connection is open.")
                : HealthCheckResult.Unhealthy("Native RabbitMQ connection is closed or recovering."));
        }

        public ValueTask<ConsumerHealthState?> GetStateAsync(IConsumerDefinition consumer, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            foreach (var receiver in transport.Consumers)
            {
                if (!ReferenceEquals(receiver.Declaration, consumer)) continue;
                var state = receiver.GetHealthState();
                return ValueTask.FromResult<ConsumerHealthState?>(state is { } value ? new(value.IsIdle, value.LastProgressAt) : null);
            }
            return ValueTask.FromResult<ConsumerHealthState?>(null);
        }
    }
}
