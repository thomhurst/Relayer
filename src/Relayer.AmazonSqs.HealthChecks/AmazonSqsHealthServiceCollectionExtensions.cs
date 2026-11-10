using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Hosting;
using Relayer.HealthChecks;

namespace Relayer.AmazonSqs;

/// <summary>Registers optional native health adapters without adding health dependencies to the transport.</summary>
public static class AmazonSqsHealthServiceCollectionExtensions
{
    /// <summary>Registers native read-only connectivity and receiver liveness under the exact SQS transport name.</summary>
    /// <remarks>Also call AddRelayerHealthChecks to register the checks. Call before starting the application host.</remarks>
    public static IServiceCollection AddRelayerAmazonSqsHealthProviders(this IServiceCollection services, string name)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        services.TryAddKeyedSingleton<ITransportHealthProbe>(name, (provider, _) =>
            new AmazonSqsHealthProvider(provider.GetRequiredKeyedService<SqsSendTransport>(name)));
        services.TryAddKeyedSingleton<IConsumerHealthProvider>(name, (provider, _) =>
            new AmazonSqsHealthProvider(provider.GetRequiredKeyedService<SqsSendTransport>(name)));
        if (!services.Any(descriptor => descriptor.ServiceType == typeof(HealthRegistration) && Equals(descriptor.ServiceKey, name)))
        {
            services.AddKeyedSingleton(name, new HealthRegistration());
            services.AddSingleton<IHostedService>(provider => new AmazonSqsHealthStartup(provider, name));
        }
        return services;
    }

    private sealed class HealthRegistration;

    private sealed class AmazonSqsHealthStartup(IServiceProvider provider, string name) : IHostedLifecycleService
    {
        public Task StartingAsync(CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            provider.GetRequiredKeyedService<SqsSendTransport>(name).EnableHealthTracking();
            return Task.CompletedTask;
        }
        public Task StartAsync(CancellationToken cancellationToken) => Task.CompletedTask;
        public Task StartedAsync(CancellationToken cancellationToken) => Task.CompletedTask;
        public Task StoppingAsync(CancellationToken cancellationToken) => Task.CompletedTask;
        public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
        public Task StoppedAsync(CancellationToken cancellationToken) => Task.CompletedTask;
    }

    private sealed class AmazonSqsHealthProvider(SqsSendTransport transport) : ITransportHealthProbe, IConsumerHealthProvider
    {
        public async ValueTask<HealthCheckResult> CheckAsync(CancellationToken cancellationToken)
        {
            try
            {
                await transport.CheckConnectivityAsync(cancellationToken).ConfigureAwait(false);
                cancellationToken.ThrowIfCancellationRequested();
                return HealthCheckResult.Healthy("Native SQS queues are accessible.");
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested) { throw; }
            catch (Exception error) { return HealthCheckResult.Unhealthy("Native SQS connectivity failed: " + error.Message, error); }
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
