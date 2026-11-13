using Relayer.Transports;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Options;

namespace Relayer.HealthChecks;

internal sealed class ConsumerLivenessHealthCheck(RelayerConfiguration configuration, IServiceProvider services,
    IOptionsMonitor<RelayerHealthCheckOptions> options, TimeProvider clock) : IHealthCheck
{
    public async Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var timeout = options.CurrentValue.ConsumerStallTimeout;
        var report = new HealthReport();
        foreach (var transport in configuration.Transports)
        {
            for (var index = 0; index < transport.Consumers.Count; index++)
            {
                var consumer = transport.Consumers[index];
                var result = await HealthReport.RunAsync(async token =>
                {
                    var provider = services.GetKeyedService<IConsumerHealthProvider>(transport.Name);
                    if (provider is null) return HealthCheckResult.Unhealthy("No keyed IConsumerHealthProvider is registered.");
                    var state = await provider.GetStateAsync(consumer, token).ConfigureAwait(false);
                    if (state is null) return HealthCheckResult.Unhealthy("Consumer state is unavailable.");
                    if (state.Value.IsIdle) return HealthCheckResult.Healthy("Consumer is idle.");
                    var elapsed = clock.GetUtcNow() - state.Value.LastProgressAt;
                    if (elapsed < TimeSpan.Zero) return HealthCheckResult.Unhealthy("Consumer progress timestamp is in the future.");
                    return elapsed >= timeout
                        ? HealthCheckResult.Unhealthy($"Active consumer has made no progress for {elapsed}; threshold is {timeout}.")
                        : HealthCheckResult.Healthy("Active consumer is making progress.");
                }, cancellationToken).ConfigureAwait(false);
                report.Add($"{transport.Name}/{index}:{consumer.Source}", result);
            }
        }
        return report.Build("No consumers configured.");
    }
}
