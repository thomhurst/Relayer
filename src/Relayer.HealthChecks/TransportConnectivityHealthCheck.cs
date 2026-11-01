using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace Relayer.HealthChecks;

internal sealed class TransportConnectivityHealthCheck(RelayerConfiguration configuration, IServiceProvider services) : IHealthCheck
{
    public async Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var report = new HealthReport();
        foreach (var transport in configuration.Transports)
        {
            var result = await HealthReport.RunAsync(token =>
            {
                var probe = services.GetKeyedService<ITransportHealthProbe>(transport.Name);
                return probe is null
                    ? ValueTask.FromResult(HealthCheckResult.Unhealthy("No keyed ITransportHealthProbe is registered."))
                    : probe.CheckAsync(token);
            }, cancellationToken).ConfigureAwait(false);
            report.Add(transport.Name, result);
        }
        return report.Build("No transports configured.");
    }
}
