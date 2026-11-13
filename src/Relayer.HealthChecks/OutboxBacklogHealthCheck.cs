using Relayer.Transports;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Options;

namespace Relayer.HealthChecks;

internal sealed class OutboxBacklogHealthCheck(IServiceProvider services, IOptionsMonitor<RelayerHealthCheckOptions> options) : IHealthCheck
{
    public async Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var settings = options.CurrentValue;
        var report = new HealthReport();
        if (settings.OutboxNames.Count == 0 &&
            services.GetService<IServiceProviderIsService>()?.IsService(typeof(IOutboxEnlistment)) == true)
            return HealthCheckResult.Unhealthy("An IOutboxEnlistment is registered. Declare every expected store in OutboxNames.");
        foreach (var registration in services.GetServices<OutboxHealthRegistration>())
        {
            if (!settings.OutboxNames.Contains(registration.StoreName, StringComparer.Ordinal))
                report.Add(registration.StoreName, HealthCheckResult.Unhealthy(
                    "An outbox health adapter is registered. Declare this store in OutboxNames."));
        }
        foreach (var name in settings.OutboxNames)
        {
            var result = await HealthReport.RunAsync(async token =>
            {
                var provider = services.GetKeyedService<IOutboxHealthProvider>(name);
                if (provider is null) return HealthCheckResult.Unhealthy("No keyed IOutboxHealthProvider is registered.");
                var countLimit = settings.OutboxUnhealthyThreshold == long.MaxValue
                    ? long.MaxValue : settings.OutboxUnhealthyThreshold + 1;
                var count = await provider.GetPendingCountAsync(countLimit, token).ConfigureAwait(false);
                if (count is null) return HealthCheckResult.Unhealthy("Outbox backlog is unavailable.");
                if (count < 0) return HealthCheckResult.Unhealthy("Outbox backlog must not be negative.");
                var status = HealthStatus.Healthy;
                if (count >= settings.OutboxUnhealthyThreshold) status = HealthStatus.Unhealthy;
                else if (count >= settings.OutboxDegradedThreshold) status = HealthStatus.Degraded;
                var lowerBound = count >= countLimit;
                return new HealthCheckResult(status, $"{(lowerBound ? "At least " : "")}{count} pending rows eligible for dispatch.",
                    data: new Dictionary<string, object>
                    {
                        ["pending_count"] = count.Value,
                        ["pending_count_is_lower_bound"] = lowerBound
                    });
            }, cancellationToken).ConfigureAwait(false);
            report.Add(name, result);
        }
        return report.Build("No outboxes configured. Declare expected stores in OutboxNames.");
    }
}
