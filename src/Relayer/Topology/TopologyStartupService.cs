using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using Relayer.Transports;

namespace Relayer;

internal sealed class TopologyStartupService(RelayerConfiguration configuration, IServiceProvider services) : IHostedService
{
    public async Task StartAsync(CancellationToken cancellationToken)
    {
        // Configuration validation runs first and rejects duplicate or missing runtime adapters.
        var adapters = services.GetServices<ITransport>().ToDictionary(transport => transport.Name, StringComparer.Ordinal);
        var failures = new List<string>();
        foreach (var declaration in configuration.Transports)
        {
            var mode = declaration.TopologyMode ?? configuration.TopologyMode;
            if (mode == TopologyMode.None || !adapters.TryGetValue(declaration.Name, out var adapter)) continue;
            cancellationToken.ThrowIfCancellationRequested();
            try
            {
                var report = await adapter.ApplyTopologyAsync(mode, cancellationToken).ConfigureAwait(false)
                    ?? throw new InvalidOperationException("ApplyTopologyAsync must return a TopologyReport.");
                foreach (var difference in report.Differences)
                    failures.Add($"Transport '{declaration.Name}' ({mode}): {difference}");
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested) { throw; }
            catch (Exception error)
            {
                failures.Add($"Transport '{declaration.Name}' ({mode}): {error.Message}");
            }
        }
        if (failures.Count != 0)
            throw new OptionsValidationException(Options.DefaultName, typeof(RelayerConfiguration), failures);
    }

    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
}
