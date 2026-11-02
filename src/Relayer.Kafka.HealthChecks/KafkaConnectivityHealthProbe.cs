using Dekaf.Admin;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Relayer.HealthChecks;

namespace Relayer.Kafka;

internal sealed class KafkaConnectivityHealthProbe(IAdminClient native) : ITransportHealthProbe, IAsyncDisposable
{
    public async ValueTask<HealthCheckResult> CheckAsync(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        try
        {
            // The no-options overload uses cached metadata and cannot detect a disconnected broker.
            var cluster = await native.DescribeClusterAsync(new DescribeClusterOptions(), cancellationToken).ConfigureAwait(false);
            return cluster.Nodes.Count == 0
                ? HealthCheckResult.Unhealthy("Kafka returned no cluster nodes.")
                : HealthCheckResult.Healthy("Kafka live cluster discovery succeeded.",
                    new Dictionary<string, object> { ["cluster_id"] = cluster.ClusterId, ["node_count"] = cluster.Nodes.Count });
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested) { throw; }
        catch (Exception error) { return HealthCheckResult.Unhealthy($"Kafka live cluster discovery failed: {error.Message}", error); }
    }

    public ValueTask DisposeAsync() => native.DisposeAsync();
}
