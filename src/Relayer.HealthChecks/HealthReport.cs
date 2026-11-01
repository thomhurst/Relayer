using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace Relayer.HealthChecks;

// Only health polls allocate. No changes to the message pipeline are needed.
internal sealed class HealthReport
{
    private HealthStatus _status = HealthStatus.Healthy;
    private readonly List<string> _descriptions = [];
    private readonly Dictionary<string, object> _data = new(StringComparer.Ordinal);
    private Exception? _exception;

    public void Add(string resource, HealthCheckResult result)
    {
        if (result.Status < _status) _status = result.Status;
        _exception ??= result.Exception;
        _descriptions.Add($"{resource}: {result.Description ?? result.Status.ToString()}");
        // Keep each native result intact, including its data and exception.
        _data.Add(resource, result);
    }

    public HealthCheckResult Build(string emptyDescription) => new(_status,
        _descriptions.Count == 0 ? emptyDescription : string.Join("; ", _descriptions), _exception, _data);

    public static async ValueTask<HealthCheckResult> RunAsync(Func<CancellationToken, ValueTask<HealthCheckResult>> probe,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        try
        {
            var result = await probe(cancellationToken).ConfigureAwait(false);
            cancellationToken.ThrowIfCancellationRequested();
            return result;
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested) { throw; }
        catch (Exception error)
        {
            return HealthCheckResult.Unhealthy($"Probe failed: {error.Message}", error);
        }
    }
}
