namespace Relayer;

/// <summary>Remaining differences after a native topology validation or provisioning operation.</summary>
public sealed class TopologyReport
{
    /// <summary>Creates a report. An empty diff means all declared entities match.</summary>
    public TopologyReport(IEnumerable<string> differences)
    {
        ArgumentNullException.ThrowIfNull(differences);
        var snapshot = differences.ToArray();
        foreach (var difference in snapshot) ArgumentException.ThrowIfNullOrWhiteSpace(difference);
        Differences = Array.AsReadOnly(snapshot);
    }

    /// <summary>Actionable, native entity differences; provisioning must report anything it could not reconcile.</summary>
    public IReadOnlyList<string> Differences { get; }
}
