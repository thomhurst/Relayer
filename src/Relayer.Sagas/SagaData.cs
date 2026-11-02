namespace Relayer.Sagas;

/// <summary>Application-owned process data. Retain completed rows to prevent a process from restarting.</summary>
public abstract class SagaData
{
    /// <summary>The stable process correlation key.</summary>
    public Guid Id { get; init; }

    /// <summary>Retains completed state as a tombstone so later messages cannot restart the process.</summary>
    public bool IsCompleted { get; set; }
}
