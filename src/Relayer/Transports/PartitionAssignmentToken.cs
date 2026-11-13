namespace Relayer.Transports;

/// <summary>Identifies one assignment, including its ownership epoch. Default and foreign tokens are invalid.</summary>
public readonly struct PartitionAssignmentToken
{
    internal PartitionAssignmentToken(object state) => State = state;
    internal object? State { get; }
}
