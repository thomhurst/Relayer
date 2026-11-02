namespace Relayer.Sagas;

/// <summary>A declared process state. This metadata does not execute a process.</summary>
/// <param name="Name">The case-sensitive state name.</param>
/// <param name="IsInitial">Whether this is the process entry state.</param>
/// <param name="IsFinal">Whether reaching this state finishes the process.</param>
public sealed record SagaState(string Name, bool IsInitial, bool IsFinal);
