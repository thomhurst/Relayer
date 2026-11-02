namespace Relayer.Sagas;

/// <summary>A declared process transition. Triggers are labels, not message registrations.</summary>
/// <param name="From">The source state name.</param>
/// <param name="To">The destination state name.</param>
/// <param name="Trigger">The event or message label for the transition.</param>
public sealed record SagaTransition(string From, string To, string Trigger);
