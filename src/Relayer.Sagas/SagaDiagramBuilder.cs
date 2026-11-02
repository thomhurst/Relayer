namespace Relayer.Sagas;

/// <summary>Explicitly declares a process diagram at startup, without scanning or executing handlers.</summary>
public sealed class SagaDiagramBuilder
{
    private readonly Dictionary<string, SagaState> _states = new(StringComparer.Ordinal);
    private readonly HashSet<SagaTransition> _transitions = [];

    /// <summary>Declares a unique, case-sensitive state. Exactly one state must be initial.</summary>
    public SagaDiagramBuilder State(string name, bool initial = false, bool final = false)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        if (!_states.TryAdd(name, new(name, initial, final)))
            throw new ArgumentException($"State '{name}' is already declared. Declare each state once.", nameof(name));
        return this;
    }

    /// <summary>Declares a transition. Its endpoints must be declared before Build, and its source must not be final.</summary>
    public SagaDiagramBuilder Transition(string from, string to, string trigger)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(from);
        ArgumentException.ThrowIfNullOrWhiteSpace(to);
        ArgumentException.ThrowIfNullOrWhiteSpace(trigger);
        if (!_transitions.Add(new(from, to, trigger)))
            throw new ArgumentException($"Transition '{from}' to '{to}' for '{trigger}' is already declared.", nameof(trigger));
        return this;
    }

    /// <summary>Validates declarations and takes an immutable snapshot sorted by ordinal names.</summary>
    public SagaDiagram Build()
    {
        var initialCount = 0;
        foreach (var state in _states.Values)
            if (state.IsInitial) initialCount++;
        if (initialCount != 1)
            throw new InvalidOperationException("Declare exactly one initial state with State(name, initial: true).");

        foreach (var transition in _transitions)
        {
            if (!_states.TryGetValue(transition.From, out var source))
                throw new InvalidOperationException($"Transition source '{transition.From}' is not declared. Add it with State(name).");
            if (!_states.ContainsKey(transition.To))
                throw new InvalidOperationException($"Transition destination '{transition.To}' is not declared. Add it with State(name).");
            if (source.IsFinal)
                throw new InvalidOperationException($"Final state '{source.Name}' cannot have outgoing transitions. Remove the transition or mark the state as non-final.");
        }

        var states = new List<SagaState>(_states.Values);
        states.Sort(static (left, right) => StringComparer.Ordinal.Compare(left.Name, right.Name));
        var transitions = new List<SagaTransition>(_transitions);
        transitions.Sort(static (left, right) =>
        {
            var order = StringComparer.Ordinal.Compare(left.From, right.From);
            if (order != 0) return order;
            order = StringComparer.Ordinal.Compare(left.To, right.To);
            return order != 0 ? order : StringComparer.Ordinal.Compare(left.Trigger, right.Trigger);
        });
        return new(states.ToArray(), transitions.ToArray());
    }
}
