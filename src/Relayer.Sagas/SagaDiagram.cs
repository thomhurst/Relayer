using System.Globalization;
using System.Text;

namespace Relayer.Sagas;

/// <summary>An immutable process declaration for inspection and diagram export. Saga execution is not implemented.</summary>
public sealed class SagaDiagram
{
    internal SagaDiagram(SagaState[] states, SagaTransition[] transitions)
    {
        States = Array.AsReadOnly(states);
        Transitions = Array.AsReadOnly(transitions);
    }

    /// <summary>All declared states, sorted by ordinal name, including states with no transitions.</summary>
    public IReadOnlyList<SagaState> States { get; }

    /// <summary>All declared transitions, sorted by ordinal source, destination and trigger.</summary>
    public IReadOnlyList<SagaTransition> Transitions { get; }

    /// <summary>Exports Mermaid stateDiagram-v2 syntax with stable identifiers and LF line endings.</summary>
    /// <remarks>Names and triggers are encoded as labels, never interpreted as diagram syntax. No file is written.</remarks>
    public string ToMermaid()
    {
        var text = new StringBuilder("stateDiagram-v2\n");
        var identifiers = new Dictionary<string, string>(States.Count, StringComparer.Ordinal);
        for (var i = 0; i < States.Count; i++)
        {
            var state = States[i];
            var identifier = "s" + i.ToString(CultureInfo.InvariantCulture);
            identifiers.Add(state.Name, identifier);
            text.Append("    state \"");
            AppendLabel(text, state.Name);
            text.Append("\" as ").Append(identifier).Append('\n');
            if (state.IsInitial) text.Append("    [*] --> ").Append(identifier).Append('\n');
            if (state.IsFinal) text.Append("    ").Append(identifier).Append(" --> [*]\n");
        }
        foreach (var transition in Transitions)
        {
            text.Append("    ").Append(identifiers[transition.From]).Append(" --> ")
                .Append(identifiers[transition.To]).Append(" : ");
            AppendLabel(text, transition.Trigger);
            text.Append('\n');
        }
        return text.ToString();
    }

    private static void AppendLabel(StringBuilder text, string label)
    {
        // Encode whitespace too: Mermaid recognizes keywords such as "direction LR" inside labels.
        // Encode the entity introducer itself to preserve literal entity-looking input.
        foreach (var rune in label.EnumerateRunes())
        {
            if (rune.IsAscii && (char.IsAsciiLetterOrDigit((char)rune.Value) || rune.Value is '_' or '-' or '.'))
                text.Append((char)rune.Value);
            else
                text.Append('#').Append(rune.Value.ToString(CultureInfo.InvariantCulture)).Append(';');
        }
    }
}
