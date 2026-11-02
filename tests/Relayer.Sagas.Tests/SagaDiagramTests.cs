using System.Globalization;
using Relayer.Sagas;

namespace Relayer.Sagas.Tests;

public sealed class SagaDiagramTests
{
    [Test]
    public async Task Exports_all_declared_states_branches_loops_and_terminal_states()
    {
        var diagram = new SagaDiagramBuilder()
            .State("Pending", initial: true)
            .State("Paid", final: true)
            .State("Cancelled", final: true)
            .State("Isolated")
            .Transition("Pending", "Paid", "PaymentReceived")
            .Transition("Pending", "Cancelled", "OrderExpired")
            .Transition("Pending", "Pending", "PaymentRetried")
            .Build();

        await Assert.That(diagram.ToMermaid()).IsEqualTo("""
            stateDiagram-v2
                state "Cancelled" as s0
                s0 --> [*]
                state "Isolated" as s1
                state "Paid" as s2
                s2 --> [*]
                state "Pending" as s3
                [*] --> s3
                s3 --> s0 : OrderExpired
                s3 --> s2 : PaymentReceived
                s3 --> s3 : PaymentRetried

            """.Replace("\r\n", "\n", StringComparison.Ordinal));
    }

    [Test]
    public async Task Equivalent_declarations_export_identically_regardless_of_order_and_culture()
    {
        var first = new SagaDiagramBuilder().State("i", initial: true).State("I", final: true)
            .Transition("i", "I", "z").Transition("i", "I", "a").Build().ToMermaid();
        var previous = CultureInfo.CurrentCulture;
        try
        {
            CultureInfo.CurrentCulture = CultureInfo.GetCultureInfo("tr-TR");
            var second = new SagaDiagramBuilder().State("I", final: true).State("i", initial: true)
                .Transition("i", "I", "a").Transition("i", "I", "z").Build();
            await Assert.That(second.ToMermaid()).IsEqualTo(first);
        }
        finally { CultureInfo.CurrentCulture = previous; }
    }

    [Test]
    public async Task Built_diagram_is_an_immutable_snapshot()
    {
        var builder = new SagaDiagramBuilder().State("Start", initial: true);
        var diagram = builder.Build();
        var original = diagram.ToMermaid();
        builder.State("End", final: true).Transition("Start", "End", "Finish");
        await Assert.That(diagram.ToMermaid()).IsEqualTo(original);
        await Assert.That(diagram.States.Count).IsEqualTo(1);
        await Assert.That(diagram.Transitions.Count).IsEqualTo(0);
        await Assert.That(() => ((IList<SagaState>)diagram.States).Clear()).Throws<NotSupportedException>();
        await Assert.That(() => ((IList<SagaTransition>)diagram.Transitions).Clear()).Throws<NotSupportedException>();
    }

    [Test]
    public async Task Labels_cannot_add_syntax_comments_html_or_entity_escapes()
    {
        const string label = "A\"\n[*] --> injected: <script>&#quot; %% direction LR 雪😀";
        var diagram = new SagaDiagramBuilder().State(label, initial: true)
            .Transition(label, label, label).Build();
        var text = diagram.ToMermaid();
        const string encoded = "A#34;#10;#91;#42;#93;#32;--#62;#32;injected#58;#32;#60;script#62;#38;#35;quot#59;#32;#37;#37;#32;direction#32;LR#32;#38634;#128512;";
        await Assert.That(text).IsEqualTo($"stateDiagram-v2\n    state \"{encoded}\" as s0\n    [*] --> s0\n    s0 --> s0 : {encoded}\n");
        await Assert.That(diagram.States[0].Name).IsEqualTo(label);
        await Assert.That(diagram.Transitions[0].Trigger).IsEqualTo(label);
    }

    [Test]
    [Arguments(null)]
    [Arguments("")]
    [Arguments(" \t")]
    public async Task Empty_names_are_rejected(string? name)
    {
        await Assert.That(() => new SagaDiagramBuilder().State(name!)).Throws<ArgumentException>();
        await Assert.That(() => new SagaDiagramBuilder().Transition(name!, "B", "T")).Throws<ArgumentException>();
        await Assert.That(() => new SagaDiagramBuilder().Transition("A", name!, "T")).Throws<ArgumentException>();
        await Assert.That(() => new SagaDiagramBuilder().Transition("A", "B", name!)).Throws<ArgumentException>();
    }

    [Test]
    public async Task Duplicate_declarations_are_rejected()
    {
        var builder = new SagaDiagramBuilder().State("A", initial: true).Transition("A", "A", "T");
        await Assert.That(() => builder.State("A")).Throws<ArgumentException>();
        await Assert.That(() => builder.Transition("A", "A", "T")).Throws<ArgumentException>();
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task Build_requires_exactly_one_initial_state(bool multiple)
    {
        var builder = new SagaDiagramBuilder().State("A", initial: multiple).State("B", initial: multiple);
        await Assert.That(() => builder.Build()).Throws<InvalidOperationException>();
        await Assert.That(() => new SagaDiagramBuilder().Build()).Throws<InvalidOperationException>();
    }

    [Test]
    [Arguments("missing", "A")]
    [Arguments("A", "missing")]
    public async Task Build_rejects_undeclared_transition_endpoints(string from, string to)
    {
        var builder = new SagaDiagramBuilder().State("A", initial: true).Transition(from, to, "T");
        await Assert.That(() => builder.Build()).Throws<InvalidOperationException>();
    }

    [Test]
    public async Task Final_states_cannot_have_outgoing_transitions()
    {
        var builder = new SagaDiagramBuilder().State("A", initial: true, final: true).Transition("A", "A", "T");
        await Assert.That(() => builder.Build()).Throws<InvalidOperationException>();
    }

    [Test]
    public async Task Single_state_process_can_start_and_finish_in_same_state()
    {
        var diagram = new SagaDiagramBuilder().State("Done", initial: true, final: true).Build();
        await Assert.That(diagram.ToMermaid()).IsEqualTo("stateDiagram-v2\n    state \"Done\" as s0\n    [*] --> s0\n    s0 --> [*]\n");
    }

    [Test]
    public async Task Transitions_can_precede_states_and_failed_build_can_be_corrected()
    {
        var builder = new SagaDiagramBuilder().Transition("Start", "Finish", "Complete")
            .State("Start", initial: true);
        await Assert.That(() => builder.Build()).Throws<InvalidOperationException>();
        var diagram = builder.State("Finish", final: true).Build();
        await Assert.That(diagram.States.Count).IsEqualTo(2);
        await Assert.That(diagram.Transitions.Count).IsEqualTo(1);
    }
}
