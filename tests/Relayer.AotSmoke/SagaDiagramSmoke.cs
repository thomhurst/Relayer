using Relayer.Sagas;

internal static class SagaDiagramSmoke
{
    internal static void Run()
    {
        var diagram = new SagaDiagramBuilder()
            .State("Pending", initial: true)
            .State("Paid", final: true)
            .Transition("Pending", "Paid", "PaymentReceived")
            .Build();
        const string expected = "stateDiagram-v2\n    state \"Paid\" as s0\n    s0 --> [*]\n    state \"Pending\" as s1\n    [*] --> s1\n    s1 --> s0 : PaymentReceived\n";
        if (diagram.ToMermaid() != expected || diagram.States.Count != 2 || diagram.Transitions.Count != 1)
            throw new InvalidOperationException("Saga diagram export failed under Native AOT.");
        Console.WriteLine("Saga diagram export passed under Native AOT.");
    }
}
