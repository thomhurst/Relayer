using BenchmarkDotNet.Attributes;

namespace Relayer.Benchmarks;

/// <summary>Compares the default classifier with configured typed rules, excluding exception construction.</summary>
[MemoryDiagnoser]
public class FailureClassifierBenchmarks
{
    private readonly IFailureClassifier _default = new DefaultFailureClassifier();
    private readonly Exception _exception = new InvalidOperationException();
    private readonly Context _context = new();
    private IFailureClassifier _matched = null!;
    private IFailureClassifier _unmatched = null!;

    [GlobalSetup]
    public void Setup()
    {
        _matched = new FailureClassifierBuilder()
            .On<ArgumentException>(FailureAction.DeadLetter)
            .On<InvalidOperationException>(FailureAction.Discard, static (_, context) => context.Attempt > 1).Build();
        _unmatched = new FailureClassifierBuilder()
            .On<ArgumentException>(FailureAction.DeadLetter)
            .On<InvalidOperationException>(FailureAction.Discard, static (_, context) => context.Attempt > 3).Build();
    }

    [Benchmark(Baseline = true)]
    public FailureAction Default() => _default.Classify(_exception, _context);

    [Benchmark]
    public FailureAction MatchedRule() => _matched.Classify(_exception, _context);

    [Benchmark]
    public FailureAction UnmatchedRules() => _unmatched.Classify(_exception, _context);

    private sealed class Context : MessageContext
    {
        public override string MessageId => "id";
        public override MessageType MessageType => new("benchmark", 1);
        public override MessageHeaders Headers => throw new NotSupportedException();
        public override int Attempt => 2;
        public override IServiceProvider Services => throw new NotSupportedException();
        public override MessageFeatures Features { get; } = new();
        public override CancellationToken CancellationToken => default;
    }
}
