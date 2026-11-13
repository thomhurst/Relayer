using Relayer.Transports;
namespace Relayer.Tests;

public class FailureClassifierTests
{
    private readonly Context _context = new();

    [Test]
    [Arguments(FailureAction.Retry)]
    [Arguments(FailureAction.DeadLetter)]
    [Arguments(FailureAction.Discard)]
    public async Task Typed_rules_match_derived_exceptions_and_all_actions(FailureAction action)
    {
        var classifier = new FailureClassifierBuilder().On<ArgumentException>(action).Build();
        await Assert.That(classifier.Classify(new ArgumentNullException("value"), _context)).IsEqualTo(action);
    }

    [Test]
    public async Task First_matching_rule_wins_and_false_predicates_fall_through()
    {
        var calls = 0;
        var error = new ArgumentNullException("value");
        var classifier = new FailureClassifierBuilder()
            .On<InvalidOperationException>(FailureAction.Retry, (_, _) => throw new Exception("Wrong type"))
            .On<ArgumentException>(FailureAction.Retry, (exception, context) =>
            {
                if (!ReferenceEquals(exception, error) || !ReferenceEquals(context, _context))
                    throw new Exception("Wrong arguments");
                calls++;
                return context.Attempt < 3;
            })
            .On<Exception>(FailureAction.Discard)
            .On<ArgumentNullException>(FailureAction.DeadLetter, (_, _) => throw new Exception("Later rule ran"))
            .Build();

        await Assert.That(classifier.Classify(error, _context)).IsEqualTo(FailureAction.Discard);
        await Assert.That(calls).IsEqualTo(1);
    }

    [Test]
    public async Task Unmatched_failures_preserve_permanent_contract_defaults()
    {
        var classifier = new FailureClassifierBuilder().On<ArgumentException>(FailureAction.Discard).Build();
        Exception[] permanent =
        [
            new MessageEnvelopeException("invalid"),
            new UnknownMessageTypeException(new("unknown", 1)),
            new MessageDeserializationException(new("invalid", 1), new FormatException()),
        ];
        foreach (var error in permanent)
            await Assert.That(classifier.Classify(error, _context)).IsEqualTo(FailureAction.DeadLetter);
        await Assert.That(classifier.Classify(new InvalidOperationException(), _context)).IsEqualTo(FailureAction.Retry);
    }

    [Test]
    public async Task Rules_can_explicitly_override_permanent_defaults()
    {
        var classifier = new FailureClassifierBuilder().On<MessageEnvelopeException>(FailureAction.Retry).Build();
        await Assert.That(classifier.Classify(new MessageEnvelopeException("invalid"), _context)).IsEqualTo(FailureAction.Retry);
    }

    [Test]
    public async Task Custom_fallback_receives_original_exception_and_context_only_when_unmatched()
    {
        var fallback = new Fallback();
        var classifier = new FailureClassifierBuilder().On<ArgumentException>(FailureAction.Retry).Build(fallback);
        var error = new InvalidOperationException();
        await Assert.That(classifier.Classify(error, _context)).IsEqualTo(FailureAction.Discard);
        await Assert.That(fallback.Error).IsSameReferenceAs(error);
        await Assert.That(fallback.Context).IsSameReferenceAs(_context);
        await Assert.That(classifier.Classify(new ArgumentException(), _context)).IsEqualTo(FailureAction.Retry);
        await Assert.That(fallback.Calls).IsEqualTo(1);
    }

    [Test]
    public async Task Composition_freezes_builder_and_rejects_invalid_actions()
    {
        var builder = new FailureClassifierBuilder();
        await Assert.That(() => builder.On<Exception>((FailureAction)99)).Throws<ArgumentOutOfRangeException>();
        var classifier = builder.On<Exception>(FailureAction.Discard).Build();
        await Assert.That(() => builder.On<Exception>(FailureAction.Retry)).Throws<InvalidOperationException>();
        await Assert.That(() => builder.Build()).Throws<InvalidOperationException>();
        await Assert.That(classifier.Classify(new Exception(), _context)).IsEqualTo(FailureAction.Discard);
        var independent = new FailureClassifierBuilder().Build();
        await Assert.That(independent.Classify(new Exception(), _context)).IsEqualTo(FailureAction.Retry);
    }

    [Test]
    public async Task Classification_rejects_null_arguments_and_propagates_predicate_errors()
    {
        var classifier = new FailureClassifierBuilder()
            .On<Exception>(FailureAction.Retry, (_, _) => throw new FormatException("predicate failed")).Build();
        await Assert.That(() => classifier.Classify(null!, _context)).Throws<ArgumentNullException>();
        await Assert.That(() => classifier.Classify(new Exception(), null!)).Throws<ArgumentNullException>();
        await Assert.That(() => classifier.Classify(new Exception(), _context)).Throws<FormatException>();
    }

    [Test]
    public async Task Warm_classification_does_not_allocate()
    {
        var classifier = new FailureClassifierBuilder()
            .On<ArgumentException>(FailureAction.Discard, static (_, context) => context.Attempt > 3)
            .On<InvalidOperationException>(FailureAction.DeadLetter).Build();
        var matched = new ArgumentException();
        var unmatched = new FormatException();
        for (var i = 0; i < 10_000; i++)
        {
            classifier.Classify(matched, _context);
            classifier.Classify(unmatched, _context);
        }
        var before = GC.GetAllocatedBytesForCurrentThread();
        for (var i = 0; i < 10_000; i++)
        {
            classifier.Classify(matched, _context);
            classifier.Classify(unmatched, _context);
        }
        var allocated = GC.GetAllocatedBytesForCurrentThread() - before;
        await Assert.That(allocated).IsEqualTo(0);
    }

    private sealed class Fallback : IFailureClassifier
    {
        internal Exception? Error { get; private set; }
        internal MessageContext? Context { get; private set; }
        internal int Calls { get; private set; }

        public FailureAction Classify(Exception exception, MessageContext context)
        {
            Error = exception;
            Context = context;
            Calls++;
            return FailureAction.Discard;
        }
    }

    private sealed class Context : MessageContext
    {
        public override string MessageId => "id";
        public override MessageType MessageType => new("message", 1);
        public override MessageHeaders Headers => throw new NotSupportedException();
        public override int Attempt => 4;
        public override IServiceProvider Services => throw new NotSupportedException();
        public override MessageFeatures Features { get; } = new();
        public override CancellationToken CancellationToken => default;
    }
}
