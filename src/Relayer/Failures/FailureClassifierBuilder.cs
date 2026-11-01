namespace Relayer;

/// <summary>Composes typed exception rules once, in registration order, for a consumer's failure classifier.</summary>
public sealed class FailureClassifierBuilder
{
    private readonly List<Rule> _rules = [];
    private bool _built;

    /// <summary>Adds a rule for an exception type and its derived types.</summary>
    /// <remarks>
    /// The first matching rule whose optional predicate returns true wins, including for permanent contract failures.
    /// Predicates must support concurrent calls. A predicate exception propagates without a settlement decision.
    /// </remarks>
    public FailureClassifierBuilder On<TException>(FailureAction action,
        Func<TException, MessageContext, bool>? predicate = null) where TException : Exception
    {
        EnsureMutable();
        if (!Enum.IsDefined(action)) throw new ArgumentOutOfRangeException(nameof(action));
        _rules.Add(new TypedRule<TException>(action, predicate));
        return this;
    }

    /// <summary>Freezes these rules and returns a classifier. Unmatched failures use the supplied fallback or the default classifier.</summary>
    /// <remarks>A custom fallback must support concurrent calls.</remarks>
    public IFailureClassifier Build(IFailureClassifier? fallback = null)
    {
        EnsureMutable();
        _built = true;
        return new Classifier(_rules.ToArray(), fallback ?? new DefaultFailureClassifier());
    }

    private void EnsureMutable()
    {
        if (_built) throw new InvalidOperationException("The failure classifier has already been built. Compose each classifier once at startup.");
    }

    private abstract class Rule
    {
        internal abstract bool TryClassify(Exception exception, MessageContext context, out FailureAction action);
    }

    private sealed class TypedRule<TException>(FailureAction action, Func<TException, MessageContext, bool>? predicate) : Rule
        where TException : Exception
    {
        internal override bool TryClassify(Exception exception, MessageContext context, out FailureAction result)
        {
            result = action;
            return exception is TException typed && (predicate is null || predicate(typed, context));
        }
    }

    private sealed class Classifier(Rule[] rules, IFailureClassifier fallback) : IFailureClassifier
    {
        public FailureAction Classify(Exception exception, MessageContext context)
        {
            ArgumentNullException.ThrowIfNull(exception);
            ArgumentNullException.ThrowIfNull(context);
            foreach (var rule in rules)
                if (rule.TryClassify(exception, context, out var action)) return action;
            return fallback.Classify(exception, context);
        }
    }
}
