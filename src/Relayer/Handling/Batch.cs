namespace Relayer;

/// <summary>A batch of messages delivered to an <see cref="IBatchHandler{TMessage}"/>.</summary>
/// <typeparam name="TMessage">The message type.</typeparam>
/// <param name="Items">The messages and their contexts, in delivery order.</param>
public sealed record Batch<TMessage>(IReadOnlyList<BatchItem<TMessage>> Items)
{
    /// <summary>The number of messages in the batch.</summary>
    public int Count => Items.Count;
}

/// <summary>A message within a batch.</summary>
/// <typeparam name="TMessage">The message type.</typeparam>
/// <param name="Message">The deserialized message.</param>
/// <param name="Context">The message context.</param>
public readonly record struct BatchItem<TMessage>(TMessage Message, MessageContext Context);

/// <summary>The outcome of handling a batch.</summary>
public sealed class BatchResult
{
    private BatchResult(IReadOnlyDictionary<int, Exception> failures) => Failures = failures;

    /// <summary>Every message succeeded.</summary>
    public static BatchResult Success { get; } = new(new Dictionary<int, Exception>());

    /// <summary>Failures keyed by index within the batch. Only these messages enter the failure pipeline.</summary>
    public IReadOnlyDictionary<int, Exception> Failures { get; }

    /// <summary>Reports that the messages at the given indexes failed.</summary>
    public static BatchResult PartialFailure(IReadOnlyDictionary<int, Exception> failures)
    {
        ArgumentNullException.ThrowIfNull(failures);
        return failures.Count == 0 ? Success : new BatchResult(failures);
    }
}
