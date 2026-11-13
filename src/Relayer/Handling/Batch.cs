namespace Relayer;

/// <summary>A batch of messages delivered to an <see cref="IBatchHandler{TMessage}"/>.</summary>
/// <typeparam name="TMessage">The message type.</typeparam>
/// <param name="Items">The messages and their contexts, in delivery order.</param>
/// <remarks>The batch, its items and contexts are borrowed only for the handler call. Do not retain them after returning.</remarks>
public sealed record Batch<TMessage>(IReadOnlyList<BatchItem<TMessage>> Items)
{
    /// <summary>The number of messages in the batch.</summary>
    public int Count => Items.Count;
}

/// <summary>A message within a batch.</summary>
/// <typeparam name="TMessage">The message type.</typeparam>
/// <param name="Message">The deserialized message.</param>
/// <param name="Context">The message context.</param>
public readonly record struct BatchItem<TMessage>(TMessage Message, MessageContext Context)
{
    private readonly IBatchItemFailureTarget? _failures;
    private readonly long _generation;
    private readonly int _index;

    internal BatchItem(TMessage message, MessageContext context, IBatchItemFailureTarget failures, long generation, int index)
        : this(message, context) => (_failures, _generation, _index) = (failures, generation, index);

    /// <summary>Marks this delivery as failed. Other items can still succeed.</summary>
    /// <remarks>Valid only during the handler invocation that supplied this item. Do not retain items or use them after returning.</remarks>
    /// <exception cref="InvalidOperationException">The item was not supplied by an active handler invocation.</exception>
    public void Fail(Exception exception)
    {
        ArgumentNullException.ThrowIfNull(exception);
        if (_failures is null)
            throw new InvalidOperationException("Only items supplied to an active batch handler can be marked as failed.");
        _failures.Fail(_generation, _index, exception);
    }
}

internal interface IBatchItemFailureTarget
{
    void Fail(long generation, int index, Exception exception);
}

/// <summary>The outcome of handling a batch.</summary>
public sealed class BatchResult
{
    private BatchResult(Exception? error) => Error = error;

    /// <summary>All items not explicitly marked with <c>item.Fail(exception)</c> succeeded.</summary>
    public static BatchResult Success { get; } = new(null);

    /// <summary>The exception that fails every item, or null when only explicitly marked items failed.</summary>
    public Exception? Error { get; }

    /// <summary>Reports that every item in the invocation failed.</summary>
    public static BatchResult Failure(Exception exception)
    {
        ArgumentNullException.ThrowIfNull(exception);
        return new BatchResult(exception);
    }
}
