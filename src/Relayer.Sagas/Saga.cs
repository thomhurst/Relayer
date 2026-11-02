using Microsoft.Extensions.DependencyInjection;

namespace Relayer.Sagas;

/// <summary>Runs an explicit typed transition with application- or integration-provided persistence.</summary>
/// <remarks>Persistence must share the consumer inbox/outbox boundary. Use the EF Core integration for PostgreSQL.</remarks>
public abstract class Saga<TState, TMessage> : IHandler<TMessage>
    where TState : SagaData
{
    /// <summary>Validates the consumer's transaction before loading state or invoking application code.</summary>
    protected abstract void ValidateTransaction();

    /// <summary>Loads tracked state using an explicit application query and correlation key.</summary>
    protected abstract ValueTask<TState?> LoadAsync(TMessage message, CancellationToken cancellationToken);

    /// <summary>Creates state for a starting message. The default rejects missing state.</summary>
    protected virtual TState Create(TMessage message)
        => throw new InvalidOperationException("No saga state exists for this message. Override Create only for messages that may start the process.");

    /// <summary>Changes tracked state and sends through the scoped ISender. The runtime saves changes afterward.</summary>
    protected abstract ValueTask TransitionAsync(TState state, TMessage message, MessageContext context,
        CancellationToken cancellationToken);

    /// <summary>Persists a new or existing transition, including optimistic concurrency for send-only work.</summary>
    protected abstract ValueTask SaveAsync(TState state, bool isNew, CancellationToken cancellationToken);

    /// <inheritdoc />
    public ValueTask HandleAsync(TMessage message, MessageContext context, CancellationToken cancellationToken)
        => context.Services.GetRequiredService<SagaExecution<TState, TMessage>>().Start(this, message, context, cancellationToken);

    internal void Validate() => ValidateTransaction();
    internal ValueTask<TState?> Load(TMessage message, CancellationToken cancellation) => LoadAsync(message, cancellation);
    internal TState New(TMessage message) => Create(message);
    internal ValueTask Apply(TState state, TMessage message, MessageContext context, CancellationToken cancellation)
        => TransitionAsync(state, message, context, cancellation);
    internal ValueTask Save(TState state, bool isNew, CancellationToken cancellation) => SaveAsync(state, isNew, cancellation);
}
