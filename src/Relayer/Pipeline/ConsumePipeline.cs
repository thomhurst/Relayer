using System.Collections.Frozen;
using System.Runtime.CompilerServices;
using Microsoft.Extensions.DependencyInjection;

namespace Relayer;

/// <summary>Durably handles a failed delivery. A successful return permits the receive layer to continue settlement.</summary>
public delegate ValueTask ConsumeFailureDelegate(MessageContext context, Exception error, FailureAction action);

/// <summary>A consumer's composed middleware and generated dispatch, shared across concurrent deliveries.</summary>
public sealed class ConsumePipeline
{
    private readonly IServiceScopeFactory _scopes;
    private readonly FrozenDictionary<MessageType, ConsumeDelegate> _dispatch;
    private readonly ConsumeFailureDelegate _failure;
    private readonly IFailureClassifier _classifier;
    private readonly ConsumeDelegate _pipeline;
    private readonly DefaultMessageContext?[] _contexts;

    internal ConsumePipeline(IServiceScopeFactory scopes, FrozenDictionary<MessageType, ConsumeDelegate> dispatch,
        IReadOnlyList<IConsumeMiddleware> middleware, ConsumeFailureDelegate failure, IFailureClassifier classifier, int capacity)
    {
        _scopes = scopes;
        _dispatch = dispatch;
        _failure = failure;
        _classifier = classifier;
        _contexts = new DefaultMessageContext[capacity];
        for (var i = 0; i < capacity; i++)
        {
            _contexts[i] = new DefaultMessageContext(this);
        }
        _pipeline = InvokeHandlerAsync;
        for (var i = middleware.Count - 1; i >= 0; i--)
        {
            var step = middleware[i];
            var next = _pipeline;
            _pipeline = context => step.InvokeAsync(context, next);
        }
    }

    /// <summary>Rents a context for one delivery. Populate native features, await processing, then dispose it.</summary>
    public DefaultMessageContext RentContext(in ConsumeMessage message, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(message.MessageId);
        ArgumentOutOfRangeException.ThrowIfLessThan(message.Attempt, 1);
        for (var i = 0; i < _contexts.Length; i++)
        {
            if (Interlocked.Exchange(ref _contexts[i], null) is { } context)
            {
                context.Initialize(message, cancellationToken);
                return context;
            }
        }
        var overflow = new DefaultMessageContext(this);
        overflow.Initialize(message, cancellationToken);
        return overflow;
    }

    /// <summary>Processes one delivery in a fresh async DI scope. Does not settle the native message.</summary>
    /// <remarks>Cancellation propagates. Failure-pipeline and scope-disposal errors propagate, preventing successful settlement.</remarks>
    public ValueTask ProcessAsync(DefaultMessageContext context)
    {
        ArgumentNullException.ThrowIfNull(context);
        context.BeginProcessing(this);
        return ProcessCoreAsync(context);
    }

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    private async ValueTask ProcessCoreAsync(DefaultMessageContext context)
    {
        try
        {
            context.CancellationToken.ThrowIfCancellationRequested();
            var scope = new AsyncServiceScope(_scopes.CreateScope());
            await using var scopeLifetime = scope.ConfigureAwait(false);
            context.SetServices(scope.ServiceProvider);
            if (!_dispatch.TryGetValue(context.MessageType, out var dispatch))
            {
                var error = new UnknownMessageTypeException(context.MessageType);
                await _failure(context, error, _classifier.Classify(error, context)).ConfigureAwait(false);
                return;
            }
            context.Dispatch = dispatch;
            try
            {
                await _pipeline(context).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (context.CancellationToken.IsCancellationRequested) { throw; }
            catch (Exception error)
            {
                await _failure(context, error, _classifier.Classify(error, context)).ConfigureAwait(false);
            }
        }
        finally
        {
            context.EndProcessing();
        }
    }

    private static ValueTask InvokeHandlerAsync(MessageContext context) => ((DefaultMessageContext)context).Dispatch(context);

    internal void Return(DefaultMessageContext context)
    {
        for (var i = 0; i < _contexts.Length; i++)
        {
            if (Interlocked.CompareExchange(ref _contexts[i], context, null) is null) return;
        }
        // Concurrency beyond the configured capacity must not grow the retained pool without a bound.
    }
}
