using Relayer.Transports;
using System.Collections.Frozen;
using System.Diagnostics;
using System.Globalization;
using System.Runtime.CompilerServices;
using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Relayer.Pipeline;

/// <summary>Durably handles a failed delivery. A successful return permits the receive layer to continue settlement.</summary>
public delegate ValueTask ConsumeFailureDelegate(MessageContext context, Exception error, FailureAction action);

/// <summary>A consumer's composed middleware and generated dispatch, shared across concurrent deliveries.</summary>
public sealed partial class ConsumePipeline
{
    internal BatchExecution? BatchExecution { get; init; }
    // Native durable failure delegates can reuse attempt buffering without a second retry runtime.
    internal bool BufferAttemptSends { get; set; }

    /// <summary>Whether this consumer dispatches a generated batch handler.</summary>
    public bool IsBatch => BatchExecution is not null;

    /// <summary>Processes one batch, with one scope per batch attempt and only failed items retried.</summary>
    /// <remarks>Optional native settlement callbacks run after all durable work. Keep context memory unchanged until completion. Only use settlement decisions after the entire call succeeds.</remarks>
    public ValueTask ProcessBatchAsync(ReadOnlyMemory<DefaultMessageContext> contexts, CancellationToken cancellationToken = default)
        => Observer is { } observer ? observer.ProcessBatchAsync(this, contexts, cancellationToken)
            : ProcessBatchCoreAsync(contexts, cancellationToken);

    internal IServiceScopeFactory Scopes => _scopes;

    // Set when every handler is a singleton, so dispatch needs no delivery scope.
    internal IServiceProvider? SingletonHandlerServices { get; }

    internal ValueTask ProcessBatchCoreAsync(ReadOnlyMemory<DefaultMessageContext> contexts, CancellationToken cancellationToken)
        => (BatchExecution ?? throw new InvalidOperationException("Build a batch consumer before using batch dispatch."))
            .ProcessAsync(this, contexts, cancellationToken);
    internal IRelayerObserver? Observer { get; }
    internal bool RestoresTenant { get; }
    private readonly ConsumeDelegate _process;
    private readonly IServiceScopeFactory _scopes;
    private readonly bool _scopesOnDemand;
    private readonly FrozenDictionary<MessageType, ConsumeDelegate> _dispatch;
    private readonly ConsumeFailureDelegate _failure;
    private readonly IFailureClassifier _classifier;
    private readonly FailurePipeline? _failures;
    private readonly ConsumeDelegate _pipeline;
    private readonly SlotPool<DefaultMessageContext> _contexts;
    private readonly WireIdentity[] _wireIdentities;
    private readonly EndpointTelemetry? _telemetry;
    private readonly ConsumeDelegate? _settlement;
    private readonly ConsumeTracing? _tracing;
    private readonly ILogger? _logger;
    internal HeaderNames EnvelopeNames { get; } = new();
    private int _batchCursor;
    internal ConsumerCircuit? Circuit { get; set; }
    internal Action<bool>? CircuitChanged { get; set; }

    internal ConsumePipeline(IServiceScopeFactory scopes, FrozenDictionary<MessageType, ConsumeDelegate> dispatch,
        IReadOnlyList<IConsumeMiddleware> middleware, ConsumeFailureDelegate failure, IFailureClassifier classifier,
        int capacity, FailurePipeline? failures, EndpointTelemetry? telemetry, ConsumeDelegate? settlement, ConsumeTracing? tracing,
        IServiceProvider? singletonHandlerServices = null)
    {
        _scopes = scopes;
        using (var scope = scopes.CreateScope())
        {
            Observer = scope.ServiceProvider.GetService<IRelayerObserver>();
            RestoresTenant = scope.ServiceProvider.GetService<TenantContextPool>() is not null;
        }
        SingletonHandlerServices = singletonHandlerServices;
        // Tenant restoration writes to a scoped service, so those consumers keep one scope per attempt.
        _scopesOnDemand = failures is not null && singletonHandlerServices is not null && !RestoresTenant;
        _failures = failures;
        _dispatch = dispatch;
        _failure = failure;
        _classifier = classifier;
        _telemetry = telemetry;
        _settlement = settlement is not null && Observer is { } observer ? observer.WrapSettlement(settlement) : settlement;
        _process = static context => ProcessCoreAsync((DefaultMessageContext)context);
        _tracing = tracing;
        _logger = telemetry?.Logger ?? tracing?.Logger;
        _contexts = new(capacity, capacity);
        _wireIdentities = new WireIdentity[dispatch.Count];
        var identityIndex = 0;
        foreach (var pair in dispatch)
        {
            var identity = pair.Key;
            var text = identity.ToString();
            _wireIdentities[identityIndex++] = new(identity, text, Encoding.UTF8.GetBytes(text), pair.Value);
        }
        for (var i = 0; i < capacity; i++)
        {
            _contexts.TryReturn(new DefaultMessageContext(this, i), i);
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
        var context = RentContext();
        context.Initialize(message, cancellationToken);
        return context;
    }

    private DefaultMessageContext RentContext()
        => (IsBatch ? _contexts.TryRent(ref _batchCursor) : _contexts.TryRent()) ?? new DefaultMessageContext(this);

    internal DefaultMessageContext RentContext(ReceiveMessageState message, CancellationToken cancellationToken)
    {
        var context = RentContext();
        context.Initialize(message, cancellationToken);
        return context;
    }

    internal MessageType ResolveWireIdentity(WireString wire, out ConsumeDelegate? dispatch)
    {
        foreach (var identity in _wireIdentities)
        {
            if (wire.Text is { } text ? text == identity.Text : wire.Utf8.Span.SequenceEqual(identity.Utf8))
            {
                dispatch = identity.Dispatch;
                return identity.Identity;
            }
        }

        dispatch = null;
        // Unknown identities are an error path. Preserve them for the consume failure classifier.
        var unknown = wire.ToString();
        var separator = unknown.LastIndexOf(";v=", StringComparison.Ordinal);
        if (separator <= 0 || !int.TryParse(unknown.AsSpan(separator + 3), NumberStyles.None,
                CultureInfo.InvariantCulture, out var version) || version < 1)
            throw new MessageEnvelopeException("The incoming message type must contain a stable name and positive version, for example 'orders;v=1'.");
        return new(unknown[..separator], version);
    }

    internal ConsumeDelegate ResolveDispatch(DefaultMessageContext context)
    {
        // Native receive already selected the delegate while decoding the wire identity.
        if (context.ResolvedDispatch is { } resolved) return resolved;
        if (_dispatch.TryGetValue(context.MessageType, out var dispatch)) return dispatch;
        throw new UnknownMessageTypeException(context.MessageType);
    }

    internal string GetWireIdentityText(MessageType messageType)
    {
        foreach (var identity in _wireIdentities)
        {
            if (identity.Identity == messageType) return identity.Text;
        }
        return messageType.ToString();
    }

    private readonly record struct WireIdentity(MessageType Identity, string Text, byte[] Utf8, ConsumeDelegate Dispatch);

    /// <summary>Processes one delivery in a fresh async DI scope, then invokes the configured native settlement callback.</summary>
    /// <remarks>Cancellation propagates. Failure-pipeline and scope-disposal errors propagate, preventing successful settlement.</remarks>
    public ValueTask ProcessAsync(DefaultMessageContext context)
    {
        if (IsBatch) throw new InvalidOperationException("Use ProcessBatchAsync for a batch consumer.");
        ArgumentNullException.ThrowIfNull(context);
        context.BeginProcessing(this);
        if (Observer is { } observer) return observer.ProcessAsync(context, _process);
        return ProcessCoreAsync(context);
    }

    private static ValueTask ProcessCoreAsync(DefaultMessageContext context) => context.CoreProcessing.Start();

    internal void ValidateSettlement(Disposition disposition) => _failures?.ValidateSettlement(disposition);

    internal ConsumeDelegate? Settlement => _settlement;

    internal void LogFailure(DefaultMessageContext context, Exception error)
    {
        if (ReferenceEquals(context.LoggedSettlementError, error))
        {
            context.LoggedSettlementError = null;
            return;
        }
        Observer?.Failed(context, error);
        if (context.TryConsumeNativeSettlementError(error)) return;
        try
        {
            if (_logger?.IsEnabled(LogLevel.Error) != true) return;
            using var scope = _logger.BeginScope(ConsumeLog.CreateScope(context));
            var activity = Activity.Current;
            ConsumeLog.Failed(_logger, error, activity?.TraceId.ToString(), activity?.SpanId.ToString());
        }
        catch (Exception)
        {
            // A broken logging provider must not replace the delivery error or skip durable failure handling.
        }
    }

    internal void LogSettlementFailure(DefaultMessageContext context, Exception error)
    {
        context.LoggedSettlementError = null;
        LogFailure(context, error);
        context.LoggedSettlementError = error;
    }

    private static ValueTask InvokeHandlerAsync(MessageContext context) => ((DefaultMessageContext)context).Dispatch(context);

    internal void Return(DefaultMessageContext context)
    {
        if (IsBatch) _contexts.TryReturn(context, context.PoolSlot);
        else _contexts.TryReturn(context);
    }
}
