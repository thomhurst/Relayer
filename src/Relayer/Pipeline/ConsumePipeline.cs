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
    private readonly DefaultMessageContext?[] _contexts;
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
        _process = failures is null ? context => ProcessCoreAsync((DefaultMessageContext)context)
            : context => ProcessWithFailuresAsync((DefaultMessageContext)context);
        _tracing = tracing;
        _logger = telemetry?.Logger ?? tracing?.Logger;
        _contexts = new DefaultMessageContext[capacity];
        _wireIdentities = new WireIdentity[dispatch.Count];
        var identityIndex = 0;
        foreach (var identity in dispatch.Keys)
        {
            var text = identity.ToString();
            _wireIdentities[identityIndex++] = new(identity, text, Encoding.UTF8.GetBytes(text));
        }
        for (var i = 0; i < capacity; i++)
        {
            _contexts[i] = new DefaultMessageContext(this, i);
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
    {
        if (IsBatch)
        {
            var start = Volatile.Read(ref _batchCursor);
            for (var offset = 0; offset < _contexts.Length; offset++)
            {
                var index = start + offset;
                if (index >= _contexts.Length) index -= _contexts.Length;
                if (Interlocked.Exchange(ref _contexts[index], null) is not { } context) continue;
                Volatile.Write(ref _batchCursor, index + 1 == _contexts.Length ? 0 : index + 1);
                return context;
            }
            return new DefaultMessageContext(this);
        }
        for (var i = 0; i < _contexts.Length; i++)
        {
            if (Interlocked.Exchange(ref _contexts[i], null) is { } context)
            {
                return context;
            }
        }
        return new DefaultMessageContext(this);
    }

    internal DefaultMessageContext RentContext(ReceiveMessageState message, CancellationToken cancellationToken)
    {
        var context = RentContext();
        context.Initialize(message, cancellationToken);
        return context;
    }

    internal MessageType ResolveWireIdentity(WireString wire)
    {
        foreach (var identity in _wireIdentities)
        {
            if (wire.Text is { } text ? text == identity.Text : wire.Utf8.Span.SequenceEqual(identity.Utf8))
                return identity.Identity;
        }

        // Unknown identities are an error path. Preserve them for the consume failure classifier.
        var unknown = wire.ToString();
        var separator = unknown.LastIndexOf(";v=", StringComparison.Ordinal);
        if (separator <= 0 || !int.TryParse(unknown.AsSpan(separator + 3), NumberStyles.None,
                CultureInfo.InvariantCulture, out var version) || version < 1)
            throw new MessageEnvelopeException("The incoming message type must contain a stable name and positive version, for example 'orders;v=1'.");
        return new(unknown[..separator], version);
    }

    internal string GetWireIdentityText(MessageType messageType)
    {
        foreach (var identity in _wireIdentities)
        {
            if (identity.Identity == messageType) return identity.Text;
        }
        return messageType.ToString();
    }

    private readonly record struct WireIdentity(MessageType Identity, string Text, byte[] Utf8);

    /// <summary>Processes one delivery in a fresh async DI scope, then invokes the configured native settlement callback.</summary>
    /// <remarks>Cancellation propagates. Failure-pipeline and scope-disposal errors propagate, preventing successful settlement.</remarks>
    public ValueTask ProcessAsync(DefaultMessageContext context)
    {
        if (IsBatch) throw new InvalidOperationException("Use ProcessBatchAsync for a batch consumer.");
        ArgumentNullException.ThrowIfNull(context);
        context.BeginProcessing(this);
        if (Observer is { } observer) return observer.ProcessAsync(context, _process);
        return _failures is null ? ProcessCoreAsync(context) : ProcessWithFailuresAsync(context);
    }

    private static ValueTask ProcessCoreAsync(DefaultMessageContext context) => context.CoreProcessing.Start();

    [RuntimeAsyncMethodGeneration(false)]
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    private async ValueTask ProcessWithFailuresAsync(DefaultMessageContext context)
    {
        var onDemand = _scopesOnDemand;
        var telemetry = _telemetry;
        var start = telemetry is { DurationEnabled: true } ? Stopwatch.GetTimestamp() : 0;
        Exception? failure = null;
        Activity? activity = null;
        var circuit = Circuit;
        var admitted = false;
        var probe = false;
        var outcomeRecorded = false;
        try
        {
            if (circuit is not null)
            {
                admitted = circuit.TryEnter(out probe);
                if (!admitted)
                {
                    context.FailureDisposition = Disposition.Redeliver(TimeSpan.Zero);
                    context.ApplyFailureDisposition();
                    // Service Bus leaves rejected callbacks unsettled: native lock expiry returns them
                    // without repeatedly abandoning the same delivery while a probe is running.
                    return;
                }
            }
            activity = _tracing?.Start(context);
            telemetry?.RecordConsumed();
            var retry = 0;
            var initialize = true;
            while (true)
            {
                context.CancellationToken.ThrowIfCancellationRequested();
                Exception? attemptFailure = null;
                var action = FailureAction.Retry;
                if (!onDemand) context.CreateScope();
                try
                {
                    if (onDemand) context.DemandScope();
                    else context.BindScope();
                    try
                    {
                        if (initialize)
                        {
                            initialize = false;
                            context.InitializeAttempts();
                        }
                        // This is the same scoped sender injected into handlers and their dependencies.
                        if (!onDemand) context.AttachSender();
                        if (!_dispatch.TryGetValue(context.MessageType, out var dispatch))
                            throw new UnknownMessageTypeException(context.MessageType);
                        context.Dispatch = dispatch;
                        await _pipeline(context).ConfigureAwait(false);
                    }
                    catch (OperationCanceledException) when (context.CancellationToken.IsCancellationRequested) { throw; }
                    catch (Exception error)
                    {
                        if (context.ObservedDisposition.Kind == DispositionKind.SettledByHandler ||
                            ReferenceEquals(context.ScopeCleanupError, error)) throw;
                        context.Settlement.Clear();
                        failure = attemptFailure = error;
                        LogFailure(context, error);
                        action = _classifier.Classify(error, context);
                        _failures!.Record(context, error);
                    }
                }
                finally
                {
                    try
                    {
                        await context.DisposeScopeAsync().ConfigureAwait(false);
                    }
                    finally { context.EndAttemptSender(); }
                }
                context.CancellationToken.ThrowIfCancellationRequested();
                if (attemptFailure is null)
                {
                    // Dispose the successful scope before allowing sends to escape. A flush failure propagates:
                    // some sends may have succeeded, so do not retry the handler or return a settlement decision.
                    if (context.Settlement.Request is { } request && request.Kind != DispositionKind.Complete)
                    {
                        context.SendBuffer.Clear();
                        await _failures!.HandleSettlementAsync(context, request).ConfigureAwait(false);
                        context.ApplyFailureDisposition();
                    }
                    else
                    {
                        await context.SendBuffer.FlushAsync(context.CancellationToken).ConfigureAwait(false);
                        if (context.Settlement.Request is { } complete)
                        {
                            context.FailureDisposition = complete;
                            context.ApplyFailureDisposition();
                        }
                        failure = null;
                    }
                    _failures!.Circuit?.Complete(false);
                    break;
                }
                context.Settlement.Clear();
                context.SendBuffer.Clear();
                if (action == FailureAction.Retry && _failures!.CanRetry(retry))
                {
                    await _failures.WaitRetryAsync(retry++, context.CancellationToken).ConfigureAwait(false);
                    context.NextAttempt();
                    continue;
                }
                if (!Enum.IsDefined(action)) throw new InvalidOperationException("IFailureClassifier returned an undefined FailureAction.");
                if (_failures!.Circuit?.Complete(action == FailureAction.Retry) == true)
                {
                    // The serial adapter retains this native delivery and suppresses settlement.
                    context.FailureDisposition = Disposition.Redeliver(TimeSpan.Zero);
                    context.ApplyFailureDisposition();
                    break;
                }
                if (circuit is not null)
                {
                    outcomeRecorded = true;
                    if (circuit.Complete(probe, true)) CircuitChanged?.Invoke(true);
                    // Include outcomes already in flight when another callback opened the circuit.
                    if (circuit.State != 0)
                    {
                        context.FailureDisposition = Disposition.Redeliver(TimeSpan.Zero);
                        context.ApplyFailureDisposition();
                        break;
                    }
                }
                await _failures!.HandleAsync(context, attemptFailure, action).ConfigureAwait(false);
                context.ApplyFailureDisposition();
                break;
            }
            context.ClearServices();
            if (_settlement is { } settle) await settle(context).ConfigureAwait(false);
            if (circuit is not null && !outcomeRecorded)
            {
                outcomeRecorded = true;
                var opened = circuit.Complete(probe, false);
                if (opened || probe) CircuitChanged?.Invoke(opened);
            }
        }
        catch (OperationCanceledException) when (context.CancellationToken.IsCancellationRequested) { throw; }
        catch (Exception error)
        {
            if (!ReferenceEquals(failure, error)) LogFailure(context, error);
            failure = error;
            throw;
        }
        finally
        {
            // Scope, flush, settlement and cancellation failures must release a half-open probe too.
            if (admitted && circuit is not null && !outcomeRecorded)
            {
                if (circuit.Complete(probe, true)) CircuitChanged?.Invoke(true);
            }
            context.SendBuffer.Clear();
            try
            {
                if (start != 0) telemetry!.RecordDuration(Stopwatch.GetElapsedTime(start), failure is not null);
                telemetry?.RecordCriticalTime(context);
            }
            finally
            {
                try
                {
                    if (failure is not null) MessageOperationTracing.Fail(activity, "_OTHER");
                    activity?.Dispose();
                }
                finally { context.EndProcessing(); }
            }
        }
    }

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
        if (IsBatch)
        {
            // Batch rentals retain their original slot, avoiding a scan for each item in a full batch.
            if (context.PoolSlot >= 0) Interlocked.CompareExchange(ref _contexts[context.PoolSlot], context, null);
            return;
        }
        for (var i = 0; i < _contexts.Length; i++)
        {
            if (Interlocked.CompareExchange(ref _contexts[i], context, null) is null) return;
        }
        // Concurrency beyond the configured capacity must not grow the retained pool without a bound.
    }
}
