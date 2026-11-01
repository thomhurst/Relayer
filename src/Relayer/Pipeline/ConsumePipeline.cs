using System.Collections.Frozen;
using System.Diagnostics;
using System.Globalization;
using System.Runtime.CompilerServices;
using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

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
    private readonly FailurePipeline? _failures;
    private readonly ConsumeDelegate _pipeline;
    private readonly DefaultMessageContext?[] _contexts;
    private readonly WireIdentity[] _wireIdentities;
    private readonly EndpointTelemetry? _telemetry;
    private readonly ConsumeDelegate? _settlement;
    internal HeaderNames EnvelopeNames { get; } = new();

    internal ConsumePipeline(IServiceScopeFactory scopes, FrozenDictionary<MessageType, ConsumeDelegate> dispatch,
        IReadOnlyList<IConsumeMiddleware> middleware, ConsumeFailureDelegate failure, IFailureClassifier classifier,
        int capacity, FailurePipeline? failures, EndpointTelemetry? telemetry, ConsumeDelegate? settlement)
    {
        _scopes = scopes;
        _failures = failures;
        _dispatch = dispatch;
        _failure = failure;
        _classifier = classifier;
        _telemetry = telemetry;
        _settlement = settlement;
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
        var context = RentContext();
        context.Initialize(message, cancellationToken);
        return context;
    }

    private DefaultMessageContext RentContext()
    {
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

    private readonly record struct WireIdentity(MessageType Identity, string Text, byte[] Utf8);

    /// <summary>Processes one delivery in a fresh async DI scope, then invokes the configured native settlement callback.</summary>
    /// <remarks>Cancellation propagates. Failure-pipeline and scope-disposal errors propagate, preventing successful settlement.</remarks>
    public ValueTask ProcessAsync(DefaultMessageContext context)
    {
        ArgumentNullException.ThrowIfNull(context);
        context.BeginProcessing(this);
        return _failures is null ? ProcessCoreAsync(context) : ProcessWithFailuresAsync(context);
    }

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    private async ValueTask ProcessCoreAsync(DefaultMessageContext context)
    {
        var telemetry = _telemetry;
        var start = telemetry is { DurationEnabled: true } ? Stopwatch.GetTimestamp() : 0;
        Exception? failure = null;
        IDisposable? logScope = null;
        try
        {
            telemetry?.RecordConsumed();
            if (telemetry?.Logger.IsEnabled(LogLevel.Error) == true)
            {
                context.LogScope.Initialize(context);
                logScope = telemetry.Logger.BeginScope(context.LogScope);
            }
            context.CancellationToken.ThrowIfCancellationRequested();
            var scope = new AsyncServiceScope(_scopes.CreateScope());
            try
            {
                context.SetServices(scope.ServiceProvider);
                try
                {
                    if (!_dispatch.TryGetValue(context.MessageType, out var dispatch))
                        throw new UnknownMessageTypeException(context.MessageType);
                    context.Dispatch = dispatch;
                    await _pipeline(context).ConfigureAwait(false);
                }
                catch (OperationCanceledException) when (context.CancellationToken.IsCancellationRequested) { throw; }
                catch (Exception error)
                {
                    failure = error;
                    LogFailure(telemetry, error);
                    await _failure(context, error, _classifier.Classify(error, context)).ConfigureAwait(false);
                }
            }
            finally
            {
                // Calling the struct directly avoids boxing it through IAsyncDisposable.ConfigureAwait.
                await scope.DisposeAsync().ConfigureAwait(false);
            }
            context.ClearServices();
            if (_settlement is { } settle) await settle(context).ConfigureAwait(false);
        }
        catch (Exception error)
        {
            if (!ReferenceEquals(failure, error)) LogFailure(telemetry, error);
            failure = error;
            throw;
        }
        finally
        {
            try
            {
                if (start != 0) telemetry!.RecordDuration(Stopwatch.GetElapsedTime(start), failure is not null);
                telemetry?.RecordCriticalTime(context);
            }
            finally
            {
                try { logScope?.Dispose(); }
                finally
                {
                    context.LogScope.Clear();
                    context.EndProcessing();
                }
            }
        }
    }

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    private async ValueTask ProcessWithFailuresAsync(DefaultMessageContext context)
    {
        try
        {
            var retry = 0;
            var initialize = true;
            while (true)
            {
                context.CancellationToken.ThrowIfCancellationRequested();
                Exception? failure = null;
                var action = FailureAction.Retry;
                var scope = new AsyncServiceScope(_scopes.CreateScope());
                Sender? sender = null;
                try
                {
                    context.SetServices(scope.ServiceProvider);
                    try
                    {
                        if (initialize)
                        {
                            initialize = false;
                            context.InitializeAttempts();
                        }
                        // This is the same scoped sender injected into handlers and their dependencies.
                        var service = scope.ServiceProvider.GetService<ISender>();
                        if (service is not null && service is not Sender)
                            throw new InvalidOperationException("The failure pipeline requires Relayer's scoped ISender to buffer failed-attempt sends.");
                        sender = (Sender?)service;
                        if (sender is not null) sender.AttemptBuffer = context.SendBuffer;
                        if (!_dispatch.TryGetValue(context.MessageType, out var dispatch))
                            throw new UnknownMessageTypeException(context.MessageType);
                        context.Dispatch = dispatch;
                        await _pipeline(context).ConfigureAwait(false);
                    }
                    catch (OperationCanceledException) when (context.CancellationToken.IsCancellationRequested) { throw; }
                    catch (Exception error)
                    {
                        failure = error;
                        action = _classifier.Classify(error, context);
                        _failures!.Record(context, error);
                    }
                }
                finally
                {
                    try
                    {
                        await scope.DisposeAsync().ConfigureAwait(false);
                    }
                    finally
                    {
                        // Disposers can send too. Keep the buffer and sender reserved until all disposal ends,
                        // including when a disposer throws before DI reaches the sender's own Dispose.
                        sender?.EndAttempt();
                    }
                }
                context.CancellationToken.ThrowIfCancellationRequested();
                if (failure is null)
                {
                    // Dispose the successful scope before allowing sends to escape. A flush failure propagates:
                    // some sends may have succeeded, so do not retry the handler or return a settlement decision.
                    await context.SendBuffer.FlushAsync(context.CancellationToken).ConfigureAwait(false);
                    return;
                }
                context.SendBuffer.Clear();
                if (action == FailureAction.Retry && _failures!.CanRetry(retry))
                {
                    await _failures.WaitRetryAsync(retry++, context.CancellationToken).ConfigureAwait(false);
                    context.NextAttempt();
                    continue;
                }
                await _failures!.HandleAsync(context, failure, action).ConfigureAwait(false);
                return;
            }
        }
        finally
        {
            context.SendBuffer.Clear();
            context.EndProcessing();
        }
    }

    private static void LogFailure(EndpointTelemetry? telemetry, Exception error)
    {
        if (telemetry?.Logger.IsEnabled(LogLevel.Error) != true) return;
        var activity = Activity.Current;
        ConsumeLog.Failed(telemetry.Logger, error, activity?.TraceId.ToString(), activity?.SpanId.ToString());
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
