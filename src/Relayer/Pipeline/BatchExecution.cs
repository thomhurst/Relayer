using System.Diagnostics;
using System.Runtime.CompilerServices;
using Microsoft.Extensions.DependencyInjection;

namespace Relayer;

internal sealed class BatchExecution
{
    private readonly IServiceScopeFactory _scopes;
    private readonly BatchDispatcher _dispatch;
    private readonly MessageType _identity;
    private readonly ConsumeFailureDelegate _failure;
    private readonly IFailureClassifier _classifier;
    private readonly FailurePipeline? _failures;
    private readonly ActivitySource? _source;
    private readonly string _spanName;
    private readonly string _destination;
    private readonly State?[] _states;
    internal int Size { get; }

    internal BatchExecution(IServiceScopeFactory scopes, BatchDispatcher dispatch, MessageType identity,
        IConsumerDefinition consumer, ConsumeFailureDelegate failure, IFailureClassifier classifier,
        FailurePipeline? failures, ActivitySource? source)
    {
        _scopes = scopes;
        _dispatch = dispatch;
        _identity = identity;
        _failure = failure;
        _classifier = classifier;
        _failures = failures;
        _source = source;
        Size = consumer.BatchSize;
        _destination = consumer.Source;
        _spanName = "process " + consumer.Source;
        _states = new State[consumer.Concurrency];
        for (var i = 0; i < _states.Length; i++) _states[i] = new State(Size);
    }

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    internal async ValueTask ProcessAsync(ConsumePipeline owner, ReadOnlyMemory<DefaultMessageContext> contexts, CancellationToken cancellationToken)
    {
        if (contexts.Length > Size) throw new ArgumentOutOfRangeException(nameof(contexts), "Batch exceeds the consumer's configured maximum size.");
        if (contexts.IsEmpty) return;
        cancellationToken.ThrowIfCancellationRequested();
        var state = Rent();
        var begun = 0;
        Activity? activity = null;
        try
        {
            for (; begun < contexts.Length; begun++)
                contexts.Span[begun].BeginProcessing(owner);
            activity = StartActivity(contexts, state);
            var count = 0;
            for (var i = 0; i < contexts.Length; i++)
            {
                var context = contexts.Span[i];
                Exception? error = null;
                try
                {
                    context.CancellationToken.ThrowIfCancellationRequested();
                    if (context.MessageType != _identity) throw new UnknownMessageTypeException(context.MessageType);
                    if (_failures is not null) context.InitializeAttempts();
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested || context.CancellationToken.IsCancellationRequested) { throw; }
                catch (Exception failure) { error = failure; }
                if (error is null) state.Active[count++] = context;
                else
                {
                    RecordError(activity, error);
                    await HandleFailureAsync(context, error).ConfigureAwait(false);
                }
            }
            var retry = 0;
            while (count > 0)
            {
                cancellationToken.ThrowIfCancellationRequested();
                Array.Clear(state.Errors, 0, count);
                var scope = new AsyncServiceScope(_scopes.CreateScope());
                Sender? sender = null;
                var buffer = state.Active[0].SendBuffer;
                try
                {
                    for (var i = 0; i < count; i++) state.Active[i].SetServices(scope.ServiceProvider);
                    var service = scope.ServiceProvider.GetService<ISender>();
                    if (service is not null && service is not Sender)
                        throw new InvalidOperationException("Batch execution requires Relayer's scoped ISender to buffer failed-attempt sends.");
                    sender = (Sender?)service;
                    if (sender is not null) sender.AttemptBuffer = buffer;
                    await _dispatch.InvokeAsync(state.Active.AsMemory(0, count), state.Errors.AsMemory(0, count), cancellationToken).ConfigureAwait(false);
                }
                finally
                {
                    try { await scope.DisposeAsync().ConfigureAwait(false); }
                    finally { sender?.EndAttempt(); }
                }
                cancellationToken.ThrowIfCancellationRequested();
                var failed = 0;
                for (var i = 0; i < count; i++) if (state.Errors[i] is not null) failed++;
                if (failed == 0) await buffer.FlushAsync(cancellationToken).ConfigureAwait(false);
                else if (failed < count && !buffer.IsEmpty)
                    throw new InvalidOperationException("A partially failed batch has buffered sends that cannot be attributed to individual messages. Use per-message durable outbox transactions or return success only after all batch work is durable.");
                buffer.Clear();
                var pending = 0;
                for (var i = 0; i < count; i++)
                {
                    var context = state.Active[i];
                    context.ClearServices();
                    if (state.Errors[i] is not { } error) continue;
                    RecordError(activity, error);
                    var action = _classifier.Classify(error, context);
                    if (_failures is not null)
                    {
                        _failures.Record(context, error);
                        if (action == FailureAction.Retry && _failures.CanRetry(retry))
                        {
                            context.NextAttempt();
                            state.Active[pending++] = context;
                            continue;
                        }
                        await _failures.HandleAsync(context, error, action).ConfigureAwait(false);
                    }
                    else await _failure(context, error, action).ConfigureAwait(false);
                }
                count = pending;
                if (count > 0) await _failures!.WaitRetryAsync(retry++, cancellationToken).ConfigureAwait(false);
            }
        }
        catch (Exception error)
        {
            RecordError(activity, error);
            throw;
        }
        finally
        {
            activity?.Dispose();
            for (var i = 0; i < begun; i++)
            {
                contexts.Span[i].SendBuffer.Clear();
                contexts.Span[i].EndProcessing();
            }
            Array.Clear(state.Active);
            Array.Clear(state.Errors);
            Array.Clear(state.Links);
            for (var i = 0; i < _states.Length; i++)
                if (Interlocked.CompareExchange(ref _states[i], state, null) is null) break;
        }
    }

    private async ValueTask HandleFailureAsync(DefaultMessageContext context, Exception error)
    {
        var action = _classifier.Classify(error, context);
        if (_failures is null) await _failure(context, error, action).ConfigureAwait(false);
        else
        {
            _failures.Record(context, error);
            await _failures.HandleAsync(context, error, action).ConfigureAwait(false);
        }
    }

    private static void RecordError(Activity? activity, Exception error)
    {
        if (activity is null) return;
        // Exception type is error-path diagnostic metadata, never a dispatch lookup.
        activity.SetTag("error.type", error.GetType().FullName ?? "_OTHER");
        activity.SetStatus(ActivityStatusCode.Error);
    }

    private Activity? StartActivity(ReadOnlyMemory<DefaultMessageContext> contexts, State state)
    {
        if (_source is null || !_source.HasListeners()) return null;
        var count = 0;
        for (var i = 0; i < contexts.Length; i++)
        {
            var headers = contexts.Span[i].Headers;
            if (headers.TryGetValue("traceparent", out var parent))
            {
                var traceState = headers.TryGetValue("tracestate", out var value) ? value.ToString() : null;
                if (ActivityContext.TryParse(parent.ToString(), traceState, true, out var creation))
                    state.Links[count++] = new(creation);
            }
        }
        var tags = new ActivityTagsCollection
        {
            { "messaging.operation.type", "process" }, { "messaging.operation.name", "process" },
            { "messaging.destination.name", _destination }, { "messaging.batch.message_count", contexts.Length }
        };
        return _source.StartActivity(_spanName, ActivityKind.Consumer, Activity.Current?.Context ?? default,
            tags, new ArraySegment<ActivityLink>(state.Links, 0, count));
    }

    private State Rent()
    {
        for (var i = 0; i < _states.Length; i++)
            if (Interlocked.Exchange(ref _states[i], null) is { } state) return state;
        return new State(Size);
    }

    private sealed class State(int size)
    {
        internal readonly DefaultMessageContext[] Active = new DefaultMessageContext[size];
        internal readonly Exception?[] Errors = new Exception[size];
        internal readonly ActivityLink[] Links = new ActivityLink[size];
    }
}
