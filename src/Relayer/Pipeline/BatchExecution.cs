using Relayer.Transports;
using System.Collections.Frozen;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using Microsoft.Extensions.DependencyInjection;

namespace Relayer.Pipeline;

internal sealed class BatchExecution
{
    private readonly IServiceScopeFactory _scopes;
    private readonly BatchDispatcher _dispatch;
    private readonly MessageType _identity;
    private readonly FrozenSet<MessageType>? _upcastIdentities;
    private readonly ConsumeFailureDelegate _failure;
    private readonly IFailureClassifier _classifier;
    private readonly FailurePipeline? _failures;
    private readonly ActivitySource? _source;
    private readonly ConsumeTracing? _tracing;
    private readonly string _spanName;
    private readonly string _destination;
    private readonly KeyValuePair<string, object?>[] _batchCountTags;
    private readonly State?[] _states;
    internal int Size { get; }

    internal BatchExecution(IServiceScopeFactory scopes, BatchDispatcher dispatch, MessageType identity,
        IConsumerDefinition consumer, ConsumeFailureDelegate failure, IFailureClassifier classifier,
        FailurePipeline? failures, ActivitySource? source, ConsumeTracing? tracing, FrozenSet<MessageType>? upcastIdentities)
    {
        _scopes = scopes;
        _dispatch = dispatch;
        _identity = identity;
        _upcastIdentities = upcastIdentities;
        _failure = failure;
        _classifier = classifier;
        _failures = failures;
        _source = source;
        _tracing = tracing;
        Size = consumer.BatchSize;
        _destination = consumer.Source;
        _spanName = "process " + consumer.Source;
        _batchCountTags = new KeyValuePair<string, object?>[Size];
        for (var i = 0; i < Size; i++) _batchCountTags[i] = new("messaging.batch.message_count", i + 1);
        _states = new State[consumer.Concurrency];
        for (var i = 0; i < _states.Length; i++) _states[i] = new State(Size, _destination, _tracing);
    }

    [RuntimeAsyncMethodGeneration(false)]
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    internal async ValueTask ProcessAsync(ConsumePipeline owner, ReadOnlyMemory<DefaultMessageContext> contexts, CancellationToken cancellationToken)
    {
        if (contexts.Length > Size) throw new ArgumentOutOfRangeException(nameof(contexts), "Batch exceeds the consumer's configured maximum size.");
        if (contexts.IsEmpty) return;
        cancellationToken.ThrowIfCancellationRequested();
        var state = Rent();
        var begun = 0;
        Activity? activity = null;
        var unrecovered = false;
        var pendingFailure = false;
        Exception? loggedFailure = null;
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
                    if (context.MessageType != _identity && _upcastIdentities?.Contains(context.MessageType) != true)
                        throw new UnknownMessageTypeException(context.MessageType);
                    if (_failures is not null) context.InitializeAttempts();
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested || context.CancellationToken.IsCancellationRequested) { throw; }
                catch (Exception failure) { error = failure; }
                if (error is null) state.Active[count++] = context;
                else
                {
                    unrecovered = true;
                    owner.LogFailure(context, error);
                    loggedFailure = error;
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
                    for (var i = 0; i < count; i++)
                    {
                        state.Active[i].CancellationToken.ThrowIfCancellationRequested();
                        state.Active[i].SetServices(scope.ServiceProvider);
                    }
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
                var redirected = 0;
                for (var i = 0; i < count; i++)
                {
                    state.Active[i].CancellationToken.ThrowIfCancellationRequested();
                    if (state.Errors[i] is not null) failed++;
                    else if (state.Active[i].Settlement.Request is { Kind: not DispositionKind.Complete }) redirected++;
                }
                if (failed == 0 && redirected == 0)
                {
                    await state.FlushAsync(buffer, count, cancellationToken).ConfigureAwait(false);
                    pendingFailure = false;
                }
                else if (failed + redirected < count && !buffer.IsEmpty)
                {
                    // One scoped sender cannot attribute output to individual items. Fail the
                    // whole attempt so discarded sends never accompany successful settlement.
                    var error = new InvalidOperationException("A partially failed or manually redirected batch has buffered sends that cannot be attributed to individual messages. The whole attempt failed; use per-message durable outbox transactions for independent success.");
                    for (var i = 0; i < count; i++) state.Errors[i] ??= error;
                }
                if (failed != 0) pendingFailure = true;
                cancellationToken.ThrowIfCancellationRequested();
                buffer.Clear();
                var pending = 0;
                for (var i = 0; i < count; i++)
                {
                    var context = state.Active[i];
                    context.CancellationToken.ThrowIfCancellationRequested();
                    context.ClearServices();
                    if (state.Errors[i] is not { } error)
                    {
                        if (context.Settlement.Request is { } request)
                        {
                            if (_failures is null || request.Kind == DispositionKind.Complete)
                                context.FailureDisposition = request;
                            else await _failures.HandleSettlementAsync(context, request).ConfigureAwait(false);
                        }
                        continue;
                    }
                    context.Settlement.Clear();
                    pendingFailure = true;
                    owner.LogFailure(context, error);
                    loggedFailure = error;
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
                        unrecovered = true;
                        await _failures.HandleAsync(context, error, action).ConfigureAwait(false);
                    }
                    else
                    {
                        unrecovered = true;
                        await _failure(context, error, action).ConfigureAwait(false);
                    }
                }
                count = pending;
                if (count > 0) await _failures!.WaitRetryAsync(retry++, cancellationToken).ConfigureAwait(false);
            }
            if (owner.Settlement is { } settle)
            {
                for (var i = 0; i < contexts.Length; i++)
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    var context = contexts.Span[i];
                    context.CancellationToken.ThrowIfCancellationRequested();
                    context.ClearServices();
                    context.ApplyFailureDisposition();
                    try { await settle(context).ConfigureAwait(false); }
                    catch (Exception error)
                    {
                        if (error is not OperationCanceledException || !context.CancellationToken.IsCancellationRequested)
                            owner.LogFailure(context, error);
                        loggedFailure = error;
                        throw;
                    }
                }
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested || DeliveryCancelled(contexts)) { throw; }
        catch (Exception error)
        {
            unrecovered = true;
            if (!ReferenceEquals(loggedFailure, error)) owner.LogFailure(contexts.Span[0], error);
            throw;
        }
        finally
        {
            try
            {
                if (unrecovered || pendingFailure) MessageOperationTracing.Fail(activity, "_OTHER");
                activity?.Dispose();
            }
            finally
            {
                for (var i = 0; i < begun; i++)
                {
                    contexts.Span[i].SendBuffer.Clear();
                    contexts.Span[i].EndProcessing();
                }
                Array.Clear(state.Active);
                Array.Clear(state.Errors);
                state.Links.Clear();
                for (var i = 0; i < _states.Length; i++)
                    if (Interlocked.CompareExchange(ref _states[i], state, null) is null) break;
            }
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

    private static bool DeliveryCancelled(ReadOnlyMemory<DefaultMessageContext> contexts)
    {
        for (var i = 0; i < contexts.Length; i++)
            if (contexts.Span[i].CancellationToken.IsCancellationRequested) return true;
        return false;
    }

    private Activity? StartActivity(ReadOnlyMemory<DefaultMessageContext> contexts, State state)
    {
        if (_tracing is not null)
            return _tracing.StartBatch(contexts, state.Tags, state.Links, _batchCountTags[contexts.Length - 1]);
        if (_source is null || !_source.HasListeners()) return null;
        for (var i = 0; i < contexts.Length; i++)
        {
            var context = contexts.Span[i];
            var creation = MessageCreationContext.Read<MessageHeaders, CreationHeaderReader>(context.Headers, context.EnvelopeNames);
            if (creation != default) state.Links.Add(new(creation));
        }
        state.Tags[^1] = _batchCountTags[contexts.Length - 1];
        // ActivitySource consumes these reusable sampling inputs synchronously.
        return _source.StartActivity(_spanName, ActivityKind.Consumer, Activity.Current?.Context ?? default,
            state.Tags, state.Links);
    }

    private State Rent()
    {
        for (var i = 0; i < _states.Length; i++)
            if (Interlocked.Exchange(ref _states[i], null) is { } state) return state;
        return new State(Size, _destination, _tracing);
    }

    private readonly struct CreationHeaderReader : IMessageHeaderReader<MessageHeaders>
    {
        public static bool TryGetHeader(MessageHeaders message, string name, out WireString value) =>
            message.TryGetValue(name, out value);
    }

    private sealed class State(int size, string destination, ConsumeTracing? tracing)
    {
        internal readonly DefaultMessageContext[] Active = new DefaultMessageContext[size];
        internal readonly Exception?[] Errors = new Exception[size];
        internal readonly List<ActivityLink> Links = new(size);
        internal readonly KeyValuePair<string, object?>[] Tags = tracing?.CreateBatchTags() ??
        [
            new("messaging.operation.type", "process"), new("messaging.operation.name", "process"),
            new("messaging.destination.name", destination), default
        ];
        private CancellationTokenSource _flushCancellation = new();
        private readonly CancellationTokenRegistration[] _flushRegistrations = new CancellationTokenRegistration[size + 1];

        [RuntimeAsyncMethodGeneration(false)]
        [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
        internal async ValueTask FlushAsync(AttemptSendBuffer buffer, int count, CancellationToken cancellationToken)
        {
            if (buffer.IsEmpty) return;
            // Reuse the source and registration storage; replace only after cancellation.
            if (!_flushCancellation.TryReset())
            {
                _flushCancellation.Dispose();
                _flushCancellation = new();
            }
            var registered = 0;
            try
            {
                _flushRegistrations[registered++] = Register(cancellationToken);
                for (var i = 0; i < count; i++)
                    _flushRegistrations[registered++] = Register(Active[i].CancellationToken);
                await buffer.FlushAsync(_flushCancellation.Token).ConfigureAwait(false);
            }
            finally
            {
                for (var i = 0; i < registered; i++)
                {
                    _flushRegistrations[i].Dispose();
                    _flushRegistrations[i] = default;
                }
            }
        }

        private CancellationTokenRegistration Register(CancellationToken token) =>
            token.UnsafeRegister(static state => ((CancellationTokenSource)state!).Cancel(), _flushCancellation);
    }
}
