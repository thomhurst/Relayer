using Relayer.Transports;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.ExceptionServices;
using System.Threading.Tasks.Sources;
using Microsoft.Extensions.DependencyInjection;

namespace Relayer.Pipeline;

public sealed partial class ConsumePipeline
{
    // Continuations belong to the rented context, rather than a worker-local runtime cache.
    internal sealed class CoreOperation : IValueTaskSource
    {
        private readonly ConsumePipeline _owner;
        private readonly DefaultMessageContext _context;
        private readonly Action _continuation;
        private ManualResetValueTaskSourceCore<bool> _completion;
        private ConfiguredValueTaskAwaitable.ConfiguredValueTaskAwaiter _pending;
        private ExecutionContext? _executionContext;
        private AsyncServiceScope _scope;
        // The context-owned runner carries policy and settlement through one execution context.
        private Activity? TraceActivity;
        private Exception? Failure;
        private Exception? _error;
        private long StartedAt;
        private bool Admitted;
        private bool Probe;
        private bool OutcomeRecorded;
        private int _retry;
        private bool _initialize;
        private Exception? _attemptFailure;
        private FailureAction _action;
        private Stage _stage;
        private bool _suspended;

        internal CoreOperation(ConsumePipeline owner, DefaultMessageContext context)
        {
            _owner = owner;
            _context = context;
            _continuation = Resume;
        }

        internal ValueTask Start()
        {
            _suspended = false;
            _stage = Stage.Start;
            // Start restores the caller's ExecutionContext, including Activity and AsyncLocal changes.
            var runner = new Runner(this);
            var builder = AsyncTaskMethodBuilder.Create();
            builder.Start(ref runner);
            if (_suspended) return new ValueTask(this, _completion.Version);
            var error = _error;
            _error = null;
            return error is null ? default : ValueTask.FromException(error);
        }

        private void Resume()
        {
            var executionContext = _executionContext;
            _executionContext = null;
            if (executionContext is not null)
                ExecutionContext.Run(executionContext, static state => ((CoreOperation)state!).MoveNext(), this);
            else
            {
                var runner = new Runner(this);
                var builder = AsyncTaskMethodBuilder.Create();
                builder.Start(ref runner);
            }
        }

        private bool AwaitPending()
        {
            if (!_pending.IsCompleted)
            {
                if (!_suspended)
                {
                    _completion.Reset();
                    _suspended = true;
                }
                _executionContext = ExecutionContext.Capture();
                _pending.UnsafeOnCompleted(_continuation);
                return false;
            }
            var pending = _pending;
            _pending = default;
            pending.GetResult();
            return true;
        }

        private void MoveNext()
        {
            while (true)
            {
                try
                {
                    switch (_stage)
                    {
                        case Stage.Start:
                            if (_owner._failures is not null)
                            {
                                Admitted = Probe = OutcomeRecorded = false;
                                _retry = 0;
                                _initialize = true;
                                StartedAt = _owner._telemetry is { DurationEnabled: true } ? Stopwatch.GetTimestamp() : 0;
                                if (_owner.Circuit is { } policyCircuit)
                                {
                                    Admitted = policyCircuit.TryEnter(out Probe);
                                    if (!Admitted)
                                    {
                                        _context.FailureDisposition = Disposition.Redeliver(TimeSpan.Zero);
                                        _context.ApplyFailureDisposition();
                                        // Native lock expiry returns rejected Service Bus callbacks.
                                        Finish(null);
                                        return;
                                    }
                                }
                                TraceActivity = _owner._tracing?.Start(_context);
                                _owner._telemetry?.RecordConsumed();
                                goto case Stage.PolicyAttempt;
                            }
                            StartedAt = _owner._telemetry is { DurationEnabled: true } ? Stopwatch.GetTimestamp() : 0;
                            TraceActivity = _owner._tracing?.Start(_context);
                            _owner._telemetry?.RecordConsumed();
                            _context.CancellationToken.ThrowIfCancellationRequested();
                            _scope = new AsyncServiceScope(_owner._scopes.CreateScope());
                            _stage = Stage.Scope;
                            _context.SetServices(_scope.ServiceProvider);
                            if (_owner.BufferAttemptSends) _context.AttachSender(_scope.ServiceProvider);
                            _stage = Stage.Handler;
                            if (!_owner._dispatch.TryGetValue(_context.MessageType, out var dispatch))
                                throw new UnknownMessageTypeException(_context.MessageType);
                            _context.Dispatch = dispatch;
                            _pending = _owner._pipeline(_context).ConfigureAwait(false).GetAwaiter();
                            goto case Stage.Handler;
                        case Stage.PolicyAttempt:
                            _stage = Stage.PolicyAttempt;
                            _context.CancellationToken.ThrowIfCancellationRequested();
                            _attemptFailure = null;
                            _action = FailureAction.Retry;
                            if (!_owner._scopesOnDemand) _context.CreateScope();
                            _stage = Stage.PolicyScope;
                            if (_owner._scopesOnDemand) _context.DemandScope();
                            else _context.BindScope();
                            _stage = Stage.PolicyHandler;
                            if (_initialize)
                            {
                                _initialize = false;
                                _context.InitializeAttempts();
                            }
                            if (!_owner._scopesOnDemand) _context.AttachSender();
                            if (!_owner._dispatch.TryGetValue(_context.MessageType, out var policyDispatch))
                                throw new UnknownMessageTypeException(_context.MessageType);
                            _context.Dispatch = policyDispatch;
                            _pending = _owner._pipeline(_context).ConfigureAwait(false).GetAwaiter();
                            goto case Stage.PolicyHandler;
                        case Stage.PolicyHandler:
                            if (!AwaitPending()) return;
                            goto case Stage.PolicyDispose;
                        case Stage.PolicyDispose:
                            _stage = Stage.PolicyAfterDispose;
                            try { _pending = _context.DisposeScopeAsync().ConfigureAwait(false).GetAwaiter(); }
                            catch { _context.EndAttemptSender(); throw; }
                            goto case Stage.PolicyAfterDispose;
                        case Stage.PolicyAfterDispose:
                            try { if (!AwaitPending()) return; }
                            catch { _context.EndAttemptSender(); throw; }
                            _context.EndAttemptSender();
                            if (_error is { } policyError) ExceptionDispatchInfo.Throw(policyError);
                            _context.CancellationToken.ThrowIfCancellationRequested();
                            if (_attemptFailure is null)
                            {
                                if (_context.Settlement.Request is { } policyRequest && policyRequest.Kind != DispositionKind.Complete)
                                {
                                    _context.SendBuffer.Clear();
                                    _stage = Stage.PolicyRequested;
                                    _pending = _owner._failures!.HandleSettlementAsync(_context, policyRequest).ConfigureAwait(false).GetAwaiter();
                                    goto case Stage.PolicyRequested;
                                }
                                // A flush can partially succeed; its failure propagates without retrying the handler.
                                _stage = Stage.PolicyFlush;
                                _pending = _context.SendBuffer.FlushAsync(_context.CancellationToken).ConfigureAwait(false).GetAwaiter();
                                goto case Stage.PolicyFlush;
                            }
                            _context.Settlement.Clear();
                            _context.SendBuffer.Clear();
                            if (_action == FailureAction.Retry && _owner._failures!.CanRetry(_retry))
                            {
                                _stage = Stage.PolicyRetry;
                                _pending = _owner._failures.WaitRetryAsync(_retry++, _context.CancellationToken).ConfigureAwait(false).GetAwaiter();
                                goto case Stage.PolicyRetry;
                            }
                            if (!Enum.IsDefined(_action)) throw new InvalidOperationException("IFailureClassifier returned an undefined FailureAction.");
                            if (_owner._failures!.Circuit?.Complete(_action == FailureAction.Retry) == true)
                            {
                                // The serial adapter retains this native delivery and suppresses settlement.
                                _context.FailureDisposition = Disposition.Redeliver(TimeSpan.Zero);
                                _context.ApplyFailureDisposition();
                                goto case Stage.PolicyComplete;
                            }
                            if (_owner.Circuit is { } failedCircuit)
                            {
                                OutcomeRecorded = true;
                                if (failedCircuit.Complete(Probe, true)) _owner.CircuitChanged?.Invoke(true);
                                if (failedCircuit.State != 0)
                                {
                                    _context.FailureDisposition = Disposition.Redeliver(TimeSpan.Zero);
                                    _context.ApplyFailureDisposition();
                                    goto case Stage.PolicyComplete;
                                }
                            }
                            _stage = Stage.PolicyCopy;
                            _pending = _owner._failures.HandleAsync(_context, _attemptFailure, _action).ConfigureAwait(false).GetAwaiter();
                            goto case Stage.PolicyCopy;
                        case Stage.PolicyRequested:
                            if (!AwaitPending()) return;
                            _context.ApplyFailureDisposition();
                            goto case Stage.PolicySuccess;
                        case Stage.PolicyFlush:
                            if (!AwaitPending()) return;
                            if (_context.Settlement.Request is { } complete)
                            {
                                _context.FailureDisposition = complete;
                                _context.ApplyFailureDisposition();
                            }
                            Failure = null;
                            goto case Stage.PolicySuccess;
                        case Stage.PolicySuccess:
                            _owner._failures!.Circuit?.Complete(false);
                            goto case Stage.PolicyComplete;
                        case Stage.PolicyCopy:
                            if (!AwaitPending()) return;
                            _context.ApplyFailureDisposition();
                            goto case Stage.PolicyComplete;
                        case Stage.PolicyRetry:
                            if (!AwaitPending()) return;
                            _context.NextAttempt();
                            goto case Stage.PolicyAttempt;
                        case Stage.PolicyComplete:
                            _context.ClearServices();
                            goto case Stage.PrepareSettlement;
                        case Stage.Handler:
                        case Stage.Failure:
                            if (!AwaitPending()) return;
                            _stage = Stage.Dispose;
                            goto case Stage.Dispose;
                        case Stage.Dispose:
                            _stage = Stage.AfterDispose;
                            _pending = _scope.DisposeAsync().ConfigureAwait(false).GetAwaiter();
                            goto case Stage.AfterDispose;
                        case Stage.AfterDispose:
                            if (!AwaitPending()) return;
                            if (_owner.BufferAttemptSends) _context.EndAttemptSender();
                            if (_error is { } error) ExceptionDispatchInfo.Throw(error);
                            _context.CancellationToken.ThrowIfCancellationRequested();
                            if (Failure is null && _context.Settlement.Request is { } request)
                            {
                                _context.FailureDisposition = request;
                                _context.ApplyFailureDisposition();
                            }
                            _context.ClearServices();
                            if (!_owner.BufferAttemptSends) goto case Stage.PrepareSettlement;
                            _stage = Stage.Flush;
                            // Scope disposal is part of the attempt. Failure or a non-complete
                            // settlement must discard its sends, just like the policy path.
                            if (Failure is null &&
                                (_context.Settlement.Request is not { } settlement || settlement.Kind == DispositionKind.Complete))
                                _pending = _context.SendBuffer.FlushAsync(_context.CancellationToken).ConfigureAwait(false).GetAwaiter();
                            else
                            {
                                _context.SendBuffer.Clear();
                                _pending = default;
                            }
                            goto case Stage.Flush;
                        case Stage.Flush:
                            if (!AwaitPending()) return;
                            goto case Stage.PrepareSettlement;
                        case Stage.PrepareSettlement:
                            _stage = Stage.Settlement;
                            _pending = (_owner._settlement?.Invoke(_context) ?? default).ConfigureAwait(false).GetAwaiter();
                            goto case Stage.Settlement;
                        case Stage.Settlement:
                            if (!AwaitPending()) return;
                            if (_owner._failures is not null && _owner.Circuit is { } circuit && !OutcomeRecorded)
                            {
                                OutcomeRecorded = true;
                                var opened = circuit.Complete(Probe, false);
                                if (opened || Probe) _owner.CircuitChanged?.Invoke(opened);
                            }
                            Finish(null);
                            return;
                    }
                }
                catch (Exception error)
                {
                    if (_stage is Stage.PolicyHandler or Stage.PolicyScope)
                    {
                        var classify = _stage == Stage.PolicyHandler;
                        _stage = Stage.PolicyDispose;
                        try
                        {
                            if (!classify ||
                                error is OperationCanceledException && _context.CancellationToken.IsCancellationRequested ||
                                _context.ObservedDisposition.Kind == DispositionKind.SettledByHandler ||
                                ReferenceEquals(_context.ScopeCleanupError, error))
                                ExceptionDispatchInfo.Throw(error);
                            _context.Settlement.Clear();
                            Failure = _attemptFailure = error;
                            _owner.LogFailure(_context, error);
                            _action = _owner._classifier.Classify(error, _context);
                            _owner._failures!.Record(_context, error);
                        }
                        catch (Exception policyError) { _error = policyError; }
                        continue;
                    }
                    if (_stage == Stage.Handler)
                    {
                        _stage = Stage.Failure;
                        try
                        {
                            // A handler's successful native settlement is final, even when later handler work fails.
                            if (error is OperationCanceledException && _context.CancellationToken.IsCancellationRequested ||
                                _context.ObservedDisposition.Kind == DispositionKind.SettledByHandler ||
                                ReferenceEquals(_context.ScopeCleanupError, error))
                                ExceptionDispatchInfo.Throw(error);
                            _context.Settlement.Clear();
                            Failure = error;
                            _owner.LogFailure(_context, error);
                            _pending = _owner._failure(_context, error, _owner._classifier.Classify(error, _context))
                                .ConfigureAwait(false).GetAwaiter();
                            continue;
                        }
                        catch (Exception failureError) { _error = failureError; }
                        _stage = Stage.Dispose;
                    }
                    else if (_stage is Stage.Failure or Stage.Scope)
                    {
                        _error = error;
                        _stage = Stage.Dispose;
                    }
                    else
                    {
                        Finish(error);
                        return;
                    }
                }
            }
        }

        private void Finish(Exception? error)
        {
            try
            {
                if (_owner._failures is not null)
                {
                    if (Admitted && _owner.Circuit is { } circuit && !OutcomeRecorded)
                        if (circuit.Complete(Probe, true)) _owner.CircuitChanged?.Invoke(true);
                    _context.SendBuffer.Clear();
                }
                else if (_owner.BufferAttemptSends)
                {
                    _context.EndAttemptSender();
                    _context.SendBuffer.Clear();
                }
                try
                {
                    if (error is not null && !(error is OperationCanceledException && _context.CancellationToken.IsCancellationRequested))
                    {
                        if (!ReferenceEquals(Failure, error)) _owner.LogFailure(_context, error);
                        Failure = error;
                    }
                }
                finally
                {
                    try
                    {
                        if (StartedAt != 0) _owner._telemetry!.RecordDuration(Stopwatch.GetElapsedTime(StartedAt), Failure is not null);
                        _owner._telemetry?.RecordCriticalTime(_context);
                    }
                    finally
                    {
                        try
                        {
                            if (Failure is not null) MessageOperationTracing.Fail(TraceActivity, "_OTHER");
                            TraceActivity?.Dispose();
                        }
                        finally { _context.EndProcessing(); }
                    }
                }
            }
            catch (Exception finalError) { error = finalError; }
            _scope = default;
            TraceActivity = null;
            Failure = null;
            _attemptFailure = null;
            _pending = default;
            _executionContext = null;
            _error = null;
            if (_suspended)
            {
                if (error is null) _completion.SetResult(true);
                else _completion.SetException(error);
            }
            else _error = error;
        }

        public void GetResult(short token)
        {
            try { _completion.GetResult(token); }
            finally { _completion.Reset(); }
        }
        public ValueTaskSourceStatus GetStatus(short token) => _completion.GetStatus(token);
        public void OnCompleted(Action<object?> continuation, object? state, short token, ValueTaskSourceOnCompletedFlags flags)
            => _completion.OnCompleted(continuation, state, token, flags);

        private enum Stage
        {
            Start,
            PolicyAttempt, PolicyScope, PolicyHandler, PolicyDispose, PolicyAfterDispose,
            PolicyRequested, PolicyFlush, PolicySuccess, PolicyCopy, PolicyRetry, PolicyComplete,
            Scope, Handler, Failure, Dispose, AfterDispose, Flush, PrepareSettlement, Settlement
        }
        private struct Runner(CoreOperation operation) : IAsyncStateMachine
        {
            public void MoveNext() => operation.MoveNext();
            public void SetStateMachine(IAsyncStateMachine stateMachine) => throw new NotSupportedException();
        }
    }
}
