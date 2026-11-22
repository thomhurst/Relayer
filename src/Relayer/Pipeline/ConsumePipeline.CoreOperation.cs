using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.ExceptionServices;
using System.Threading.Tasks.Sources;
using Microsoft.Extensions.DependencyInjection;
using Relayer.Transports;

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
        private Activity? _traceActivity;
        private Exception? _failure;
        private Exception? _error;
        private long _startedAt;
        private bool _admitted;
        private bool _probe;
        private bool _outcomeRecorded;
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

            if (_suspended)
            {
                return new ValueTask(this, _completion.Version);
            }

            var error = _error;
            _error = null;
            return error is null ? default : ValueTask.FromException(error);
        }

        private void Resume()
        {
            var executionContext = _executionContext;
            _executionContext = null;

            if (executionContext is not null)
            {
                ExecutionContext.Run(executionContext, static state => ((CoreOperation)state!).MoveNext(), this);
            }
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
                                _admitted = _probe = _outcomeRecorded = false;
                                _retry = 0;
                                _initialize = true;
                                _startedAt = _owner._telemetry is { DurationEnabled: true } ? Stopwatch.GetTimestamp() : 0;

                                if (_owner.Circuit is { } policyCircuit)
                                {
                                    _admitted = policyCircuit.TryEnter(out _probe);

                                    if (!_admitted)
                                    {
                                        _context.FailureDisposition = Disposition.Redeliver(TimeSpan.Zero);
                                        _context.ApplyFailureDisposition();
                                        // Native lock expiry returns rejected Service Bus callbacks.
                                        Finish(null);
                                        return;
                                    }
                                }

                                _traceActivity = _owner._tracing?.Start(_context);
                                _owner._telemetry?.RecordConsumed();
                                goto case Stage.PolicyAttempt;
                            }

                            _startedAt = _owner._telemetry is { DurationEnabled: true } ? Stopwatch.GetTimestamp() : 0;
                            _traceActivity = _owner._tracing?.Start(_context);
                            _owner._telemetry?.RecordConsumed();
                            _context.CancellationToken.ThrowIfCancellationRequested();
                            _scope = new AsyncServiceScope(_owner._scopes.CreateScope());
                            _stage = Stage.Scope;
                            _context.SetServices(_scope.ServiceProvider);
                            if (_owner.BufferAttemptSends)
                            {
                                _context.AttachSender(_scope.ServiceProvider);
                            }

                            _stage = Stage.Handler;
                            _context.Dispatch = _owner.ResolveDispatch(_context);
                            _pending = _owner._pipeline(_context).ConfigureAwait(false).GetAwaiter();
                            goto case Stage.Handler;
                        case Stage.PolicyAttempt:
                            _stage = Stage.PolicyAttempt;
                            _context.CancellationToken.ThrowIfCancellationRequested();
                            _attemptFailure = null;
                            _action = FailureAction.Retry;
                            if (!_owner._scopesOnDemand)
                            {
                                _context.CreateScope();
                            }

                            _stage = Stage.PolicyScope;
                            if (_owner._scopesOnDemand)
                            {
                                _context.DemandScope();
                            }
                            else
                            {
                                _context.BindScope();
                            }

                            _stage = Stage.PolicyHandler;
                            if (_initialize)
                            {
                                _initialize = false;
                                _context.InitializeAttempts();
                            }

                            if (!_owner._scopesOnDemand)
                            {
                                _context.AttachSender();
                            }

                            _context.Dispatch = _owner.ResolveDispatch(_context);
                            _pending = _owner._pipeline(_context).ConfigureAwait(false).GetAwaiter();
                            goto case Stage.PolicyHandler;
                        case Stage.PolicyHandler:
                            if (!AwaitPending())
                            {
                                return;
                            }

                            goto case Stage.PolicyDispose;
                        case Stage.PolicyDispose:
                            _stage = Stage.PolicyAfterDispose;
                            try
                            {
                                _pending = _context.DisposeScopeAsync().ConfigureAwait(false).GetAwaiter();
                            }
                            catch
                            {
                                _context.EndAttemptSender();
                                throw;
                            }

                            goto case Stage.PolicyAfterDispose;
                        case Stage.PolicyAfterDispose:
                            try
                            {
                                if (!AwaitPending())
                                {
                                    return;
                                }
                            }
                            catch
                            {
                                _context.EndAttemptSender();
                                throw;
                            }

                            _context.EndAttemptSender();
                            if (_error is { } policyError)
                            {
                                ExceptionDispatchInfo.Throw(policyError);
                            }

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

                            if (!Enum.IsDefined(_action))
                            {
                                throw new InvalidOperationException("IFailureClassifier returned an undefined FailureAction.");
                            }

                            if (_owner.Circuit is { } failedCircuit && (_action == FailureAction.Retry || !_owner.CircuitCountsRetryFailuresOnly))
                            {
                                _outcomeRecorded = true;

                                if (failedCircuit.Complete(_probe, true))
                                {
                                    _owner.CircuitChanged?.Invoke(true);
                                }

                                if (failedCircuit.State != 0)
                                {
                                    _context.FailureDisposition = Disposition.Redeliver(TimeSpan.Zero);
                                    _context.ApplyFailureDisposition();
                                    goto case Stage.PolicyComplete;
                                }
                            }

                            _stage = Stage.PolicyCopy;
                            _pending = _owner._failures!.HandleAsync(_context, _attemptFailure, _action).ConfigureAwait(false).GetAwaiter();
                            goto case Stage.PolicyCopy;
                        case Stage.PolicyRequested:
                            if (!AwaitPending())
                            {
                                return;
                            }

                            _context.ApplyFailureDisposition();
                            goto case Stage.PolicyComplete;
                        case Stage.PolicyFlush:
                            if (!AwaitPending())
                            {
                                return;
                            }

                            if (_context.Settlement.Request is { } complete)
                            {
                                _context.FailureDisposition = complete;
                                _context.ApplyFailureDisposition();
                            }

                            _failure = null;
                            goto case Stage.PolicyComplete;
                        case Stage.PolicyCopy:
                            if (!AwaitPending())
                            {
                                return;
                            }

                            _context.ApplyFailureDisposition();
                            goto case Stage.PolicyComplete;
                        case Stage.PolicyRetry:
                            if (!AwaitPending())
                            {
                                return;
                            }

                            _context.NextAttempt();
                            goto case Stage.PolicyAttempt;
                        case Stage.PolicyComplete:
                            _context.ClearServices();
                            goto case Stage.PrepareSettlement;
                        case Stage.Handler:
                        case Stage.Failure:
                            if (!AwaitPending())
                            {
                                return;
                            }

                            _stage = Stage.Dispose;
                            goto case Stage.Dispose;
                        case Stage.Dispose:
                            _stage = Stage.AfterDispose;
                            _pending = _scope.DisposeAsync().ConfigureAwait(false).GetAwaiter();
                            goto case Stage.AfterDispose;
                        case Stage.AfterDispose:
                            if (!AwaitPending())
                            {
                                return;
                            }

                            if (_owner.BufferAttemptSends)
                            {
                                _context.EndAttemptSender();
                            }

                            if (_error is { } error)
                            {
                                ExceptionDispatchInfo.Throw(error);
                            }

                            _context.CancellationToken.ThrowIfCancellationRequested();
                            if (_failure is null && _context.Settlement.Request is { } request)
                            {
                                _context.FailureDisposition = request;
                                _context.ApplyFailureDisposition();
                            }

                            _context.ClearServices();
                            if (!_owner.BufferAttemptSends)
                            {
                                goto case Stage.PrepareSettlement;
                            }

                            _stage = Stage.Flush;
                            // Scope disposal is part of the attempt. Failure or a non-complete
                            // settlement must discard its sends, just like the policy path.
                            if (_failure is null &&
                                (_context.Settlement.Request is not { } settlement || settlement.Kind == DispositionKind.Complete))
                            {
                                _pending = _context.SendBuffer.FlushAsync(_context.CancellationToken).ConfigureAwait(false).GetAwaiter();
                            }
                            else
                            {
                                _context.SendBuffer.Clear();
                                _pending = default;
                            }

                            goto case Stage.Flush;
                        case Stage.Flush:
                            if (!AwaitPending())
                            {
                                return;
                            }

                            goto case Stage.PrepareSettlement;
                        case Stage.PrepareSettlement:
                            _stage = Stage.Settlement;
                            _pending = (_owner._settlement?.Invoke(_context) ?? default).ConfigureAwait(false).GetAwaiter();
                            goto case Stage.Settlement;
                        case Stage.Settlement:
                            if (!AwaitPending())
                            {
                                return;
                            }

                            if (_owner._failures is not null && _owner.Circuit is { } circuit && !_outcomeRecorded)
                            {
                                _outcomeRecorded = true;
                                var opened = circuit.Complete(_probe, false);

                                if (opened || _probe)
                                {
                                    _owner.CircuitChanged?.Invoke(opened);
                                }
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
                            {
                                ExceptionDispatchInfo.Throw(error);
                            }

                            _context.Settlement.Clear();
                            _failure = _attemptFailure = error;
                            _owner.LogFailure(_context, error);
                            _action = _owner._classifier.Classify(error, _context);
                            _owner._failures!.Record(_context, error);
                        }
                        catch (Exception policyError)
                        {
                            _error = policyError;
                        }

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
                            {
                                ExceptionDispatchInfo.Throw(error);
                            }

                            _context.Settlement.Clear();
                            _failure = error;
                            _owner.LogFailure(_context, error);
                            _pending = _owner._failure(_context, error, _owner._classifier.Classify(error, _context))
                                .ConfigureAwait(false).GetAwaiter();
                            continue;
                        }
                        catch (Exception failureError)
                        {
                            _error = failureError;
                        }

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

        // Called during settlement when a transport logs the failure and leaves redelivery to the broker.
        internal void RecordSettlementFailure(Exception error)
        {
            _failure = error;
        }

        private void Finish(Exception? error)
        {
            try
            {
                if (_owner._failures is not null)
                {
                    if (_admitted && _owner.Circuit is { } circuit && !_outcomeRecorded)
                    {
                        if (circuit.Complete(_probe, true))
                        {
                            _owner.CircuitChanged?.Invoke(true);
                        }
                    }

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
                        if (!ReferenceEquals(_failure, error) && !(error is ConsumeFailureHandoff && ReferenceEquals(error.InnerException, _failure)))
                        {
                            _owner.LogFailure(_context, error);
                        }

                        _failure = error;
                    }
                }
                finally
                {
                    try
                    {
                        if (_startedAt != 0)
                        {
                            _owner._telemetry!.RecordDuration(Stopwatch.GetElapsedTime(_startedAt), _failure is not null);
                        }

                        _owner._telemetry?.RecordCriticalTime(_context);
                    }
                    finally
                    {
                        try
                        {
                            if (_failure is not null)
                            {
                                MessageOperationTracing.Fail(_traceActivity, "_OTHER");
                            }

                            _traceActivity?.Dispose();
                        }
                        finally
                        {
                            _context.EndProcessing();
                        }
                    }
                }
            }
            catch (Exception finalError)
            {
                error = finalError;
            }

            _scope = default;
            _traceActivity = null;
            _failure = null;
            _attemptFailure = null;
            _pending = default;
            _executionContext = null;
            _error = null;

            if (_suspended)
            {
                if (error is null)
                {
                    _completion.SetResult(true);
                }
                else
                {
                    _completion.SetException(error);
                }
            }
            else
            {
                _error = error;
            }
        }

        public void GetResult(short token)
        {
            try
            {
                _completion.GetResult(token);
            }
            finally
            {
                _completion.Reset();
            }
        }

        public ValueTaskSourceStatus GetStatus(short token)
        {
            return _completion.GetStatus(token);
        }

        public void OnCompleted(Action<object?> continuation, object? state, short token, ValueTaskSourceOnCompletedFlags flags)
        {
            _completion.OnCompleted(continuation, state, token, flags);
        }

        private enum Stage
        {
            Start,
            PolicyAttempt,
            PolicyScope,
            PolicyHandler,
            PolicyDispose,
            PolicyAfterDispose,
            PolicyRequested,
            PolicyFlush,
            PolicyCopy,
            PolicyRetry,
            PolicyComplete,
            Scope,
            Handler,
            Failure,
            Dispose,
            AfterDispose,
            Flush,
            PrepareSettlement,
            Settlement
        }

        private struct Runner(CoreOperation operation) : IAsyncStateMachine
        {
            public void MoveNext()
            {
                operation.MoveNext();
            }

            public void SetStateMachine(IAsyncStateMachine stateMachine)
            {
                throw new NotSupportedException();
            }
        }
    }
}
