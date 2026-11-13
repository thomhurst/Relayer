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
        private Activity? _activity;
        private Exception? _failure;
        private Exception? _error;
        private long _start;
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
                            _start = _owner._telemetry is { DurationEnabled: true } ? Stopwatch.GetTimestamp() : 0;
                            _activity = _owner._tracing?.Start(_context);
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
                            if (_failure is null && _context.Settlement.Request is { } request)
                            {
                                _context.FailureDisposition = request;
                                _context.ApplyFailureDisposition();
                            }
                            _context.ClearServices();
                            if (!_owner.BufferAttemptSends) goto case Stage.PrepareSettlement;
                            _stage = Stage.Flush;
                            // Scope disposal is part of the attempt. Failure or a non-complete
                            // settlement must discard its sends, just like ProcessWithFailuresAsync.
                            if (_failure is null &&
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
                            Finish(null);
                            return;
                    }
                }
                catch (Exception error)
                {
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
                            _failure = error;
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
                if (_owner.BufferAttemptSends)
                {
                    _context.EndAttemptSender();
                    _context.SendBuffer.Clear();
                }
                try
                {
                    if (error is not null && !(error is OperationCanceledException && _context.CancellationToken.IsCancellationRequested))
                    {
                        if (!ReferenceEquals(_failure, error)) _owner.LogFailure(_context, error);
                        _failure = error;
                    }
                }
                finally
                {
                    try
                    {
                        if (_start != 0) _owner._telemetry!.RecordDuration(Stopwatch.GetElapsedTime(_start), _failure is not null);
                        _owner._telemetry?.RecordCriticalTime(_context);
                    }
                    finally
                    {
                        try
                        {
                            if (_failure is not null) MessageOperationTracing.Fail(_activity, "_OTHER");
                            _activity?.Dispose();
                        }
                        finally { _context.EndProcessing(); }
                    }
                }
            }
            catch (Exception finalError) { error = finalError; }
            _scope = default;
            _activity = null;
            _failure = null;
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

        private enum Stage { Start, Scope, Handler, Failure, Dispose, AfterDispose, Flush, PrepareSettlement, Settlement }
        private struct Runner(CoreOperation operation) : IAsyncStateMachine
        {
            public void MoveNext() => operation.MoveNext();
            public void SetStateMachine(IAsyncStateMachine stateMachine) => throw new NotSupportedException();
        }
    }
}
