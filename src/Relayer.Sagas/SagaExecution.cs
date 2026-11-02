using System.Runtime.CompilerServices;
using System.Threading.Tasks.Sources;

namespace Relayer.Sagas;

/// <summary>Reusable typed execution state. Generated Handler registration adds one singleton per state/message pair.</summary>
/// <remarks>Applications normally use Saga rather than calling this infrastructure directly.</remarks>
public sealed class SagaExecution<TState, TMessage> where TState : SagaData
{
    // Same peak-retaining, cross-thread pool as SendRoute; no worker-local async state-machine cache.
    private readonly Segment _operations = new();

    internal ValueTask Start(Saga<TState, TMessage> handler, TMessage message, MessageContext context, CancellationToken cancellation)
    {
        for (var segment = _operations; segment is not null; segment = Volatile.Read(ref segment.Next))
            for (var i = 0; i < segment.Items.Length; i++)
                if (Interlocked.Exchange(ref segment.Items[i], null) is { } operation)
                    return operation.Start(handler, message, context, cancellation);
        return new Operation(this).Start(handler, message, context, cancellation);
    }

    private void Return(Operation operation)
    {
        var segment = _operations;
        while (true)
        {
            for (var i = 0; i < segment.Items.Length; i++)
                if (Interlocked.CompareExchange(ref segment.Items[i], operation, null) is null) return;
            var next = Volatile.Read(ref segment.Next);
            if (next is null)
            {
                var added = new Segment();
                next = Interlocked.CompareExchange(ref segment.Next, added, null) ?? added;
            }
            segment = next;
        }
    }

    private sealed class Segment
    {
        internal readonly Operation?[] Items = new Operation?[Math.Max(1, Environment.ProcessorCount)];
        internal Segment? Next;
    }

    private sealed class Operation : IValueTaskSource
    {
        private readonly SagaExecution<TState, TMessage> _owner;
        private readonly Action _continuation;
        private ManualResetValueTaskSourceCore<bool> _completion;
        private ConfiguredValueTaskAwaitable<TState?>.ConfiguredValueTaskAwaiter _load;
        private ConfiguredValueTaskAwaitable.ConfiguredValueTaskAwaiter _pending;
        private ExecutionContext? _executionContext;
        private Saga<TState, TMessage> _handler = null!;
        private TMessage _message = default!;
        private MessageContext _context = null!;
        private CancellationToken _cancellation;
        private TState _state = null!;
        private Stage _stage;
        private bool _isNew;
        private bool _suspended;
        private Exception? _error;

        internal Operation(SagaExecution<TState, TMessage> owner)
        {
            _owner = owner;
            _continuation = Resume;
        }

        internal ValueTask Start(Saga<TState, TMessage> handler, TMessage message, MessageContext context, CancellationToken cancellation)
        {
            _handler = handler;
            _message = message;
            _context = context;
            _cancellation = cancellation;
            _stage = Stage.Start;
            _suspended = false;
            _completion.Reset();
            // Match CoreOperation's ExecutionContext restoration around synchronous application code.
            var runner = new Runner(this);
            var builder = AsyncTaskMethodBuilder.Create();
            builder.Start(ref runner);
            if (_suspended) return new ValueTask(this, _completion.Version);
            var error = _error;
            _error = null;
            _owner.Return(this);
            return error is null ? default : ValueTask.FromException(error);
        }

        private void Resume()
        {
            var executionContext = _executionContext;
            _executionContext = null;
            if (executionContext is not null)
                ExecutionContext.Run(executionContext, static state => ((Operation)state!).MoveNext(), this);
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
                _suspended = true;
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
            try
            {
                switch (_stage)
                {
                    case Stage.Start:
                        _handler.Validate();
                        _stage = Stage.Load;
                        _load = _handler.Load(_message, _cancellation).ConfigureAwait(false).GetAwaiter();
                        goto case Stage.Load;
                    case Stage.Load:
                        if (!_load.IsCompleted)
                        {
                            _suspended = true;
                            _executionContext = ExecutionContext.Capture();
                            _load.UnsafeOnCompleted(_continuation);
                            return;
                        }
                        var state = _load.GetResult();
                        _load = default;
                        if (state is { IsCompleted: true }) { Finish(null); return; }
                        _isNew = state is null;
                        _state = state ?? _handler.New(_message);
                        if (_isNew && _state.Id == Guid.Empty)
                            throw new InvalidOperationException("A new saga state requires a non-empty correlation Id.");
                        _stage = Stage.Transition;
                        _pending = _handler.Apply(_state, _message, _context, _cancellation).ConfigureAwait(false).GetAwaiter();
                        goto case Stage.Transition;
                    case Stage.Transition:
                        if (!AwaitPending()) return;
                        _stage = Stage.Save;
                        _pending = _handler.Save(_state, _isNew, _cancellation).ConfigureAwait(false).GetAwaiter();
                        goto case Stage.Save;
                    case Stage.Save:
                        if (!AwaitPending()) return;
                        Finish(null);
                        return;
                }
            }
            catch (Exception error) { Finish(error); }
        }

        private void Finish(Exception? error)
        {
            _handler = null!;
            _message = default!;
            _context = null!;
            _state = null!;
            _cancellation = default;
            _pending = default;
            _load = default;
            _executionContext = null;
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
            finally
            {
                _completion.Reset();
                _owner.Return(this);
            }
        }
        public ValueTaskSourceStatus GetStatus(short token) => _completion.GetStatus(token);
        public void OnCompleted(Action<object?> continuation, object? state, short token, ValueTaskSourceOnCompletedFlags flags)
            => _completion.OnCompleted(continuation, state, token, flags);

        private enum Stage { Start, Load, Transition, Save }
        private struct Runner(Operation operation) : IAsyncStateMachine
        {
            public void MoveNext() => operation.MoveNext();
            public void SetStateMachine(IAsyncStateMachine stateMachine) => throw new NotSupportedException();
        }
    }
}
