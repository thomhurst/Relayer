using System.Collections;
using System.Collections.Frozen;
using System.Runtime.CompilerServices;
using Relayer.Serialization;

namespace Relayer.Pipeline;

internal abstract class BatchDispatcher
{
    internal abstract ValueTask InvokeAsync(ReadOnlyMemory<DefaultMessageContext> contexts,
        Memory<Exception?> errors, CancellationToken cancellationToken);
}

internal sealed class BatchDispatcher<TMessage> : BatchDispatcher
{
    private readonly BatchHandlerDescriptor<TMessage> _descriptor;
    private readonly SerializedReader<TMessage> _reader;
    private readonly FrozenDictionary<MessageType, UpcastReader<TMessage>>? _upcastReaders;
    private readonly SlotPool<State> _states;
    private readonly int _size;

    internal BatchDispatcher(BatchHandlerDescriptor<TMessage> descriptor, IMessageSerializer serializer, int size, int concurrency,
        Dictionary<MessageType, UpcastReader> readers)
    {
        _descriptor = descriptor;
        _reader = new(serializer);
        if (readers.Count != 0)
            _upcastReaders = readers.ToFrozenDictionary(pair => pair.Key, pair => (UpcastReader<TMessage>)pair.Value);
        _size = size;
        _states = new(concurrency, concurrency);
        for (var i = 0; i < concurrency; i++) _states.TryReturn(new State(size), i);
    }

    [RuntimeAsyncMethodGeneration(false)]
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    internal override async ValueTask InvokeAsync(ReadOnlyMemory<DefaultMessageContext> contexts,
        Memory<Exception?> errors, CancellationToken cancellationToken)
    {
        var state = Rent();
        state.Begin(errors);
        try
        {
            for (var i = 0; i < contexts.Length; i++)
            {
                var context = contexts.Span[i];
                try
                {
                    var message = _upcastReaders is { } readers && readers.TryGetValue(context.MessageType, out var reader)
                        ? reader.Read(context) : _reader.Read(context);
                    state.Items[state.Count] = new(message, context, state, state.Generation, i);
                    state.Indexes[state.Count++] = i;
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested) { throw; }
                catch (Exception error) { errors.Span[i] = error; }
            }
            if (state.Count == 0) return;
            BatchResult result;
            try
            {
                var handler = _descriptor.Resolve(contexts.Span[0].Services);
                result = await _descriptor.DispatchAsync(handler, state.Batch, cancellationToken).ConfigureAwait(false);
                ArgumentNullException.ThrowIfNull(result);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested) { throw; }
            catch (Exception error)
            {
                for (var i = 0; i < state.Count; i++) errors.Span[state.Indexes[i]] = error;
                return;
            }
            if (result.Error is { } failure)
                for (var i = 0; i < state.Count; i++) errors.Span[state.Indexes[i]] = failure;
        }
        finally
        {
            state.End();
            Array.Clear(state.Items, 0, state.Count);
            state.Count = 0;
            _states.TryReturn(state);
        }
    }

    private State Rent()
    {
        if (_states.TryRent() is { } state) return state;
        return new State(_size);
    }

    private sealed class State : IReadOnlyList<BatchItem<TMessage>>, IBatchItemFailureTarget
    {
        private Memory<Exception?> _errors;
        internal long Generation { get; private set; }
        internal void Begin(Memory<Exception?> errors)
        {
            Generation = checked(Generation + 1);
            _errors = errors;
        }
        internal void End() => _errors = default;
        public void Fail(long generation, int index, Exception exception)
        {
            if (generation != Generation || _errors.IsEmpty)
                throw new InvalidOperationException("A batch item can only be marked as failed during the handler invocation that supplied it.");
            _errors.Span[index] = exception;
        }
        internal readonly BatchItem<TMessage>[] Items;
        internal readonly int[] Indexes;
        internal readonly Batch<TMessage> Batch;
        internal State(int size) { Items = new BatchItem<TMessage>[size]; Indexes = new int[size]; Batch = new(this); }
        public int Count { get; internal set; }
        public BatchItem<TMessage> this[int index] => (uint)index < (uint)Count ? Items[index] : throw new ArgumentOutOfRangeException(nameof(index));
        public IEnumerator<BatchItem<TMessage>> GetEnumerator()
        {
            for (var i = 0; i < Count; i++) yield return Items[i];
        }
        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
    }
}
