using System.Collections;
using System.Collections.Frozen;
using System.Runtime.CompilerServices;
using Relayer.Serialization;

namespace Relayer;

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
    private readonly State?[] _states;
    private readonly int _size;

    internal BatchDispatcher(BatchHandlerDescriptor<TMessage> descriptor, IMessageSerializer serializer, int size, int concurrency,
        Dictionary<MessageType, UpcastReader> readers)
    {
        _descriptor = descriptor;
        _reader = new(serializer);
        if (readers.Count != 0)
            _upcastReaders = readers.ToFrozenDictionary(pair => pair.Key, pair => (UpcastReader<TMessage>)pair.Value);
        _size = size;
        _states = new State[concurrency];
        for (var i = 0; i < concurrency; i++) _states[i] = new State(size);
    }

    [RuntimeAsyncMethodGeneration(false)]
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    internal override async ValueTask InvokeAsync(ReadOnlyMemory<DefaultMessageContext> contexts,
        Memory<Exception?> errors, CancellationToken cancellationToken)
    {
        var state = Rent();
        try
        {
            for (var i = 0; i < contexts.Length; i++)
            {
                var context = contexts.Span[i];
                try
                {
                    var message = _upcastReaders is { } readers && readers.TryGetValue(context.MessageType, out var reader)
                        ? reader.Read(context) : _reader.Read(context);
                    state.Items[state.Count] = new(message, context);
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
                if (result.Failures.Count == 0) return;
                foreach (var failure in result.Failures)
                    if ((uint)failure.Key >= (uint)state.Count || failure.Value is null)
                        throw new InvalidOperationException("BatchResult failures must contain valid batch indexes and non-null exceptions.");
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested) { throw; }
            catch (Exception error)
            {
                for (var i = 0; i < state.Count; i++) errors.Span[state.Indexes[i]] = error;
                return;
            }
            foreach (var failure in result.Failures) errors.Span[state.Indexes[failure.Key]] = failure.Value;
        }
        finally
        {
            Array.Clear(state.Items, 0, state.Count);
            state.Count = 0;
            for (var i = 0; i < _states.Length; i++)
                if (Interlocked.CompareExchange(ref _states[i], state, null) is null) break;
        }
    }

    private State Rent()
    {
        for (var i = 0; i < _states.Length; i++)
            if (Interlocked.Exchange(ref _states[i], null) is { } state) return state;
        return new State(_size);
    }

    private sealed class State : IReadOnlyList<BatchItem<TMessage>>
    {
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
