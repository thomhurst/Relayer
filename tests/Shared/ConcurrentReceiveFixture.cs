using System.Buffers;
using System.Threading.Tasks.Sources;
using Microsoft.Extensions.DependencyInjection;
using Relayer.Serialization;

namespace Relayer.TestFixtures;

// Shared by the allocation regression test and BenchmarkDotNet. No fixture Task or async builder
// participates in completion. Each invocation holds eight handlers pending before another thread releases them.
public sealed class ConcurrentReceiveFixture : IDisposable
{
    public const int Count = 8;
    private readonly ServiceProvider _provider;
    private readonly ConsumePipeline _consumer;
    private readonly ReceivePipeline<Incoming, Binding> _receive;
    private readonly Incoming _incoming = new();
    private readonly ConsumeMessage _message = new() { MessageId = "id", MessageType = new("pending", 1), Body = ReadOnlySequence<byte>.Empty };
    private readonly Handler _handler = new();
    private readonly ValueTask[] _pending = new ValueTask[Count];
    private readonly ValueTask<Disposition>[] _received = new ValueTask<Disposition>[Count];
    private readonly DefaultMessageContext[] _contexts = new DefaultMessageContext[Count];
    private readonly AsyncServiceScope[] _scopes = new AsyncServiceScope[Count];
    private readonly AutoResetEvent[] _release = [new(false), new(false)];
    private readonly AutoResetEvent _finished = new(false);
    private readonly Thread[] _workers;
    private bool _stopping;
    private int _nextWorker;
    private long _workerBytes;
    public long CallerBytes { get; private set; }
    public long WorkerBytes => _workerBytes;

    public ConcurrentReceiveFixture()
    {
        var services = new ServiceCollection();
        services.AddSingleton(_handler);
        services.AddRelayer(r => { r.MessageTypes.Add<Payload>("pending"); r.Handler<Handler>(); });
        _provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true });
        _consumer = new ConsumePipelineBuilder(_provider.GetRequiredService<IServiceScopeFactory>(),
            _provider.GetRequiredService<MessageTypeRegistry>(), new Serializer(),
            static (_, error, _) => ValueTask.FromException(error))
            .Handler(_provider.GetRequiredService<HandlerRegistry>().GetHandler<Payload, Handler>()).Build(Count);
        _receive = new(_consumer, new HeaderNames(), Count);
        _workers = [new(() => Work(0)), new(() => Work(1))];
        foreach (var worker in _workers) { worker.IsBackground = true; worker.Start(); }
    }

    public enum Mode { Scoped, Consumer, Receive }

    // Return exact inclusive allocations from the driver and the completing worker. MemoryDiagnoser
    // remains enabled separately; this counter also covers work on the dedicated completion thread.
    public long Run(Mode mode)
    {
        var start = GC.GetAllocatedBytesForCurrentThread();
        for (var i = 0; i < Count; i++)
        {
            if (mode == Mode.Receive)
            {
                _received[i] = _receive.ProcessAsync(_incoming);
                if (_received[i].IsCompleted) throw new InvalidOperationException("Receive must suspend.");
            }
            else if (mode == Mode.Consumer)
            {
                var context = _consumer.RentContext(_message);
                _contexts[i] = context;
                _pending[i] = _consumer.ProcessAsync(context);
                if (_pending[i].IsCompleted) throw new InvalidOperationException("Consumer must suspend.");
            }
            else
            {
                _scopes[i] = _provider.CreateAsyncScope();
                _pending[i] = _scopes[i].ServiceProvider.GetRequiredService<Handler>()
                    .HandleAsync(Serializer.Value, null!, CancellationToken.None);
                if (_pending[i].IsCompleted) throw new InvalidOperationException("Handler must suspend.");
            }
        }
        _release[_nextWorker].Set();
        _nextWorker ^= 1;
        if (!_finished.WaitOne(TimeSpan.FromSeconds(30))) throw new TimeoutException("Completion worker did not finish.");
        for (var i = 0; i < Count; i++)
        {
            if (mode == Mode.Receive)
            {
                if (!_received[i].IsCompleted) throw new InvalidOperationException("Receive did not complete.");
                if (_received[i].GetAwaiter().GetResult().Kind != DispositionKind.Complete)
                    throw new InvalidOperationException("Unexpected settlement.");
                _received[i] = default;
            }
            else
            {
                if (!_pending[i].IsCompleted) throw new InvalidOperationException("Consumer did not complete.");
                _pending[i].GetAwaiter().GetResult();
                _pending[i] = default;
                if (mode == Mode.Consumer)
                {
                    _contexts[i].Dispose();
                    _contexts[i] = null!;
                }
                else
                {
                    var disposal = _scopes[i].DisposeAsync();
                    if (!disposal.IsCompletedSuccessfully) throw new InvalidOperationException("Empty native scope must dispose synchronously.");
                    disposal.GetAwaiter().GetResult();
                    _scopes[i] = default;
                }
            }
        }
        CallerBytes = GC.GetAllocatedBytesForCurrentThread() - start;
        return CallerBytes + _workerBytes;
    }

    private void Work(int index)
    {
        while (true)
        {
            _release[index].WaitOne();
            if (_stopping) return;
            var start = GC.GetAllocatedBytesForCurrentThread();
            foreach (var source in _handler.Sources) source.Complete();
            _workerBytes = GC.GetAllocatedBytesForCurrentThread() - start;
            _finished.Set();
        }
    }

    public void Dispose()
    {
        _stopping = true;
        foreach (var release in _release) release.Set();
        foreach (var worker in _workers) worker.Join();
        foreach (var release in _release) release.Dispose();
        _finished.Dispose();
        _provider.Dispose();
    }

    [Message("pending")]
    public sealed class Payload;
    public sealed class Handler : IHandler<Payload>
    {
        internal readonly Source[] Sources = Enumerable.Range(0, Count).Select(_ => new Source()).ToArray();
        private int _next;
        public ValueTask HandleAsync(Payload message, MessageContext context, CancellationToken cancellationToken)
        {
            var source = Sources[_next];
            _next = (_next + 1) % Count;
            return source.Wait();
        }
    }
    internal sealed class Source : IValueTaskSource
    {
        private ManualResetValueTaskSourceCore<bool> _completion;
        internal ValueTask Wait() { _completion.Reset(); return new ValueTask(this, _completion.Version); }
        internal void Complete() => _completion.SetResult(true);
        public void GetResult(short token) => _completion.GetResult(token);
        public ValueTaskSourceStatus GetStatus(short token) => _completion.GetStatus(token);
        public void OnCompleted(Action<object?> continuation, object? state, short token, ValueTaskSourceOnCompletedFlags flags)
            => _completion.OnCompleted(continuation, state, token, flags);
    }
    private sealed class Serializer : IMessageSerializer
    {
        internal static readonly Payload Value = new();
        public string ContentType => "test/cached";
        public TMessage Deserialize<TMessage>(ReadOnlySequence<byte> source) => (TMessage)(object)Value;
        public void Serialize<TMessage>(TMessage message, IBufferWriter<byte> destination) => throw new NotSupportedException();
    }
    private sealed class Incoming;
    private readonly struct Binding : IMessageBinding<Incoming>
    {
        public static ReadOnlySequence<byte> GetBody(Incoming message) => ReadOnlySequence<byte>.Empty;
        public static int? GetDeliveryCount(Incoming message) => 1;
        public static bool TryGetHeader(Incoming message, string name, out WireString value)
        {
            value = name switch { "ce_id" => new("id"), "ce_type" => new("pending;v=1"), _ => default };
            return !value.IsEmpty;
        }
    }
}
