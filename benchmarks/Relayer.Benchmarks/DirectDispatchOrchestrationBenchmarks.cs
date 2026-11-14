using System.Runtime.CompilerServices;
using System.Threading.Tasks.Sources;
using BenchmarkDotNet.Attributes;
using Microsoft.Extensions.Logging.Abstractions;
using Relayer.Outbox;
using Relayer.Outbox.PostgreSql;
using Relayer.Transports;

namespace Relayer.Benchmarks;

/// <summary>Isolates scheduling overhead from native database/broker I/O, which remains provider-owned.</summary>
// Reuse the existing explicit net11 toolchain workaround for BenchmarkDotNet 0.15.8.
[MemoryDiagnoser]
[Config(typeof(PooledBuilderCompletionBenchmarks.Frameworks))]
public class DirectDispatchOrchestrationBenchmarks
{
    private Worker _worker = null!;
    private readonly OutboxDispatchBatch _batch = new();

    [Params(1, 100)]
    public int MessageCount { get; set; }

    [Params(false, true)]
    public bool AsynchronousClaim { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        var source = new List<BufferedOutboxMessage>();
        for (var i = 0; i < MessageCount; i++)
        {
            var row = new BufferedOutboxMessage();
            row.Copy(new(Guid.CreateVersion7(), "events", "benchmark;v=1",
                new("orders", new byte[256]), DateTimeOffset.UtcNow), 1);
            source.Add(row);
        }
        _batch.Take(source, source.Count, DateTimeOffset.UtcNow);
        _worker = new(MessageCount, AsynchronousClaim);
    }

    [GlobalCleanup]
    public void Cleanup() => _worker.Dispose();

    [Benchmark(Baseline = true)]
    public ValueTask InlineGroupLoop() => _worker.InlineAsync(_batch);

    [Benchmark]
    public ValueTask SharedGroupLoop() => _worker.SharedAsync(_batch);

    private sealed class Worker : OutboxDirectDispatchWorker<OutboxDispatchBatch>
    {
        private readonly int _batchSize;
        private readonly Completion? _completion;

        internal Worker(int batchSize, bool asynchronousClaim) : base(1, batchSize, TimeSpan.FromMinutes(1), [new Transport()], NullLogger.Instance, null)
        {
            _batchSize = batchSize;
            if (asynchronousClaim) _completion = new();
        }
        private readonly DirectDispatchCapabilityCache _inlineCapabilities = new();

        internal ValueTask SharedAsync(OutboxDispatchBatch batch) => DispatchAsync(batch, default);

        // The previous dispatch loop, with the native claim/send/completion body represented by
        // completed ValueTasks on both sides. This exposes CPU overhead that real I/O would hide.
        [RuntimeAsyncMethodGeneration(false)]
        [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
        internal async ValueTask InlineAsync(OutboxDispatchBatch batch)
        {
            batch.Sort();
            var offset = 0;
            while (offset < batch.Count)
            {
                var first = batch.Rows[offset];
                ITransport transport;
                int limit;
                try
                {
                    if (!_transports.TryGetValue(first.Transport, out transport!))
                        throw new InvalidOperationException("Missing benchmark transport.");
                    limit = Math.Min(_batchSize, _inlineCapabilities.Get(transport, first.Destination).MaxBatchSize);
                }
                catch (Exception error)
                {
                    LogFailure(error);
                    do { offset++; }
                    while (offset < batch.Count && batch.Rows[offset].Transport == first.Transport &&
                        batch.Rows[offset].Destination == first.Destination);
                    continue;
                }
                var count = 0;
                try
                {
                    PrepareGroup(batch, ref offset, limit, ref count);
                    var now = DateTimeOffset.UtcNow;
                    var expires = now + TimeSpan.FromMinutes(1);
                    await ClaimAsync(count, now, expires, default).ConfigureAwait(false);
                    var live = CompactClaims(_messages.AsSpan(0, count), _claims.AsSpan(0, count),
                        _claimed.AsSpan(0, count), _created.AsSpan(0, count));
                    if (live == 0 || expires <= DateTimeOffset.UtcNow) continue;
                    if (live == 1) await transport.SendAsync(_messages[0], default).ConfigureAwait(false);
                    else await transport.SendBatchAsync(_messages.AsMemory(0, live), default).ConfigureAwait(false);
                    await CompleteAsync(0, live, default).ConfigureAwait(false); // native completion
                }
                catch (Exception error) { LogFailure(error); }
                finally
                {
                    Array.Clear(_messages, 0, count);
                    Array.Clear(_claims, 0, count);
                }
            }
        }

        protected override ValueTask ClaimAsync(int count, DateTimeOffset now, DateTimeOffset expires, CancellationToken cancellationToken) =>
            _completion?.Begin() ?? ValueTask.CompletedTask;
        protected override ValueTask<long> CompleteAsync(int offset, int count, CancellationToken cancellationToken) => new(count);
        protected override ValueTask<bool> RecordSendFailureAsync(OutboxDispatchBatch batch, int index, Exception error,
            CancellationToken cancellationToken, bool propagateDatabaseFailures, bool retryable = true) => throw error;

        protected override void LogFailure(Exception error) => throw new InvalidOperationException("Benchmark failed.", error);
        protected override OutboxDispatchBatch CreateBatch() => new();
        protected override int Count(OutboxDispatchBatch batch) => batch.Count;
        protected override void Append(OutboxDispatchBatch batch, OutboxDispatchBatch source) => batch.Append(source);
        protected override void Sort(OutboxDispatchBatch batch) => batch.Sort();
        protected override void Clear(OutboxDispatchBatch batch) => batch.Clear();
        protected override (string Transport, string Destination) Endpoint(OutboxDispatchBatch batch, int offset) =>
            (batch.Rows[offset].Transport, batch.Rows[offset].Destination);

        protected override void PrepareGroup(OutboxDispatchBatch batch, ref int offset, int limit, ref int count)
        {
            var first = batch.Rows[offset];
            while (offset < batch.Count && count < limit)
            {
                var row = batch.Rows[offset];
                if (row.Transport != first.Transport || row.Destination != first.Destination) break;
                offset++;
                _claimed[count] = true;
                _claims[count] = new(row.Id, Guid.NewGuid());
                _created[count] = row.CreatedAt;
                _messages[count++] = row.ToMessage();
            }
        }

        protected override ValueTask<int> SkipInvalidGroupAsync(OutboxDispatchBatch batch, int offset,
            Exception error, bool recovery, CancellationToken cancellationToken) => throw error;


    }

    // Reusable stand-in for suspended native I/O: neither the work item nor its callback allocates.
    private sealed class Completion : IValueTaskSource, IThreadPoolWorkItem
    {
        private ManualResetValueTaskSourceCore<bool> _source;

        internal ValueTask Begin()
        {
            _source.Reset();
            var result = new ValueTask(this, _source.Version);
            ThreadPool.UnsafeQueueUserWorkItem(this, preferLocal: false);
            return result;
        }

        public void Execute() => _source.SetResult(true);
        public void GetResult(short token) => _source.GetResult(token);
        public ValueTaskSourceStatus GetStatus(short token) => _source.GetStatus(token);
        public void OnCompleted(Action<object?> continuation, object? state, short token, ValueTaskSourceOnCompletedFlags flags) =>
            _source.OnCompleted(continuation, state, token, flags);
    }

    private sealed class Transport : ITransport
    {
        public string Name => "events";
        public TransportCapabilities GetCapabilities(string endpoint) => new() { MaxBatchSize = 100 };
        public ValueTask SendAsync(OutgoingMessage message, CancellationToken cancellationToken) => ValueTask.CompletedTask;
        public ValueTask SendBatchAsync(ReadOnlyMemory<OutgoingMessage> messages, CancellationToken cancellationToken) => ValueTask.CompletedTask;
        public ValueTask<TopologyReport> ApplyTopologyAsync(TopologyMode mode, CancellationToken cancellationToken) => throw new NotSupportedException();
        public Task StartAsync(CancellationToken cancellationToken) => Task.CompletedTask;
        public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }
}
