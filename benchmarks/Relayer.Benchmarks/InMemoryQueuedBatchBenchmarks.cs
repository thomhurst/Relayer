using System.Runtime.CompilerServices;
using System.Threading.Channels;
using BenchmarkDotNet.Attributes;
using Relayer.InMemory;
using Relayer.Transports;

namespace Relayer.Benchmarks;

/// <summary>Measures native queue ownership and dispatch with 64 messages simultaneously in flight.</summary>
/// <remarks>All results are per 64-message batch. Divide time and bytes by 64 for per-message costs.</remarks>
[MemoryDiagnoser]
public class InMemoryQueuedBatchBenchmarks
{
    private const int BatchSize = 64;
    private readonly OutgoingMessage[] _batch = new OutgoingMessage[BatchSize];
    private readonly Channel<Disposition> _release = Channel.CreateBounded<Disposition>(1);
    private readonly Channel<int> _received = Channel.CreateBounded<int>(1);
    private readonly Channel<byte> _control = Channel.CreateBounded<byte>(BatchSize + 1);
    private InMemoryTransport? _transport;
    private Task? _controlLoop;
    private int _count;

    [GlobalSetup(Target = nameof(QueuedBatchRoundTrip))]
    public Task Setup() => SetupNative(Receive);

    [GlobalSetup(Target = nameof(QueuedBatchWithPooledCallback))]
    public Task SetupPooledCallback() => SetupNative(ReceiveWithPooledCallback);

    private async Task SetupNative(InMemoryReceiveDelegate receive)
    {
        var body = new byte[] { 42 };
        for (var i = 0; i < _batch.Length; i++) _batch[i] = new("orders", body);
        _transport = new("native", new Dictionary<string, InMemoryEndpointOptions>
        {
            ["orders"] = new() { Capacity = BatchSize, MessagePoolCapacity = BatchSize, Capabilities = new() { MaxBatchSize = BatchSize } },
        });
        _transport.RegisterConsumer("orders", receive);
        await _transport.StartAsync(default);
    }

    [GlobalSetup(Target = nameof(ChannelBatchControl))]
    public void SetupChannelControl() => _controlLoop = Task.Run(RunChannelControl);

    private async Task RunChannelControl()
    {
        try
        {
            while (true)
            {
                await _control.Reader.ReadAsync();
                await Receive(default, default);
            }
        }
        catch (ChannelClosedException) { }
    }

    private ValueTask<Disposition> Receive(InMemoryDelivery delivery, CancellationToken cancellationToken)
    {
        // Return the gate's source directly: a pooled async callback can allocate on worker migration.
        // The receiver cannot fetch another message until this disposition completes.
        if (++_count == 1) return _release.Reader.ReadAsync();
        if (_count == BatchSize)
        {
            _count = 0;
            if (!_received.Writer.TryWrite(BatchSize)) throw new InvalidOperationException("Completion channel is full.");
        }
        return new(Disposition.Complete);
    }

    // Attribution control for the original fixture's extra pooled async continuation.
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    private async ValueTask<Disposition> ReceiveWithPooledCallback(InMemoryDelivery delivery, CancellationToken cancellationToken) =>
        await Receive(delivery, cancellationToken);

    [Benchmark(Description = "Native queue (64 messages/batch)")]
    public ValueTask<int> QueuedBatchRoundTrip() => RoundTrip();

    [Benchmark(Description = "Native queue + pooled callback (64 messages/batch)")]
    public ValueTask<int> QueuedBatchWithPooledCallback() => RoundTrip();

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    private async ValueTask<int> RoundTrip()
    {
        await _transport!.SendBatchAsync(_batch, default);
        if (!_release.Writer.TryWrite(Disposition.Complete)) throw new InvalidOperationException("Release channel is full.");
        return await _received.Reader.ReadAsync();
    }

    // Same gated 64-item channel drain, without native admission, copying, pooling or settlement.
    [Benchmark(Description = "Channel control (64 items/batch)")]
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    public async ValueTask<int> ChannelBatchControl()
    {
        for (var i = 0; i < BatchSize; i++)
            if (!_control.Writer.TryWrite(0)) throw new InvalidOperationException("Control channel is full.");
        if (!_release.Writer.TryWrite(Disposition.Complete)) throw new InvalidOperationException("Release channel is full.");
        return await _received.Reader.ReadAsync();
    }

    [GlobalCleanup]
    public async Task Cleanup()
    {
        if (_transport is not null) await _transport.DisposeAsync();
        if (_controlLoop is not null)
        {
            _control.Writer.TryComplete();
            await _controlLoop;
        }
    }
}
