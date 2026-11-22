using System.Runtime.CompilerServices;
using BenchmarkDotNet.Attributes;
using Relayer.Transports;

namespace Relayer.Benchmarks;

/// <summary>One buffered attempt, including native send completion and storage cleanup.</summary>
[MemoryDiagnoser]
public class AttemptSendBufferBenchmarks
{
    private readonly AttemptSendBuffer _buffer = new();
    private readonly Sink _sink = new();
    private readonly byte[] _body = new byte[512 * 1024];
    private readonly TimeSpan _timeout = TimeSpan.FromSeconds(30);

    [Benchmark]
    [Arguments(1, 256)]
    [Arguments(100, 256)]
    [Arguments(129, 256)]
    [Arguments(1, 512 * 1024)]
    [RuntimeAsyncMethodGeneration(false)]
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    public async ValueTask BufferedAttempt(int count, int bodySize)
    {
        var message = new OutgoingMessage("orders", _body.AsMemory(0, bodySize));

        for (var i = 0; i < count; i++)
        {
            _buffer.Add(_sink, message, _timeout);
        }

        await _buffer.FlushAsync(default).ConfigureAwait(false);
        _buffer.Clear();
    }

    private sealed class Sink : ITransport
    {
        public string Name => "sink";

        public ValueTask SendAsync(OutgoingMessage message, CancellationToken cancellationToken)
        {
            return default;
        }

        public TransportCapabilities GetCapabilities(string endpoint)
        {
            throw new NotSupportedException();
        }

        public ValueTask<TopologyReport> ApplyTopologyAsync(TopologyMode mode, CancellationToken cancellationToken)
        {
            throw new NotSupportedException();
        }

        public ValueTask SendBatchAsync(ReadOnlyMemory<OutgoingMessage> messages, CancellationToken cancellationToken)
        {
            throw new NotSupportedException();
        }

        public Task StartAsync(CancellationToken cancellationToken)
        {
            throw new NotSupportedException();
        }

        public Task StopAsync(CancellationToken cancellationToken)
        {
            throw new NotSupportedException();
        }

        public ValueTask DisposeAsync()
        {
            return default;
        }
    }
}
