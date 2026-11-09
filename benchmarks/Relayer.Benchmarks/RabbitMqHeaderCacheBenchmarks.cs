using BenchmarkDotNet.Attributes;
using Relayer.RabbitMQ;

namespace Relayer.Benchmarks;

/// <summary>Isolates bounded header conversion hits and FIFO churn without broker latency.</summary>
[MemoryDiagnoser]
public class RabbitMqHeaderCacheBenchmarks
{
    private RabbitMqPublisher _publisher = null!;
    private OutgoingMessage[] _messages = null!;
    private int _next;

    [Params(2, 257)]
    public int Shapes { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        _publisher = new(null!);
        _messages = new OutgoingMessage[Shapes];
        for (var i = 0; i < Shapes; i++)
        {
            var value = new byte[i + 3];
            _messages[i] = new("orders", default,
                new KeyValuePair<string, WireString>[] { new("id", new(value.AsMemory(1, i + 1))) });
        }
        for (var i = 0; i < Shapes; i++) Prepare();
    }

    [Benchmark]
    public void Prepare()
    {
        _publisher.Prepare(_messages[_next], 0);
        _publisher.Clear();
        if (++_next == Shapes) _next = 0;
    }
}
