using System.Text;
using Amazon.SQS.Model;
using BenchmarkDotNet.Attributes;
using Relayer.AmazonSqs;

namespace Relayer.Benchmarks;

/// <summary>Isolates Relayer-owned body/header allocations from serialization and SDK/network allocations.</summary>
[MemoryDiagnoser]
public class SqsBindingBenchmarks
{
    private readonly SqsSendState _relayer = new("0");
    private readonly SendMessageRequest _raw = new() { MessageAttributes = new(10) };
    private readonly HeaderBuffer _headers = new();
    private readonly MessageEnvelope _envelope = new(new("benchmark.order", 1), "/benchmarks", new());
    private readonly byte[] _body = Encoding.UTF8.GetBytes(new string('x', 256));
    private readonly DateTimeOffset _time = DateTimeOffset.UtcNow;
    private long _sequence;

    [GlobalSetup]
    public void Setup()
    {
        var message = Next();
        foreach (var header in message.Headers.Span) _raw.MessageAttributes.Add(header.Key, new() { DataType = "String" });
        _relayer.Prepare(message, "https://sqs.us-east-1.amazonaws.com/123456789012/orders", false);
        _relayer.Clear();
        RawBinding();
    }

    [Benchmark(Baseline = true)]
    public int RawBinding()
    {
        var message = Next();
        _raw.MessageBody = Encoding.UTF8.GetString(message.Body.Span);
        foreach (var header in message.Headers.Span) _raw.MessageAttributes[header.Key].StringValue = header.Value.ToString();
        return _raw.MessageBody.Length + _raw.MessageAttributes.Count;
    }

    [Benchmark]
    public int RelayerBinding()
    {
        var size = _relayer.Prepare(Next(), "https://sqs.us-east-1.amazonaws.com/123456789012/orders", false);
        _relayer.Clear();
        return size;
    }

    private OutgoingMessage Next()
    {
        _envelope.Write<HeaderBuffer, HeaderBuffer>(_headers, new(Guid.CreateVersion7(), _time.AddTicks(++_sequence), default));
        return new("orders", _body, _headers.Entries);
    }

    private sealed class HeaderBuffer : IMessageHeaderWriter<HeaderBuffer>
    {
        internal readonly KeyValuePair<string, WireString>[] Entries = new KeyValuePair<string, WireString>[6];
        private readonly byte[][] _values = [new byte[128], new byte[128], new byte[128], new byte[128], new byte[128]];
        internal HeaderBuffer() => Entries[5] = new("content-type", new("application/json"));

        public static void SetHeader(HeaderBuffer message, string name, ReadOnlySpan<char> value)
        {
            for (var i = 0; i < message._values.Length; i++)
            {
                if (message.Entries[i].Key is not null && message.Entries[i].Key != name) continue;
                var length = Encoding.UTF8.GetBytes(value, message._values[i]);
                message.Entries[i] = new(name, new WireString(message._values[i].AsMemory(0, length)));
                return;
            }
            throw new InvalidOperationException("Unexpected envelope header.");
        }

        public static void RemoveHeader(HeaderBuffer message, string name) { }
    }
}
