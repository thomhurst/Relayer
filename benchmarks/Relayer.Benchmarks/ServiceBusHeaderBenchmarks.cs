using System.Text;
using Azure.Messaging.ServiceBus;
using BenchmarkDotNet.Attributes;
using Relayer.AzureServiceBus;

namespace Relayer.Benchmarks;

/// <summary>Isolates UTF-8 envelope conversion from SDK/network allocations. IDs and times change every invocation.</summary>
[MemoryDiagnoser]
public class ServiceBusHeaderBenchmarks
{
    private readonly ServiceBusSendState _relayer = new();
    private readonly ServiceBusMessage _raw = new();
    private readonly HeaderBuffer _headers = new();
    private readonly MessageEnvelope _first = new(new("benchmark.order", 1), "/orders", new("cloudEvents:"));
    private readonly MessageEnvelope _second = new(new("benchmark.other", 1), "/other", new("cloudEvents:"));
    private readonly DateTimeOffset _time = DateTimeOffset.UtcNow;
    private long _sequence;

    [Params(false, true)]
    public bool AlternatingRoutes { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        _relayer.Prepare(Next());
        _relayer.Clear();
        RawHeaders();
    }

    [Benchmark(Baseline = true)]
    public int RawHeaders()
    {
        var outgoing = Next();
        foreach (var header in outgoing.Headers.Span)
            _raw.ApplicationProperties[header.Key] = header.Value.ToString();
        var count = _raw.ApplicationProperties.Count;
        _raw.ApplicationProperties.Clear();
        return count;
    }

    [Benchmark]
    public int RelayerHeaders()
    {
        _relayer.Prepare(Next());
        var count = _relayer.Message.ApplicationProperties.Count;
        _relayer.Clear();
        return count;
    }

    private OutgoingMessage Next()
    {
        var sequence = ++_sequence;
        var envelope = AlternatingRoutes && (sequence & 1) == 0 ? _second : _first;
        envelope.Write<HeaderBuffer, HeaderBuffer>(_headers,
            new(Guid.CreateVersion7(), _time.AddTicks(sequence), default));
        return new("orders", default, _headers.Entries);
    }

    private sealed class HeaderBuffer : IMessageHeaderWriter<HeaderBuffer>
    {
        internal readonly KeyValuePair<string, WireString>[] Entries = new KeyValuePair<string, WireString>[5];
        private readonly byte[][] _values = [new byte[128], new byte[128], new byte[128], new byte[128], new byte[128]];

        public static void SetHeader(HeaderBuffer message, string name, ReadOnlySpan<char> value)
        {
            for (var i = 0; i < message.Entries.Length; i++)
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
