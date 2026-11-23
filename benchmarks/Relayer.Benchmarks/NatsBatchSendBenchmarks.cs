using System.Diagnostics;
using System.Runtime.CompilerServices;
using BenchmarkDotNet.Attributes;
using Microsoft.Extensions.Options;
using NATS.Client.Core;
using NATS.Client.JetStream;
using NATS.Client.JetStream.Models;
using Relayer.Nats;
using Relayer.Transports;

namespace Relayer.Benchmarks;

[MemoryDiagnoser]
public class NatsBatchSendBenchmarks
{
    private readonly MessageTracing _tracing = new(Options.Create(new TracingOptions()), []);
    private ActivityListener? _listener;
    private NatsConnection _connection = null!;
    private NatsJSContext _jetStream = null!;
    private ITransport _transport = null!;
    private string _subject = null!;
    private OutgoingMessage[] _messages = [];

    [Params(false, true)]
    public bool RecordSpans
    {
        get; set;
    }

    [GlobalSetup]
    public async Task Setup()
    {
        _subject = "benchmark_" + Guid.NewGuid().ToString("N");
        var options = new NatsOpts
        {
            Url = Environment.GetEnvironmentVariable("NATS_URL") ?? "nats://localhost:4222",
        };
        _connection = new(options);
        _jetStream = new(_connection);
        await _jetStream.CreateStreamAsync(new(_subject, [_subject])
        {
            Storage = StreamConfigStorage.Memory,
            MaxMsgs = 1000,
        });
        var definition = new TransportDefinition("benchmark", typeof(NatsTransportOptions),
            [new RouteDefinition<object>(_subject, null, null, new())], [], null, static (_, _) =>
            {
            });
        _transport = new NatsNativeTransport("benchmark", new()
        {
            ConnectionOptions = options,
        }, definition, _tracing);
        await _transport.StartAsync(default);
        var message = new OutgoingMessage(_subject, "payload"u8.ToArray());
        _messages = new OutgoingMessage[8];
        Array.Fill(_messages, message);

        if (RecordSpans)
        {
            _listener = new()
            {
                ShouldListenTo = source => ReferenceEquals(source, _tracing.Source),
                Sample = static (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
            };
            ActivitySource.AddActivityListener(_listener);
        }
    }

    [Benchmark(Baseline = true)]
    [RuntimeAsyncMethodGeneration(false)]
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    public async ValueTask NativeConfirmedPublications()
    {
        foreach (var message in _messages)
        {
            var ack = await _jetStream.PublishAsync(message.Destination, message.Body, NatsRawSerializer<ReadOnlyMemory<byte>>.Default);
            ack.EnsureSuccess();
        }
    }

    [Benchmark]
    public ValueTask RelayerBatch()
    {
        return _transport.SendBatchAsync(_messages, default);
    }

    [GlobalCleanup]
    public async Task Cleanup()
    {
        _listener?.Dispose();
        _tracing.Dispose();
        await _transport.DisposeAsync();
        await _jetStream.DeleteStreamAsync(_subject);
        await _connection.DisposeAsync();
    }
}
