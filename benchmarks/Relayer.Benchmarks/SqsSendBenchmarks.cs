using System.Text;
using System.Diagnostics.Metrics;
using Microsoft.Extensions.Options;
using Amazon;
using Amazon.Runtime;
using Amazon.SQS;
using Amazon.SQS.Model;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Configs;
using Relayer.AmazonSqs;

namespace Relayer.Benchmarks;

/// <summary>Native acknowledged SQS sends. Both paths reuse requests and include UTF-8-to-native-text conversion.</summary>
[MemoryDiagnoser]
[CategoriesColumn]
[GroupBenchmarksBy(BenchmarkLogicalGroupRule.ByCategory)]
public class SqsSendBenchmarks
{
    [Params(false, true)]
    public bool Metrics { get; set; }
    private RelayerInstrumentation _instrumentation = null!;
    private MessageTracing _tracing = null!;
    private MeterListener _listener = null!;
    private AmazonSQSClient _client = null!;
    private SqsSendTransport _transport = null!;
    private string _queue = null!;
    private string _url = null!;
    private byte[] _body = null!;
    private KeyValuePair<string, WireString>[] _headers = null!;
    private OutgoingMessage _message;
    private OutgoingMessage[] _batch = null!;
    private SendMessageRequest _rawSingle = null!;
    private SendMessageBatchRequest _rawBatch = null!;

    private DirectJsonSend _json = null!;
    private SendMessageRequest _jsonRequest = null!;

    [GlobalSetup]
    public async Task Setup()
    {
        var serviceUrl = Environment.GetEnvironmentVariable("RELAYER_SQS_SERVICE_URL");
        _client = serviceUrl is null
            ? new AmazonSQSClient(RegionEndpoint.GetBySystemName(Environment.GetEnvironmentVariable("AWS_REGION") ?? "us-east-1"))
            : new AmazonSQSClient(new BasicAWSCredentials("test", "test"), new AmazonSQSConfig { ServiceURL = serviceUrl, AuthenticationRegion = "us-east-1" });
        _queue = "relayer-benchmark-" + Guid.NewGuid().ToString("N");
        _url = (await _client.CreateQueueAsync(_queue)).QueueUrl;
        await Task.Delay(TimeSpan.FromSeconds(1));
        var options = new AmazonSqsTransportOptions { SqsClient = _client };
        options.Queues.Add(new(_queue));
        _instrumentation = new();
        _listener = new()
        {
            InstrumentPublished = (instrument, observer) =>
            {
                if (Metrics && ReferenceEquals(instrument.Meter.Scope, _instrumentation)) observer.EnableMeasurementEvents(instrument);
            },
        };
        _listener.SetMeasurementEventCallback<long>(static (_, _, _, _) => { });
        _listener.SetMeasurementEventCallback<double>(static (_, _, _, _) => { });
        _listener.Start();
        _tracing = new(Options.Create(new TracingOptions()), [], instrumentation: _instrumentation);
        _transport = new("benchmark", _client, false, options, _tracing);
        await _transport.StartAsync(default);
        _body = Encoding.UTF8.GetBytes(new string('x', 256));
        _headers = [new("ce_id", new("019a3c1b-97c0-7000-8000-000000000001")), new("ce_type", new("benchmark.order;v=1")),
            new("ce_source", new("/benchmarks")), new("ce_specversion", new("1.0")), new("content-type", new("application/json"))];
        _message = new(_queue, _body, _headers);
        _batch = Enumerable.Repeat(_message, 10).ToArray();
        _rawSingle = new() { QueueUrl = _url, MessageAttributes = Attributes() };
        _rawBatch = new() { QueueUrl = _url, Entries = new(10) };
        for (var i = 0; i < 10; i++) _rawBatch.Entries.Add(new() { Id = i.ToString(), MessageAttributes = Attributes() });
        _json = new(_instrumentation, builder => builder.AmazonSqs("benchmark-json", transport =>
        {
            transport.TopologyMode = TopologyMode.None;
            transport.Options.Configure(options => { options.SqsClient = _client; options.Queues.Add(new(_queue)); });
            transport.Send<DirectJsonOrder>(_queue);
        }));
        await _json.StartAsync();
        _jsonRequest = new() { QueueUrl = _url, MessageAttributes = new() };
        foreach (var key in new[] { "ce_id", "ce_type", "ce_source", "ce_time", "ce_specversion", "traceparent", "tracestate", "content-type" })
            _jsonRequest.MessageAttributes[key] = new() { DataType = "String" };
        await RawJsonSingle();
        await RelayerJsonSingle();
        await RawSingle();
        await RelayerSingle();
        await RawBatch();
        await RelayerBatch();
    }

    private Dictionary<string, MessageAttributeValue> Attributes()
    {
        var attributes = new Dictionary<string, MessageAttributeValue>(10);
        foreach (var header in _headers) attributes.Add(header.Key, new() { DataType = "String" });
        return attributes;
    }

    private void Prepare(Dictionary<string, MessageAttributeValue> attributes)
    {
        foreach (var header in _headers) attributes[header.Key].StringValue = header.Value.ToString();
    }

    [Benchmark(Baseline = true), BenchmarkCategory("Single")]
    public Task<SendMessageResponse> RawSingle()
    {
        _rawSingle.MessageBody = Encoding.UTF8.GetString(_body);
        Prepare(_rawSingle.MessageAttributes);
        return _client.SendMessageAsync(_rawSingle);
    }

    [Benchmark, BenchmarkCategory("Single")]
    public ValueTask RelayerSingle() => _transport.SendAsync(_message, default);

    [Benchmark(Baseline = true, OperationsPerInvoke = 10), BenchmarkCategory("Batch")]
    public Task<SendMessageBatchResponse> RawBatch()
    {
        foreach (var entry in _rawBatch.Entries)
        {
            entry.MessageBody = Encoding.UTF8.GetString(_body);
            Prepare(entry.MessageAttributes);
        }
        return _client.SendMessageBatchAsync(_rawBatch);
    }

    [Benchmark(OperationsPerInvoke = 10), BenchmarkCategory("Batch")]
    public ValueTask RelayerBatch() => _transport.SendBatchAsync(_batch, default);

    [Benchmark(Baseline = true), BenchmarkCategory("DefaultJson")]
    public Task<SendMessageResponse> RawJsonSingle()
    {
        _jsonRequest.MessageBody = Encoding.UTF8.GetString(_json.Serialize().Span);
        _json.WriteEnvelope<Dictionary<string, MessageAttributeValue>, NativeHeaders>(_jsonRequest.MessageAttributes);
        _jsonRequest.MessageAttributes["content-type"].StringValue = _json.ContentType;
        return _client.SendMessageAsync(_jsonRequest);
    }

    [Benchmark, BenchmarkCategory("DefaultJson")]
    public ValueTask RelayerJsonSingle() => _json.Sender.SendAsync(_json.Order);

    private sealed class NativeHeaders : IMessageHeaderWriter<Dictionary<string, MessageAttributeValue>>
    {
        public static void SetHeader(Dictionary<string, MessageAttributeValue> headers, string name, ReadOnlySpan<char> value)
        {
            if (!headers.TryGetValue(name, out var attribute)) headers[name] = attribute = new() { DataType = "String" };
            attribute.StringValue = value.ToString();
        }
        public static void RemoveHeader(Dictionary<string, MessageAttributeValue> headers, string name) => headers.Remove(name);
    }

    [GlobalCleanup]
    public async Task Cleanup()
    {
        if (_json is not null) await _json.DisposeAsync();
        if (_transport is not null) await _transport.DisposeAsync();
        try { if (_client is not null && _url is not null) await _client.DeleteQueueAsync(_url); }
        finally { _client?.Dispose(); _tracing?.Dispose(); _listener?.Dispose(); _instrumentation?.Dispose(); }
    }
}
