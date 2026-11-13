using Relayer.Transports;
using System.Text;
using System.Diagnostics.Metrics;
using Microsoft.Extensions.Options;
using Amazon;
using Amazon.Runtime;
using Amazon.SimpleNotificationService;
using Amazon.SimpleNotificationService.Model;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Configs;
using Relayer.AmazonSqs;

namespace Relayer.Benchmarks;

/// <summary>Native acknowledged SNS sends. Both paths reuse requests and include UTF-8-to-native-text conversion.</summary>
[MemoryDiagnoser]
[CategoriesColumn]
[GroupBenchmarksBy(BenchmarkLogicalGroupRule.ByCategory)]
public class SnsSendBenchmarks
{
    [Params(false, true)]
    public bool Metrics { get; set; }
    private RelayerInstrumentation _instrumentation = null!;
    private MessageTracing _tracing = null!;
    private MeterListener _listener = null!;
    private AmazonSimpleNotificationServiceClient _client = null!;
    private SnsSendTransport _transport = null!;
    private string _topic = null!;
    private string _arn = null!;
    private byte[] _body = null!;
    private KeyValuePair<string, WireString>[] _headers = null!;
    private OutgoingMessage _message;
    private OutgoingMessage[] _batch = null!;
    private PublishRequest _rawSingle = null!;
    private PublishBatchRequest _rawBatch = null!;

    private DirectJsonSend _json = null!;
    private PublishRequest _jsonRequest = null!;

    [GlobalSetup]
    public async Task Setup()
    {
        var serviceUrl = Environment.GetEnvironmentVariable("RELAYER_SNS_SERVICE_URL");
        _client = serviceUrl is null
            ? new AmazonSimpleNotificationServiceClient(RegionEndpoint.GetBySystemName(Environment.GetEnvironmentVariable("AWS_REGION") ?? "us-east-1"))
            : new AmazonSimpleNotificationServiceClient(new BasicAWSCredentials("test", "test"), new AmazonSimpleNotificationServiceConfig { ServiceURL = serviceUrl, AuthenticationRegion = "us-east-1" });
        _topic = "relayer-benchmark-" + Guid.NewGuid().ToString("N");
        _arn = (await _client.CreateTopicAsync(_topic)).TopicArn;
        var options = new AmazonSnsTransportOptions { SnsClient = _client };
        options.Topics.Add(new(_topic));
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
        _message = new(_topic, _body, _headers);
        _batch = Enumerable.Repeat(_message, 10).ToArray();
        _rawSingle = new() { TopicArn = _arn, MessageAttributes = Attributes() };
        _rawBatch = new() { TopicArn = _arn, PublishBatchRequestEntries = new(10) };
        for (var i = 0; i < 10; i++) _rawBatch.PublishBatchRequestEntries.Add(new() { Id = i.ToString(), MessageAttributes = Attributes() });
        _json = new(_instrumentation, builder => builder.AmazonSns("benchmark-json", transport =>
        {
            transport.TopologyMode = TopologyMode.None;
            transport.Options.Configure(options => { options.SnsClient = _client; options.Topics.Add(new(_topic)); });
            transport.Send<DirectJsonOrder>(_topic);
        }));
        await _json.StartAsync();
        _jsonRequest = new() { TopicArn = _arn, MessageAttributes = new() };
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
    public Task<PublishResponse> RawSingle()
    {
        _rawSingle.Message = Encoding.UTF8.GetString(_body);
        Prepare(_rawSingle.MessageAttributes);
        return _client.PublishAsync(_rawSingle);
    }

    [Benchmark, BenchmarkCategory("Single")]
    public ValueTask RelayerSingle() => _transport.SendAsync(_message, default);

    [Benchmark(Baseline = true, OperationsPerInvoke = 10), BenchmarkCategory("Batch")]
    public Task<PublishBatchResponse> RawBatch()
    {
        foreach (var entry in _rawBatch.PublishBatchRequestEntries)
        {
            entry.Message = Encoding.UTF8.GetString(_body);
            Prepare(entry.MessageAttributes);
        }
        return _client.PublishBatchAsync(_rawBatch);
    }

    [Benchmark(OperationsPerInvoke = 10), BenchmarkCategory("Batch")]
    public ValueTask RelayerBatch() => _transport.SendBatchAsync(_batch, default);

    [Benchmark(Baseline = true), BenchmarkCategory("DefaultJson")]
    public Task<PublishResponse> RawJsonSingle()
    {
        _jsonRequest.Message = Encoding.UTF8.GetString(_json.Serialize().Span);
        _json.WriteEnvelope<Dictionary<string, MessageAttributeValue>, NativeHeaders>(_jsonRequest.MessageAttributes);
        _jsonRequest.MessageAttributes["content-type"].StringValue = _json.ContentType;
        return _client.PublishAsync(_jsonRequest);
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
        try { if (_client is not null && _arn is not null) await _client.DeleteTopicAsync(_arn); }
        finally { _client?.Dispose(); _tracing?.Dispose(); _listener?.Dispose(); _instrumentation?.Dispose(); }
    }
}
