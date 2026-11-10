using System.Text;
using Amazon.Runtime;
using Amazon.SQS;
using Amazon.SQS.Model;
using BenchmarkDotNet.Attributes;
using Relayer.AmazonSqs;

namespace Relayer.Benchmarks;

/// <summary>Native string-to-UTF8 preparation compared with the pooled SQS binding.</summary>
[MemoryDiagnoser]
public class SqsReceiveBindingBenchmarks
{
    private readonly Message _message = new() { Body = new string('x', 256), Attributes = new() { ["ApproximateReceiveCount"] = "1" }, MessageAttributes = [] };
    private readonly byte[] _raw = new byte[256];
    private AmazonSQSClient _client = null!;
    private SqsDelivery _delivery = null!;
    [GlobalSetup]
    public void Setup()
    {
        _client = new(new BasicAWSCredentials("test", "test"), new AmazonSQSConfig { ServiceURL = "http://localhost", AuthenticationRegion = "us-east-1" });
        _delivery = new(_client, "http://localhost/queue");
        Binding();
    }
    [Benchmark(Baseline = true)]
    public int Raw() => Encoding.UTF8.GetBytes(_message.Body, _raw);
    [Benchmark]
    public long Binding()
    {
        _delivery.Prepare(_message, TimeProvider.System.GetTimestamp(), null);
        var length = SqsMessageBinding.GetBody(_delivery).Length;
        _delivery.Clear();
        return length;
    }
    [GlobalCleanup]
    public void Cleanup() { _delivery.Dispose(); _client.Dispose(); }
}
