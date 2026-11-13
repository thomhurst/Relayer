using System.Text;
using Amazon.Runtime;
using Amazon.SQS;
using Amazon.SQS.Model;
using BenchmarkDotNet.Attributes;
using Microsoft.Extensions.Options;
using Relayer.AmazonSqs;

namespace Relayer.Benchmarks;

/// <summary>Equivalent cached native receive/delete batches isolate adapter scheduling and binding cost.
/// SDK HTTP/serialization allocations are excluded; this is not an end-to-end throughput claim.</summary>
[MemoryDiagnoser]
public class SqsReceiveBatchBenchmarks
{
    [Params(2, 10)] public int Size { get; set; }
    private Client _client = null!;
    private ReceiveMessageRequest _request = null!;
    private DeleteMessageBatchRequest _delete = null!;
    private SqsConsumer _consumer = null!;
    private SqsConsumer.SqsReceiveBatch _batch = null!;
    private MessageTracing _tracing = null!;
    private readonly byte[] _body = new byte[256];

    [GlobalSetup]
    public void Setup()
    {
        var messages = new List<Message>(Size);
        var successful = new List<DeleteMessageBatchResultEntry>(Size);
        _delete = new() { QueueUrl = "url", Entries = new(Size) };
        for (var i = 0; i < Size; i++)
        {
            var id = i.ToString(System.Globalization.CultureInfo.InvariantCulture);
            messages.Add(new() { Body = new string('x', 256), ReceiptHandle = id, Attributes = [], MessageAttributes = [] });
            _delete.Entries.Add(new() { Id = id, ReceiptHandle = id });
            successful.Add(new() { Id = id });
        }
        _client = new(new() { Messages = messages }, new() { Successful = successful, Failed = [] });
        _request = new() { QueueUrl = "url", MaxNumberOfMessages = Size };
        _tracing = new(Options.Create(new TracingOptions()), []);
        var settlement = new SqsSettlement(_tracing, new() { System = "aws_sqs", Destination = "orders" });
        _consumer = new(new("orders", new(), [], 1, 1, default), _client, "url",
            (delivery, token) => settlement.SettleAsync(delivery, Disposition.Complete, token));
        _batch = new(_consumer, Size);
    }

    [Benchmark(Baseline = true)]
    public async ValueTask Native()
    {
        var response = await _client.ReceiveMessageAsync(_request);
        for (var i = 0; i < response.Messages.Count; i++)
        {
            Encoding.UTF8.GetBytes(response.Messages[i].Body, _body);
            _delete.Entries[i].ReceiptHandle = response.Messages[i].ReceiptHandle;
        }
        await _client.DeleteMessageBatchAsync(_delete);
    }

    [Benchmark]
    public async ValueTask RelayerBatch()
    {
        var receivedAt = TimeProvider.System.GetTimestamp();
        var response = await _client.ReceiveMessageAsync(_request);
        await _batch.ProcessAsync(response, receivedAt, default);
    }

    [GlobalCleanup]
    public async Task Cleanup()
    {
        _batch.Dispose();
        await _consumer.DisposeAsync();
        _tracing.Dispose();
        _client.Dispose();
    }

    private sealed class Client(ReceiveMessageResponse receive, DeleteMessageBatchResponse delete) : AmazonSQSClient(
        new BasicAWSCredentials("test", "test"), new AmazonSQSConfig { ServiceURL = "http://localhost", AuthenticationRegion = "us-east-1" })
    {
        private readonly Task<ReceiveMessageResponse> _receive = Task.FromResult(receive);
        private readonly Task<DeleteMessageBatchResponse> _delete = Task.FromResult(delete);
        public override Task<ReceiveMessageResponse> ReceiveMessageAsync(ReceiveMessageRequest request, CancellationToken cancellationToken = default) => _receive;
        public override Task<DeleteMessageBatchResponse> DeleteMessageBatchAsync(DeleteMessageBatchRequest request, CancellationToken cancellationToken = default) => _delete;
    }
}
