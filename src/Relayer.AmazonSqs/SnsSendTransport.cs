using System.Collections.Frozen;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Text.Json;
using Amazon.SimpleNotificationService;
using Amazon.SimpleNotificationService.Model;
using Amazon.SQS;
using Microsoft.Extensions.Logging;
using Relayer.Transports;

namespace Relayer.AmazonSqs;

internal sealed class SnsSendTransport : ITransport, ITransportSendTracing
{
    private readonly IAmazonSimpleNotificationService _client;
    private readonly bool _ownsClient;
    private readonly FrozenDictionary<string, Endpoint> _endpoints;
    private readonly AmazonSnsTransportOptions _options;
    private readonly MessageTracing? _tracing;
    private readonly SnsOperationPool _operations = new();
    private SqsSendTransport? _queues;
    private int _disposed;

    internal static void ValidateDeclarations(string name, AmazonSnsTransportOptions options, IEnumerable<string> destinations)
    {
        ArgumentNullException.ThrowIfNull(options.Sns);
        var names = new HashSet<string>(StringComparer.Ordinal);
        foreach (var topic in options.Topics)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(topic.Name);
            if (!names.Add(topic.Name)) throw new InvalidOperationException($"SNS transport '{name}': duplicate topic '{topic.Name}'.");
            var fifo = topic.Name.EndsWith(".fifo", StringComparison.Ordinal);
            var declaresFifo = topic.Attributes?.TryGetValue("FifoTopic", out var value) == true && value == "true";
            if (fifo != declaresFifo) throw new InvalidOperationException($"SNS topic '{topic.Name}': .fifo and FifoTopic=true must agree.");
        }
        foreach (var destination in destinations)
            if (!names.Contains(destination)) throw new InvalidOperationException($"SNS destination '{destination}' needs a native CreateTopicRequest declaration.");
        foreach (var subscription in options.Subscriptions)
            if (subscription.Protocol != "sqs" || string.IsNullOrWhiteSpace(subscription.TopicArn) ||
                string.IsNullOrWhiteSpace(subscription.Endpoint) || subscription.Attributes?.GetValueOrDefault("RawMessageDelivery") != "true")
                throw new InvalidOperationException("SNS subscriptions require explicit topic/queue ARNs, Protocol=sqs and RawMessageDelivery=true to preserve CloudEvents attributes.");
        SqsSendTransport.ValidateDeclarations(name, options.QueueTopology, []);
    }

    internal SnsSendTransport(string name, IAmazonSimpleNotificationService client, bool ownsClient, AmazonSnsTransportOptions options,
        MessageTracing? tracing = null, ILoggerFactory? logging = null, IReadOnlyDictionary<string, string>? identityHeaders = null)
    {
        Name = name;
        _client = client;
        _ownsClient = ownsClient;
        _options = options;
        _tracing = tracing;
        var endpoints = new Dictionary<string, Endpoint>(StringComparer.Ordinal);
        foreach (var declaration in options.Topics)
        {
            var endpoint = new Endpoint(declaration, identityHeaders?.GetValueOrDefault(declaration.Name) ?? "ce_id");
            if (tracing is not null)
                endpoint.Tracing = new(tracing, new() { System = "aws_sns", Destination = declaration.Name },
                    SendCoreAsync, SendBatchCoreAsync, logging?.CreateLogger<SnsSendTransport>());
            endpoints.Add(declaration.Name, endpoint);
        }
        _endpoints = endpoints.ToFrozenDictionary(StringComparer.Ordinal);
    }
    public string Name { get; }
    private Endpoint GetEndpoint(string destination) => _endpoints.TryGetValue(destination, out var endpoint) ? endpoint
        : throw new InvalidOperationException($"SNS transport '{Name}': undeclared topic '{destination}'.");
    public TransportCapabilities GetCapabilities(string endpoint) => GetEndpoint(endpoint).Capabilities;
    public SendTracing? GetSendTracing(string destination) => GetEndpoint(destination).Tracing;

    public ValueTask SendAsync(OutgoingMessage message, CancellationToken cancellationToken) =>
        GetEndpoint(message.Destination).Tracing is { } tracing ? tracing.SendAsync(message, cancellationToken) : SendCoreAsync(message, cancellationToken);

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    private async ValueTask SendCoreAsync(OutgoingMessage message, CancellationToken cancellationToken)
    {
        var endpoint = Validate(message, cancellationToken);
        var state = _operations.Rent();
        state.Used = 1;
        var activity = CurrentActivity();
        Enrich(activity, endpoint.Arn!, null);
        try
        {
            var size = state.Messages[0].Prepare(message, endpoint.Arn!, endpoint.Fifo, endpoint.IdentityHeader);
            ValidateSize(endpoint, size);
            endpoint.Tracing?.RecordNativeSend();
            var duration = endpoint.Tracing?.StartNativeSend() ?? default;
            var failed = true;
            try
            {
                var response = await _client.PublishAsync(state.Messages[0].Request, cancellationToken).ConfigureAwait(false);
                failed = false;
                Enrich(activity, endpoint.Arn!, response.ResponseMetadata?.RequestId);
            }
            finally { duration.Complete(failed); }
        }
        catch (AmazonSimpleNotificationServiceException error)
        {
            Enrich(activity, endpoint.Arn!, error.RequestId);
            throw;
        }
        finally { _operations.Return(state); }
    }

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    public async ValueTask SendBatchAsync(ReadOnlyMemory<OutgoingMessage> messages, CancellationToken cancellationToken)
    {
        Check(cancellationToken);
        var start = 0;
        while (start < messages.Length)
        {
            var destination = messages.Span[start].Destination;
            var end = start + 1;
            while (end < messages.Length && messages.Span[end].Destination == destination) end++;
            var group = messages.Slice(start, end - start);
            var tracing = GetEndpoint(destination).Tracing;
            if (tracing is null) await SendBatchCoreAsync(group, cancellationToken).ConfigureAwait(false);
            else await tracing.SendBatchAsync(group, cancellationToken).ConfigureAwait(false);
            start = end;
        }
    }

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    private async ValueTask SendBatchCoreAsync(ReadOnlyMemory<OutgoingMessage> messages, CancellationToken cancellationToken)
    {
        Check(cancellationToken);
        if (messages.IsEmpty) return;
        var endpoint = GetEndpoint(messages.Span[0].Destination);
        var state = _operations.Rent();
        var count = 0;
        var bytes = 0;
        var batchStart = 0;
        state.Batch.TopicArn = endpoint.Arn;
        try
        {
            for (var i = 0; i < messages.Length; i++)
            {
                Validate(messages.Span[i], cancellationToken);
                if (count == 10)
                {
                    await FlushAsync(state, endpoint, batchStart, cancellationToken).ConfigureAwait(false);
                    for (var j = 0; j < count; j++) state.Messages[j].Clear();
                    state.Batch.PublishBatchRequestEntries.Clear();
                    state.Used = 0;
                    count = bytes = 0;
                    batchStart = i;
                }
                var pending = state.Messages[count];
                state.Used = count + 1;
                var size = pending.Prepare(messages.Span[i], endpoint.Arn!, endpoint.Fifo, endpoint.IdentityHeader);
                ValidateSize(endpoint, size);
                if (count > 0 && bytes + size > 262_144)
                {
                    await FlushAsync(state, endpoint, batchStart, cancellationToken).ConfigureAwait(false);
                    for (var j = 0; j < count; j++) state.Messages[j].Clear();
                    state.Batch.PublishBatchRequestEntries.Clear();
                    // Carry the already prepared request across a byte boundary without repeating its callback.
                    (state.Messages[0], state.Messages[count]) = (pending, state.Messages[0]);
                    state.Used = 1;
                    count = bytes = 0;
                    batchStart = i;
                }
                pending.CopyToEntry();
                pending.Entry.Id = state.Ids[count];
                state.Batch.PublishBatchRequestEntries.Add(pending.Entry);
                count++;
                bytes += size;
            }
            if (count > 0) await FlushAsync(state, endpoint, batchStart, cancellationToken).ConfigureAwait(false);
        }
        finally { _operations.Return(state); }
    }

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    private async ValueTask FlushAsync(SnsOperationState state, Endpoint endpoint, int offset, CancellationToken cancellationToken)
    {
        var activity = CurrentActivity();
        Enrich(activity, state.Batch.TopicArn, null);
        endpoint.Tracing?.RecordNativeSend(state.Batch.PublishBatchRequestEntries.Count);
        var duration = endpoint.Tracing?.StartNativeSend() ?? default;
        var failed = true;
        try
        {
            var response = await _client.PublishBatchAsync(state.Batch, cancellationToken).ConfigureAwait(false);
            Enrich(activity, state.Batch.TopicArn, response.ResponseMetadata?.RequestId);
            if (response.Failed is { Count: > 0 }) throw new AmazonSnsBatchException(response, offset);
            failed = false;
        }
        catch (AmazonSimpleNotificationServiceException error)
        {
            Enrich(activity, state.Batch.TopicArn, error.RequestId);
            throw;
        }
        finally { duration.Complete(failed); }
    }

    private Activity? CurrentActivity() => Activity.Current is { } activity && ReferenceEquals(activity.Source, _tracing?.Source) ? activity : null;
    private static void Enrich(Activity? activity, string arn, string? requestId)
    {
        activity?.SetTag("aws.sns.topic.arn", arn);
        if (requestId is not null) activity?.SetTag("aws.request_id", requestId);
    }

    private Endpoint Validate(OutgoingMessage message, CancellationToken cancellationToken)
    {
        Check(cancellationToken);
        var endpoint = GetEndpoint(message.Destination);
        if (endpoint.Arn is null) throw new InvalidOperationException($"SNS transport '{Name}': start the host or apply topology before sending.");
        if (message.IsTombstone) throw new NotSupportedException("SNS does not support tombstones.");
        if (message.OrderingKey is not null && !endpoint.Fifo) throw new NotSupportedException("SNS ordering keys require a FIFO topic.");
        return endpoint;
    }
    private static void ValidateSize(Endpoint endpoint, int size)
    {
        if (size > 262_144) throw new InvalidOperationException($"SNS message for '{endpoint.Declaration.Name}' exceeds 256 KiB including attributes.");
    }
    private void Check(CancellationToken cancellationToken)
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
        cancellationToken.ThrowIfCancellationRequested();
    }
    public async Task StartAsync(CancellationToken cancellationToken)
    {
        Check(cancellationToken);
        await ResolveTopicsAsync(cancellationToken).ConfigureAwait(false);
        foreach (var endpoint in _endpoints.Values)
            if (endpoint.Arn is null) throw new InvalidOperationException($"Missing SNS topic '{endpoint.Declaration.Name}'. Provision topology explicitly before sending.");
    }

    private async Task ResolveTopicsAsync(CancellationToken cancellationToken)
    {
        // Native pagination is null-safe in SDK v4, unlike FindTopicAsync (aws/aws-sdk-net#4586).
        foreach (var endpoint in _endpoints.Values) endpoint.Arn = null;
        var topics = _client.Paginators.ListTopics(new ListTopicsRequest()).Topics;
        await foreach (var topic in topics.WithCancellation(cancellationToken).ConfigureAwait(false))
        {
            var name = topic.TopicArn[(topic.TopicArn.LastIndexOf(':') + 1)..];
            if (_endpoints.TryGetValue(name, out var endpoint)) endpoint.Arn = topic.TopicArn;
        }
    }

    public async ValueTask<TopologyReport> ApplyTopologyAsync(TopologyMode mode, CancellationToken cancellationToken)
    {
        Check(cancellationToken);
        if (mode == TopologyMode.None) return new([]);
        if (mode is not (TopologyMode.Validate or TopologyMode.Provision)) throw new ArgumentOutOfRangeException(nameof(mode));
        var differences = new List<string>();
        if (_options.QueueTopology.Queues.Count != 0)
        {
            if (_queues is null)
            {
                var options = _options.QueueTopology;
                var client = options.SqsClient ?? new AmazonSQSClient(options.Sqs);
                _queues = new(Name, client, options.SqsClient is null, options);
            }
            differences.AddRange((await _queues.ApplyTopologyAsync(mode, cancellationToken).ConfigureAwait(false)).Differences);
        }
        await ResolveTopicsAsync(cancellationToken).ConfigureAwait(false);
        foreach (var endpoint in _endpoints.Values)
        {
            if (endpoint.Arn is null)
            {
                if (mode == TopologyMode.Validate)
                {
                    differences.Add($"Missing SNS topic '{endpoint.Declaration.Name}'. Provision it explicitly.");
                    continue;
                }
                endpoint.Arn = (await _client.CreateTopicAsync(endpoint.Declaration, cancellationToken).ConfigureAwait(false)).TopicArn;
            }
            var actual = await _client.GetTopicAttributesAsync(new GetTopicAttributesRequest { TopicArn = endpoint.Arn }, cancellationToken).ConfigureAwait(false);
            CompareAttributes(endpoint.Declaration.Attributes, actual.Attributes, $"SNS topic '{endpoint.Declaration.Name}'", differences);
            if (endpoint.Declaration.Tags is { Count: > 0 } tags)
            {
                var actualTags = await _client.ListTagsForResourceAsync(new ListTagsForResourceRequest { ResourceArn = endpoint.Arn }, cancellationToken).ConfigureAwait(false);
                foreach (var tag in tags)
                    if (actualTags.Tags?.Any(value => value.Key == tag.Key && value.Value == tag.Value) != true)
                        differences.Add($"SNS topic '{endpoint.Declaration.Name}': tag '{tag.Key}' differs. Reconcile through infrastructure configuration.");
            }
        }
        foreach (var declaration in _options.Subscriptions)
        {
            Subscription? found = null;
            try
            {
                var subscriptions = _client.Paginators.ListSubscriptionsByTopic(new ListSubscriptionsByTopicRequest { TopicArn = declaration.TopicArn }).Subscriptions;
                await foreach (var subscription in subscriptions.WithCancellation(cancellationToken).ConfigureAwait(false))
                    if (subscription.Protocol == declaration.Protocol && subscription.Endpoint == declaration.Endpoint)
                    {
                        found = subscription;
                        break;
                    }
            }
            catch (NotFoundException) { }
            string? arn = found?.SubscriptionArn;
            if (arn is null)
            {
                if (mode == TopologyMode.Validate)
                {
                    differences.Add($"Missing SNS subscription '{declaration.TopicArn}' to '{declaration.Endpoint}'.");
                    continue;
                }
                arn = (await _client.SubscribeAsync(declaration, cancellationToken).ConfigureAwait(false)).SubscriptionArn;
            }
            if (string.IsNullOrWhiteSpace(arn) || arn == "pending confirmation" || arn == "PendingConfirmation")
            {
                differences.Add($"SNS subscription '{declaration.Endpoint}' requires confirmation.");
                continue;
            }
            var attributes = await _client.GetSubscriptionAttributesAsync(new GetSubscriptionAttributesRequest { SubscriptionArn = arn }, cancellationToken).ConfigureAwait(false);
            CompareAttributes(declaration.Attributes, attributes.Attributes, $"SNS subscription '{declaration.Endpoint}'", differences);
        }
        return new(differences);
    }

    private static void CompareAttributes(Dictionary<string, string>? expected, Dictionary<string, string>? actual, string description, List<string> differences)
    {
        if (expected is null) return;
        foreach (var attribute in expected)
        {
            var equal = actual?.TryGetValue(attribute.Key, out var value) == true && value == attribute.Value;
            if (!equal && actual?.TryGetValue(attribute.Key, out value) == true && attribute.Key is "Policy" or "DeliveryPolicy" or "FilterPolicy" or "RedrivePolicy" or "DataProtectionPolicy")
            {
                using var left = JsonDocument.Parse(attribute.Value);
                using var right = JsonDocument.Parse(value!);
                equal = JsonElement.DeepEquals(left.RootElement, right.RootElement);
            }
            if (!equal) differences.Add($"{description}: {attribute.Key} differs. Relayer never modifies or deletes existing topology; reconcile through infrastructure configuration.");
        }
    }

    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0) return;
        if (_queues is not null) await _queues.DisposeAsync().ConfigureAwait(false);
        if (_ownsClient) _client.Dispose();
    }
    private sealed class Endpoint(CreateTopicRequest declaration, string identityHeader)
    {
        internal CreateTopicRequest Declaration { get; } = declaration;
        internal bool Fifo { get; } = declaration.Name.EndsWith(".fifo", StringComparison.Ordinal);
        internal string IdentityHeader { get; } = identityHeader;
        internal string? Arn { get; set; }
        internal TransportCapabilities Capabilities { get; } = new() { OrderedByKey = declaration.Name.EndsWith(".fifo", StringComparison.Ordinal), MaxBatchSize = 10, MaxMessageBytes = 262_144 };
        internal SendTracing? Tracing { get; set; }
    }
}
