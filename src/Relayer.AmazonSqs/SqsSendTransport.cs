using System.Collections.Frozen;
using System.Diagnostics;
using System.Globalization;
using System.Runtime.CompilerServices;
using System.Text.Json;
using Amazon.SQS;
using Amazon.SQS.Model;
using Microsoft.Extensions.Logging;
using Relayer.Transports;

namespace Relayer.AmazonSqs;

internal sealed class SqsSendTransport : ITransport, ITransportSendTracing
{
    private readonly IAmazonSQS _client;
    private readonly bool _ownsClient;
    private readonly FrozenDictionary<string, Endpoint> _endpoints;
    private readonly MessageTracing? _tracing;
    private readonly SqsOperationPool _operations = new();
    private int _disposed;

    internal static void ValidateDeclarations(string name, AmazonSqsTransportOptions options, IEnumerable<string> destinations)
    {
        ArgumentNullException.ThrowIfNull(options.Sqs);
        var names = new HashSet<string>(StringComparer.Ordinal);
        foreach (var queue in options.Queues)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(queue.QueueName);
            if (!names.Add(queue.QueueName))
                throw new InvalidOperationException($"SQS transport '{name}': duplicate queue '{queue.QueueName}'.");
            var fifo = queue.QueueName.EndsWith(".fifo", StringComparison.Ordinal);
            var declaresFifo = queue.Attributes?.TryGetValue("FifoQueue", out var value) == true && value == "true";
            if (fifo != declaresFifo)
                throw new InvalidOperationException($"SQS queue '{queue.QueueName}': the .fifo suffix and native FifoQueue=true declaration must agree.");
        }
        foreach (var destination in destinations)
            if (!names.Contains(destination))
                throw new InvalidOperationException($"SQS transport '{name}': destination '{destination}' needs a native CreateQueueRequest declaration.");
    }

    // Resolve bindings before constructing an owned client, so invalid declarations cannot leak it.
    internal static IReadOnlyDictionary<string, string> ResolveIdentityHeaders(IReadOnlyList<IRouteDefinition> routes, string endpointDescription = "SQS FIFO queue")
    {
        var headers = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (var route in routes)
        {
            if (!route.Destination.EndsWith(".fifo", StringComparison.Ordinal) ||
                route.SendOptions.EnvelopeMode != MessageEnvelopeMode.CloudEvents) continue;
            var identityHeader = route.SendOptions.HeaderNames.Id;
            if (headers.TryGetValue(route.Destination, out var existing) && existing != identityHeader)
                throw new InvalidOperationException($"{endpointDescription} '{route.Destination}': CloudEvents routes must use the same HeaderNames identity binding. Configure matching HeaderNames for every route to this queue.");
            headers[route.Destination] = identityHeader;
        }
        return headers;
    }
    internal SqsSendTransport(string name, IAmazonSQS client, bool ownsClient, AmazonSqsTransportOptions options,
        MessageTracing? tracing = null, ILoggerFactory? logging = null, IReadOnlyDictionary<string, string>? identityHeaders = null)
    {
        Name = name;
        _client = client;
        _ownsClient = ownsClient;
        _tracing = tracing;
        var endpoints = new Dictionary<string, Endpoint>(StringComparer.Ordinal);
        foreach (var declaration in options.Queues)
        {
            var endpoint = new Endpoint(declaration, identityHeaders?.GetValueOrDefault(declaration.QueueName) ?? "ce_id");
            if (tracing is not null)
                endpoint.Tracing = new(tracing, new() { System = "aws_sqs", Destination = declaration.QueueName },
                    SendCoreAsync, SendBatchCoreAsync, logging?.CreateLogger<SqsSendTransport>());
            endpoints.Add(declaration.QueueName, endpoint);
        }
        _endpoints = endpoints.ToFrozenDictionary(StringComparer.Ordinal);
    }

    public string Name { get; }
    private Endpoint GetEndpoint(string destination) => _endpoints.TryGetValue(destination, out var endpoint) ? endpoint
        : throw new InvalidOperationException($"SQS transport '{Name}': undeclared destination '{destination}'.");
    public TransportCapabilities GetCapabilities(string endpoint) => GetEndpoint(endpoint).Capabilities;
    public SendTracing? GetSendTracing(string destination) => GetEndpoint(destination).Tracing;

    public async ValueTask<TopologyReport> ApplyTopologyAsync(TopologyMode mode, CancellationToken cancellationToken)
    {
        Check(cancellationToken);
        if (mode == TopologyMode.None) return new([]);
        if (mode is not (TopologyMode.Validate or TopologyMode.Provision)) throw new ArgumentOutOfRangeException(nameof(mode));
        var differences = new List<string>();
        foreach (var endpoint in _endpoints.Values)
        {
            try { endpoint.Url = (await _client.GetQueueUrlAsync(endpoint.Declaration.QueueName, cancellationToken).ConfigureAwait(false)).QueueUrl; }
            catch (QueueDoesNotExistException)
            {
                if (mode == TopologyMode.Validate)
                {
                    differences.Add($"Missing SQS queue '{endpoint.Declaration.QueueName}'. Provision it outside the application or use Provision explicitly.");
                    continue;
                }
                endpoint.Url = (await _client.CreateQueueAsync(endpoint.Declaration, cancellationToken).ConfigureAwait(false)).QueueUrl;
                // AWS requires a one-second wait after CreateQueue before using the new queue.
                await Task.Delay(TimeSpan.FromSeconds(1), cancellationToken).ConfigureAwait(false);
            }
            var actual = await _client.GetQueueAttributesAsync(new GetQueueAttributesRequest
            {
                QueueUrl = endpoint.Url, AttributeNames = ["All"],
            }, cancellationToken).ConfigureAwait(false);
            if (endpoint.Declaration.Attributes is { } expected)
                foreach (var attribute in expected)
                    if (actual.Attributes?.TryGetValue(attribute.Key, out var value) != true || value is null || !AttributeEquals(attribute.Key, attribute.Value, value))
                        differences.Add($"SQS queue '{endpoint.Declaration.QueueName}': {attribute.Key} differs. Reconcile it through infrastructure configuration; Relayer never modifies or deletes existing queues.");
            if (endpoint.Declaration.Tags is { Count: > 0 } tags)
            {
                var actualTags = await _client.ListQueueTagsAsync(new ListQueueTagsRequest { QueueUrl = endpoint.Url }, cancellationToken).ConfigureAwait(false);
                foreach (var tag in tags)
                    if (actualTags.Tags?.TryGetValue(tag.Key, out var value) != true || value != tag.Value)
                        differences.Add($"SQS queue '{endpoint.Declaration.QueueName}': declared tag '{tag.Key}' differs. Reconcile it through infrastructure configuration.");
            }
            if (actual.Attributes?.TryGetValue("MaximumMessageSize", out var maximum) == true)
                endpoint.Capabilities = endpoint.Capabilities with { MaxMessageBytes = long.Parse(maximum, CultureInfo.InvariantCulture) };
        }
        return new(differences);
    }

    private static bool AttributeEquals(string name, string expected, string actual)
    {
        if (name is not ("Policy" or "RedrivePolicy" or "RedriveAllowPolicy")) return expected == actual;
        using var expectedJson = JsonDocument.Parse(expected);
        using var actualJson = JsonDocument.Parse(actual);
        return JsonElement.DeepEquals(expectedJson.RootElement, actualJson.RootElement);
    }

    public ValueTask SendAsync(OutgoingMessage message, CancellationToken cancellationToken) =>
        GetEndpoint(message.Destination).Tracing is { } tracing ? tracing.SendAsync(message, cancellationToken) : SendCoreAsync(message, cancellationToken);

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    private async ValueTask SendCoreAsync(OutgoingMessage message, CancellationToken cancellationToken)
    {
        var endpoint = Validate(message, cancellationToken);
        var state = _operations.Rent();
        state.Used = 1;
        var activity = CurrentActivity();
        Enrich(activity, endpoint.Url!, null);
        try
        {
            var size = state.Messages[0].Prepare(message, endpoint.Url!, endpoint.Fifo, endpoint.IdentityHeader);
            ValidateSize(endpoint, size);
            endpoint.Tracing?.RecordNativeSend();
            var duration = endpoint.Tracing?.StartNativeSend() ?? default;
            var failed = true;
            try
            {
                var response = await _client.SendMessageAsync(state.Messages[0].Request, cancellationToken).ConfigureAwait(false);
                failed = false;
                Enrich(activity, endpoint.Url!, response.ResponseMetadata?.RequestId);
            }
            finally { duration.Complete(failed); }
        }
        catch (AmazonSQSException error)
        {
            Enrich(activity, endpoint.Url!, error.RequestId);
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
        state.Batch.QueueUrl = endpoint.Url;
        try
        {
            for (var i = 0; i < messages.Length; i++)
            {
                Validate(messages.Span[i], cancellationToken);
                if (count == 10)
                {
                    await FlushAsync(state, endpoint, batchStart, cancellationToken).ConfigureAwait(false);
                    for (var j = 0; j < count; j++) state.Messages[j].Clear();
                    state.Batch.Entries.Clear();
                    state.Used = 0;
                    count = bytes = 0;
                    batchStart = i;
                }
                var pending = state.Messages[count];
                state.Used = count + 1;
                var size = pending.Prepare(messages.Span[i], endpoint.Url!, endpoint.Fifo, endpoint.IdentityHeader);
                ValidateSize(endpoint, size);
                if (count > 0 && bytes + size > 1_048_576)
                {
                    await FlushAsync(state, endpoint, batchStart, cancellationToken).ConfigureAwait(false);
                    for (var j = 0; j < count; j++) state.Messages[j].Clear();
                    state.Batch.Entries.Clear();
                    // Carry the already prepared request across a byte boundary without repeating its callback.
                    (state.Messages[0], state.Messages[count]) = (pending, state.Messages[0]);
                    state.Used = 1;
                    count = bytes = 0;
                    batchStart = i;
                }
                pending.CopyToEntry();
                pending.Entry.Id = state.Ids[count];
                state.Batch.Entries.Add(pending.Entry);
                count++;
                bytes += size;
            }
            if (count > 0) await FlushAsync(state, endpoint, batchStart, cancellationToken).ConfigureAwait(false);
        }
        finally { _operations.Return(state); }
    }

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    private async ValueTask FlushAsync(SqsOperationState state, Endpoint endpoint, int offset, CancellationToken cancellationToken)
    {
        var activity = CurrentActivity();
        Enrich(activity, state.Batch.QueueUrl, null);
        endpoint.Tracing?.RecordNativeSend(state.Batch.Entries.Count);
        var duration = endpoint.Tracing?.StartNativeSend() ?? default;
        var failed = true;
        try
        {
            var response = await _client.SendMessageBatchAsync(state.Batch, cancellationToken).ConfigureAwait(false);
            Enrich(activity, state.Batch.QueueUrl, response.ResponseMetadata?.RequestId);
            if (response.Failed is { Count: > 0 }) throw new AmazonSqsBatchException(response, offset);
            failed = false;
        }
        catch (AmazonSQSException error)
        {
            Enrich(activity, state.Batch.QueueUrl, error.RequestId);
            throw;
        }
        finally { duration.Complete(failed); }
    }

    private Activity? CurrentActivity() => Activity.Current is { } activity && ReferenceEquals(activity.Source, _tracing?.Source) ? activity : null;
    private static void Enrich(Activity? activity, string url, string? requestId)
    {
        activity?.SetTag("aws.sqs.queue.url", url);
        if (requestId is not null) activity?.SetTag("aws.request_id", requestId);
    }

    private Endpoint Validate(OutgoingMessage message, CancellationToken cancellationToken)
    {
        Check(cancellationToken);
        var endpoint = GetEndpoint(message.Destination);
        if (endpoint.Url is null) throw new InvalidOperationException($"SQS transport '{Name}': start the host or apply topology before sending to '{message.Destination}'.");
        if (message.IsTombstone) throw new NotSupportedException("SQS does not support native tombstones.");
        if (message.OrderingKey is not null && !endpoint.Fifo)
            throw new NotSupportedException("SQS ordering keys require a FIFO queue.");
        return endpoint;
    }

    private static void ValidateSize(Endpoint endpoint, int size)
    {
        if (size > (endpoint.Capabilities.MaxMessageBytes ?? 1_048_576))
            throw new InvalidOperationException($"SQS message for '{endpoint.Declaration.QueueName}' exceeds the native message byte limit, including attributes.");
    }

    private void Check(CancellationToken cancellationToken)
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
        cancellationToken.ThrowIfCancellationRequested();
    }

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        Check(cancellationToken);
        // None skips topology validation/provisioning, but the native send API still requires queue URLs.
        foreach (var endpoint in _endpoints.Values)
            if (endpoint.Url is null)
                endpoint.Url = (await _client.GetQueueUrlAsync(endpoint.Declaration.QueueName, cancellationToken).ConfigureAwait(false)).QueueUrl;
    }
    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
    public ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) == 0 && _ownsClient) _client.Dispose();
        return ValueTask.CompletedTask;
    }

    private sealed class Endpoint
    {
        internal Endpoint(CreateQueueRequest declaration, string identityHeader)
        {
            Declaration = declaration;
            IdentityHeader = identityHeader;
            Fifo = declaration.QueueName.EndsWith(".fifo", StringComparison.Ordinal);
            Capabilities = new() { OrderedByKey = Fifo, MaxBatchSize = 10, MaxDeliveryDelay = Fifo ? null : TimeSpan.FromMinutes(15) };
            if (declaration.Attributes?.TryGetValue("MaximumMessageSize", out var maximum) == true)
                Capabilities = Capabilities with { MaxMessageBytes = long.Parse(maximum, CultureInfo.InvariantCulture) };
        }
        internal CreateQueueRequest Declaration { get; }
        internal bool Fifo { get; }
        internal string IdentityHeader { get; }
        internal string? Url { get; set; }
        internal TransportCapabilities Capabilities { get; set; }
        internal SendTracing? Tracing { get; set; }
    }
}
