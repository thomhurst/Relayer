using Amazon.SQS;
using Amazon.SQS.Model;
using Microsoft.Extensions.Logging;

namespace Relayer.AmazonSqs;

// The SDK owns request retries; workers issue new long polls after exhausted transient failures.
// One message per poll lets SQS itself serialize FIFO groups, including across processes.
internal sealed partial class SqsConsumer : IAsyncDisposable
{
    private readonly IAmazonSQS _client;
    private readonly string _url;
    private readonly Func<SqsDelivery, CancellationToken, ValueTask> _receive;
    private readonly ILogger? _logger;
    private readonly TimeProvider _time;
    private readonly long[] _active;
    private CancellationTokenSource? _poll;
    private CancellationTokenSource? _abort;
    private Task[] _workers = [];
    private int _running;
    private int _disposed;
    internal ConsumerDefinition<AmazonSqsConsumerOptions> Declaration { get; }
    internal bool TrackHealth { get; set; }

    internal SqsConsumer(ConsumerDefinition<AmazonSqsConsumerOptions> declaration, IAmazonSQS client, string url,
        Func<SqsDelivery, CancellationToken, ValueTask> receive, TimeProvider? time = null, ILogger? logger = null)
    {
        Declaration = declaration;
        _client = client;
        _url = url;
        _receive = receive;
        _time = time ?? TimeProvider.System;
        _logger = logger;
        _active = new long[declaration.Concurrency];
    }

    internal static void ValidateDeclaration(string name, AmazonSqsTransportOptions transport, ConsumerDefinition<AmazonSqsConsumerOptions> consumer)
    {
        ArgumentNullException.ThrowIfNull(consumer.Options.Failure);
        if (!transport.Queues.Any(queue => queue.QueueName == consumer.Source))
            throw new InvalidOperationException($"SQS consumer '{consumer.Source}' on '{name}' needs a native CreateQueueRequest declaration.");
        if (consumer.BatchSize != 1) throw new NotSupportedException("SQS consumers currently require single-message handlers (BatchSize=1).");
        if (string.IsNullOrWhiteSpace(consumer.Options.Failure.DeadLetterEndpoint))
            throw new InvalidOperationException($"SQS consumer '{consumer.Source}': configure Failure.DeadLetterEndpoint; SQS redrive cannot attach processing failure metadata.");
        if (consumer.Options.Failure.DeadLetterEndpoint == consumer.Source)
            throw new InvalidOperationException("SQS dead-letter destination must differ from its source.");
        if (!transport.Queues.Any(queue => queue.QueueName == consumer.Options.Failure.DeadLetterEndpoint))
            throw new InvalidOperationException("Declare the SQS dead-letter queue in transport Options.Queues.");
        if (consumer.Source.EndsWith(".fifo", StringComparison.Ordinal) != consumer.Options.Failure.DeadLetterEndpoint.EndsWith(".fifo", StringComparison.Ordinal))
            throw new InvalidOperationException("SQS source and dead-letter queue must both be standard or both FIFO.");
    }

    internal Task StartAsync(CancellationToken token)
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
        token.ThrowIfCancellationRequested();
        if (_abort?.IsCancellationRequested == true || _workers.Any(worker => !worker.IsCompleted || worker.IsFaulted))
            throw new InvalidOperationException("Recreate the host after an aborted SQS drain or stopped worker.");
        if (Interlocked.CompareExchange(ref _running, 1, 0) != 0) throw new InvalidOperationException("SQS consumer is already running.");
        _poll?.Dispose();
        _abort?.Dispose();
        _poll = new();
        _abort = new();
        _workers = new Task[Declaration.Concurrency];
        var requests = new ReceiveMessageRequest[_workers.Length];
        try
        {
            for (var i = 0; i < requests.Length; i++) requests[i] = CreateRequest();
        }
        catch { _workers = []; Volatile.Write(ref _running, 0); throw; }
        for (var i = 0; i < _workers.Length; i++) _workers[i] = RunAsync(i, requests[i], _poll.Token, _abort.Token);
        return Task.CompletedTask;
    }

    private ReceiveMessageRequest CreateRequest()
    {
        var request = new ReceiveMessageRequest
        {
            QueueUrl = _url, MaxNumberOfMessages = 1, WaitTimeSeconds = 20,
            MessageAttributeNames = ["All"], MessageSystemAttributeNames = ["All"],
        };
        Declaration.Options.ConfigureReceive?.Invoke(request);
        if (request.QueueUrl != _url || request.MaxNumberOfMessages != 1 || request.WaitTimeSeconds is not (>= 1 and <= 20) ||
            request.VisibilityTimeout is < 0 or > 43_200 || request.ReceiveRequestAttemptId is not null ||
            request.MessageAttributeNames?.Contains("All") != true || request.MessageSystemAttributeNames?.Contains("All") != true)
            throw new InvalidOperationException("SQS receive request requires the declared QueueUrl, MaxNumberOfMessages=1, WaitTimeSeconds=1–20, all message/system attributes and no fixed ReceiveRequestAttemptId. Native visibility is 0–43200 seconds.");
        return request;
    }

    private async Task RunAsync(int slot, ReceiveMessageRequest request, CancellationToken poll, CancellationToken abort)
    {
        using var delivery = new SqsDelivery(_client, _url, _time);
        var retrySeconds = 1;
        while (!poll.IsCancellationRequested)
        {
            ReceiveMessageResponse response;
            var receivedAt = _time.GetTimestamp();
            try { response = await _client.ReceiveMessageAsync(request, poll).ConfigureAwait(false); }
            catch (OperationCanceledException) when (poll.IsCancellationRequested) { break; }
            catch (Exception error)
            {
                if (_logger is { } logger) PollFailed(logger, error, Declaration.Source);
                try { await Task.Delay(TimeSpan.FromSeconds(retrySeconds), _time, poll).ConfigureAwait(false); }
                catch (OperationCanceledException) when (poll.IsCancellationRequested) { break; }
                retrySeconds = Math.Min(retrySeconds * 2, 30);
                continue;
            }
            retrySeconds = 1;
            if (response.Messages is not { Count: > 0 }) continue;
            // A poll that won the stop race still drains its returned message before clients close.
            try
            {
                delivery.Prepare(response.Messages[0], receivedAt, response.ResponseMetadata?.RequestId);
                if (TrackHealth) Volatile.Write(ref _active[slot], _time.GetUtcNow().UtcTicks);
                await _receive(delivery, abort).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (abort.IsCancellationRequested) { break; }
            catch (Exception error)
            {
                // No delete on a pipeline/copy/settlement failure. Native visibility makes it available again.
                if (_logger is { } logger) ProcessingFailed(logger, error, Declaration.Source);
            }
            finally { if (TrackHealth) Volatile.Write(ref _active[slot], 0); delivery.Clear(); }
        }
    }

    internal async Task StopAsync(CancellationToken token)
    {
        if (_poll is null) return;
        _poll.Cancel();
        using var registration = token.UnsafeRegister(static state => ((CancellationTokenSource)state!).Cancel(), _abort!);
        try { await Task.WhenAll(_workers).WaitAsync(token).ConfigureAwait(false); }
        finally { Volatile.Write(ref _running, 0); }
    }

    internal (bool IsIdle, DateTimeOffset LastProgressAt)? GetHealthState()
    {
        if (!TrackHealth || Volatile.Read(ref _running) == 0) return null;
        foreach (var worker in _workers)
            if (worker.IsFaulted) throw new InvalidOperationException("SQS receive worker stopped. Recreate the host after correcting the native client failure.", worker.Exception);
        var oldest = long.MaxValue;
        for (var i = 0; i < _active.Length; i++)
        {
            var timestamp = Volatile.Read(ref _active[i]);
            if (timestamp != 0 && timestamp < oldest) oldest = timestamp;
        }
        return oldest == long.MaxValue ? (true, _time.GetUtcNow()) : (false, new(oldest, TimeSpan.Zero));
    }

    [LoggerMessage(Level = LogLevel.Warning, Message = "SQS long poll failed for {Source}; the next native poll will retry connectivity.")]
    private static partial void PollFailed(ILogger logger, Exception exception, string source);
    [LoggerMessage(Level = LogLevel.Error, Message = "SQS handling failed for {Source}; the message remains available for native redelivery.")]
    private static partial void ProcessingFailed(ILogger logger, Exception exception, string source);

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0) return;
        _abort?.Cancel();
        try { await StopAsync(CancellationToken.None).ConfigureAwait(false); }
        finally { _poll?.Dispose(); _abort?.Dispose(); }
    }
}
