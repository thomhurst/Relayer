using Relayer.Transports;
using Amazon.SQS;
using Amazon.SQS.Model;
using Microsoft.Extensions.Logging;
using System.Runtime.CompilerServices;

namespace Relayer.AmazonSqs;

// The SDK owns request retries; workers issue new long polls after exhausted transient failures.
// FIFO polls stay single-message so SQS itself serializes groups across processes.
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
        ArgumentNullException.ThrowIfNull(consumer.Failure);
        ArgumentNullException.ThrowIfNull(consumer.Options.HeaderNames);
        if (!transport.Queues.Any(queue => queue.QueueName == consumer.Source))
            throw new InvalidOperationException($"SQS consumer '{consumer.Source}' on '{name}' needs a native CreateQueueRequest declaration.");
        if (consumer.BatchSize != 1) throw new NotSupportedException("SQS consumers currently require single-message handlers (BatchSize=1).");
        if (string.IsNullOrWhiteSpace(consumer.Failure.DeadLetterEndpoint))
            throw new InvalidOperationException($"SQS consumer '{consumer.Source}': configure Failure.DeadLetterEndpoint; SQS redrive cannot attach processing failure metadata.");
        if (consumer.Failure.DeadLetterEndpoint == consumer.Source)
            throw new InvalidOperationException("SQS dead-letter destination must differ from its source.");
        if (!transport.Queues.Any(queue => queue.QueueName == consumer.Failure.DeadLetterEndpoint))
            throw new InvalidOperationException("Declare the SQS dead-letter queue in transport Options.Queues.");
        if (consumer.Source.EndsWith(".fifo", StringComparison.Ordinal) != consumer.Failure.DeadLetterEndpoint.EndsWith(".fifo", StringComparison.Ordinal))
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
        if (request.QueueUrl != _url || request.MaxNumberOfMessages is not (>= 1 and <= 10) ||
            (Declaration.Source.EndsWith(".fifo", StringComparison.Ordinal) && request.MaxNumberOfMessages != 1) || request.WaitTimeSeconds is not (>= 1 and <= 20) ||
            request.VisibilityTimeout is < 0 or > 43_200 || request.ReceiveRequestAttemptId is not null ||
            request.MessageAttributeNames?.Contains("All") != true || request.MessageSystemAttributeNames?.Contains("All") != true)
            throw new InvalidOperationException("SQS receive request requires the declared QueueUrl, MaxNumberOfMessages=1–10 (FIFO: 1), WaitTimeSeconds=1–20, all message/system attributes and no fixed ReceiveRequestAttemptId. Native visibility is 0–43200 seconds.");
        return request;
    }

    private async Task RunAsync(int slot, ReceiveMessageRequest request, CancellationToken poll, CancellationToken abort)
    {
        using var delivery = new SqsDelivery(_client, _url, _time);
        using var batch = request.MaxNumberOfMessages > 1 ? new SqsReceiveBatch(this, request.MaxNumberOfMessages.Value) : null;
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
            // A poll that won the stop race still drains every returned message before clients close.
            if (batch is not null)
            {
                if (TrackHealth) Volatile.Write(ref _active[slot], _time.GetUtcNow().UtcTicks);
                try { await batch.ProcessAsync(response, receivedAt, abort).ConfigureAwait(false); }
                finally { if (TrackHealth) Volatile.Write(ref _active[slot], 0); }
                continue;
            }
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

    // Serial handlers can retain their contexts at the settlement boundary. The native batch
    // response then releases each pipeline separately, including its tracing/error observers.
    internal sealed class SqsReceiveBatch : IDisposable
    {
        private readonly SqsConsumer _owner;
        private readonly SqsBatchSlot[] _slots;
        private readonly DeleteMessageBatchRequest _delete;

        internal SqsReceiveBatch(SqsConsumer owner, int size)
        {
            _owner = owner;
            _slots = new SqsBatchSlot[size];
            for (var i = 0; i < size; i++) _slots[i] = new(owner._client, owner._url, owner._time, i);
            _delete = new() { QueueUrl = owner._url, Entries = new(size) };
        }

        [RuntimeAsyncMethodGeneration(false)]
        [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
        internal async ValueTask ProcessAsync(ReceiveMessageResponse response, long receivedAt, CancellationToken abort)
        {
            var started = 0;
            _delete.Entries.Clear();
            for (var i = 0; i < response.Messages.Count && !abort.IsCancellationRequested; i++)
            {
                var slot = _slots[i];
                slot.Reset();
                slot.Delivery.Prepare(response.Messages[i], receivedAt, response.ResponseMetadata?.RequestId);
                slot.Process(_owner, abort);
                started++;
                await slot.Ready.ConfigureAwait(false);
                if (slot.PendingDelete) _delete.Entries.Add(slot.Entry);
            }
            DeleteMessageBatchResponse? deleted = null;
            Exception? failure = null;
            if (_delete.Entries.Count > 0)
            {
                try { deleted = await _owner._client.DeleteMessageBatchAsync(_delete, abort).ConfigureAwait(false); }
                catch (Exception error) { failure = error; }
            }
            for (var i = 0; i < started; i++)
            {
                var slot = _slots[i];
                if (!slot.PendingDelete) continue;
                Exception? error = failure;
                if (error is null)
                {
                    var success = false;
                    if (deleted?.Successful is { } successes)
                        foreach (var entry in successes)
                            if (entry.Id == slot.Entry.Id) { success = true; break; }
                    if (deleted?.Failed is { } failures)
                        foreach (var entry in failures)
                            if (entry.Id == slot.Entry.Id)
                            {
                                error = new AmazonSQSException(entry.Message) { ErrorCode = entry.Code, RequestId = deleted.ResponseMetadata?.RequestId };
                                break;
                            }
                    if (error is null && !success)
                        error = new InvalidOperationException("SQS DeleteMessageBatch omitted the delivery's result; native redelivery remains enabled.");
                }
                slot.DeleteRequestId = deleted?.ResponseMetadata?.RequestId;
                slot.CompleteDelete(error);
            }
            for (var i = 0; i < started; i++)
            {
                await _slots[i].Processing.ConfigureAwait(false);
                _slots[i].Entry.ReceiptHandle = null;
            }
            _delete.Entries.Clear();
        }

        public void Dispose()
        {
            foreach (var slot in _slots) slot.Dispose();
        }
    }

    internal ValueTask Receive(SqsDelivery delivery, CancellationToken abort) => _receive(delivery, abort);
    internal void ReportProcessingFailure(Exception error)
    {
        if (_logger is { } logger) ProcessingFailed(logger, error, Declaration.Source);
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
