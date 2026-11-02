namespace Relayer;

internal sealed class RequestClient<TRequest, TResponse>(SendPipeline pipeline, RequestReplies<TRequest, TResponse> replies)
    : IRequestClient<TRequest, TResponse>
{
    private readonly SendRoute<TRequest> _route = pipeline.GetRoute<TRequest>();

    public async ValueTask<TResponse> RequestAsync(TRequest request, TimeSpan timeout, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        if (timeout <= TimeSpan.Zero || timeout.TotalMilliseconds > uint.MaxValue - 1)
            throw new ArgumentOutOfRangeException(nameof(timeout), "A request requires a finite positive timeout of at most 4294967294 ms.");
        cancellationToken.ThrowIfCancellationRequested();
        if (_route.Options.EnvelopeMode != MessageEnvelopeMode.CloudEvents || _route.Options.TransactionRequirement == SendTransactionRequirement.Outbox)
            throw new InvalidOperationException("Requests require CloudEvents and an immediate send route without an outbox transaction requirement.");
        var pending = replies.Reserve();
        try
        {
            using var deadline = new CancellationTokenSource(timeout, replies.Clock);
            using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, deadline.Token);
            try
            {
                // Do not wait on a handler's send buffer or an uncommitted outbox transaction.
                await _route.SendAsync(request, null, null, cancellation.Token, null,
                    new(new(replies.Address), new(pending.Correlation))).ConfigureAwait(false);
                cancellation.Token.ThrowIfCancellationRequested();
                return await pending.Completion.Task.WaitAsync(cancellation.Token).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                throw new OperationCanceledException(cancellationToken);
            }
            catch (OperationCanceledException error) when (deadline.IsCancellationRequested)
            {
                throw new TimeoutException("The request exceeded its timeout while sending or waiting for a reply.", error);
            }
        }
        finally { replies.Release(pending); }
    }
}
