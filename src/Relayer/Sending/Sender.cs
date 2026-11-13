using Relayer.Transports;
namespace Relayer;

internal enum SendMode { Automatic, Direct, Outbox }

internal class Sender(SendPipeline pipeline, IOutboxEnlistment? outbox = null) : ISender
{
    internal IOutboxEnlistment? Outbox { get; set; } = outbox;
    internal TenantContext? Tenant { get; set; }
    internal AttemptSendBuffer? AttemptBuffer { get; set; }

    internal virtual void EndAttempt() => AttemptBuffer = null;

    internal ValueTask ReplyAsync<TMessage>(TMessage message, WireString correlationId, CancellationToken cancellationToken) =>
        pipeline.GetRoute<TMessage>().SendAsync(message, null, Outbox is { IsActive: true } ? Outbox : null,
            cancellationToken, AttemptBuffer, new(default, correlationId), Tenant?.Id ?? default);

    public ValueTask SendAsync<TMessage>(TMessage message, CancellationToken cancellationToken = default) =>
        Send(message, null, SendMode.Automatic, cancellationToken);

    public ValueTask SendAsync<TMessage>(TMessage message, Action<SendOptions> configure, CancellationToken cancellationToken = default) =>
        Send(message, configure ?? throw new ArgumentNullException(nameof(configure)), SendMode.Automatic, cancellationToken);

    public ValueTask SendNowAsync<TMessage>(TMessage message, CancellationToken cancellationToken = default) =>
        Send(message, null, SendMode.Direct, cancellationToken);

    public ValueTask SendNowAsync<TMessage>(TMessage message, Action<SendOptions> configure, CancellationToken cancellationToken = default) =>
        Send(message, configure ?? throw new ArgumentNullException(nameof(configure)), SendMode.Direct, cancellationToken);

    public ValueTask SendViaOutboxAsync<TMessage>(TMessage message, CancellationToken cancellationToken = default) =>
        Send(message, null, SendMode.Outbox, cancellationToken);

    public ValueTask SendViaOutboxAsync<TMessage>(TMessage message, Action<SendOptions> configure, CancellationToken cancellationToken = default) =>
        Send(message, configure ?? throw new ArgumentNullException(nameof(configure)), SendMode.Outbox, cancellationToken);

    private ValueTask Send<TMessage>(TMessage message, Action<SendOptions>? configure, SendMode mode, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(message);
        if (cancellationToken.IsCancellationRequested)
        {
            return ValueTask.FromCanceled(cancellationToken);
        }
        var enlistment = mode != SendMode.Direct && Outbox is { IsActive: true } ? Outbox : null;
        if (mode == SendMode.Outbox && enlistment is null)
        {
            throw new InvalidOperationException("SendViaOutboxAsync requires an outbox with an active transaction in this scope.");
        }
        return pipeline.GetRoute<TMessage>().SendAsync(message, configure, enlistment, cancellationToken, AttemptBuffer, tenant: Tenant?.Id ?? default);
    }
}
