using System.Runtime.CompilerServices;
using Relayer.Transports;

namespace Relayer;

internal enum SendMode
{
    Automatic,
    Direct,
    Outbox
}

internal class Sender(SendPipeline pipeline, IOutboxEnlistment? outbox = null) : ISender
{
    private const string OutboxActivationHint = "Begin an outbox transaction or, with EF Core, call db.EnlistRelayerOutbox() before sending.";

    internal IOutboxEnlistment? Outbox
    {
        get; set;
    } = outbox;

    internal TenantContext? Tenant
    {
        get; set;
    }

    internal AttemptSendBuffer? AttemptBuffer
    {
        get; set;
    }

    internal virtual void EndAttempt()
    {
        AttemptBuffer = null;
    }

    internal ValueTask ReplyAsync<TMessage>(TMessage message, WireString correlationId, CancellationToken cancellationToken)
    {
        if (cancellationToken.IsCancellationRequested)
        {
            return ValueTask.FromCanceled(cancellationToken);
        }

        var enlistment = ResolveOutbox(SendMode.Automatic, "ReplyAsync requires an active outbox transaction when an outbox is registered. " + OutboxActivationHint);
        return pipeline.GetRoute<TMessage>().SendAsync(message, null, enlistment,
                    cancellationToken, AttemptBuffer, new(default, correlationId), Tenant?.Id ?? default);
    }

    public ValueTask SendAsync<TMessage>(TMessage message, CancellationToken cancellationToken = default)
    {
        return Send(message, null, SendMode.Automatic, cancellationToken);
    }

    public ValueTask SendAsync<TMessage>(TMessage message, Action<SendOptions> configure, CancellationToken cancellationToken = default)
    {
        return Send(message, configure ?? throw new ArgumentNullException(nameof(configure)), SendMode.Automatic, cancellationToken);
    }

    public ValueTask SendWithoutOutboxAsync<TMessage>(TMessage message, CancellationToken cancellationToken = default)
    {
        return Send(message, null, SendMode.Direct, cancellationToken);
    }

    public ValueTask SendWithoutOutboxAsync<TMessage>(TMessage message, Action<SendOptions> configure, CancellationToken cancellationToken = default)
    {
        return Send(message, configure ?? throw new ArgumentNullException(nameof(configure)), SendMode.Direct, cancellationToken);
    }

    public ValueTask SendViaOutboxAsync<TMessage>(TMessage message, CancellationToken cancellationToken = default)
    {
        return Send(message, null, SendMode.Outbox, cancellationToken);
    }

    public ValueTask SendViaOutboxAsync<TMessage>(TMessage message, Action<SendOptions> configure, CancellationToken cancellationToken = default)
    {
        return Send(message, configure ?? throw new ArgumentNullException(nameof(configure)), SendMode.Outbox, cancellationToken);
    }

    private ValueTask Send<TMessage>(TMessage message, Action<SendOptions>? configure, SendMode mode, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(message);

        if (cancellationToken.IsCancellationRequested)
        {
            return ValueTask.FromCanceled(cancellationToken);
        }

        var enlistment = ResolveOutbox(mode, "SendAsync requires an active outbox transaction when an outbox is registered. " +
            OutboxActivationHint + " Use SendWithoutOutboxAsync for an intentional direct send.");
        return pipeline.GetTarget<TMessage>().SendAsync(message, configure, enlistment, cancellationToken, AttemptBuffer, tenant: Tenant?.Id ?? default);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private IOutboxEnlistment? ResolveOutbox(SendMode mode, string inactiveOutboxMessage)
    {
        var enlistment = mode != SendMode.Direct && Outbox is { IsActive: true } active ? active : null;

        if (mode == SendMode.Outbox && enlistment is null)
        {
            throw new InvalidOperationException("SendViaOutboxAsync requires an outbox with an active transaction in this scope.");
        }

        if (mode == SendMode.Automatic && Outbox is not null && enlistment is null)
        {
            throw new InvalidOperationException(inactiveOutboxMessage);
        }

        return enlistment;
    }
}
