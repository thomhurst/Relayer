using System.Diagnostics;
using Google.Cloud.PubSub.V1;
using Relayer.Transports;

namespace Relayer.GooglePubSub;

internal sealed class GooglePubSubConsumer(Func<SubscriberClient> create, MessageTracing tracing, string topic, IGooglePubSubClients clients, string? subscription = null) : IAsyncDisposable
{
    private static readonly Task<SubscriberClient.Reply> Ack = Task.FromResult(SubscriberClient.Reply.Ack);
    private static readonly Task<SubscriberClient.Reply> Nack = Task.FromResult(SubscriberClient.Reply.Nack);
    private readonly MessageOperationTracing _ack = new(tracing, new() { System = "gcp_pubsub", Destination = topic, Subscription = subscription }, MessagingOperation.Settle, "ack");
    private bool _started;
    private bool _stopped;
    private SubscriberClient? _native;
    private readonly GooglePubSubPool<GooglePubSubReply> _replies = new(static () => new());
    internal Func<GooglePubSubDelivery, CancellationToken, ValueTask<Disposition>> Receive { get; set; } = null!;
    internal Task Completion { get; private set; } = Task.CompletedTask;

    internal void Start()
    {
        if (_started && !_stopped) throw new InvalidOperationException("Pub/Sub subscriber is already running.");
        if (Completion.IsFaulted || Completion.IsCanceled) throw new InvalidOperationException("Recreate the application host after a failed Pub/Sub subscriber.");
        _started = true;
        _stopped = false;
        _native = create();
        if (ExecutionContext.IsFlowSuppressed()) Completion = _native.StartAsync(HandleAsync);
        else { using (ExecutionContext.SuppressFlow()) Completion = _native.StartAsync(HandleAsync); }
    }

    private Task<SubscriberClient.Reply> HandleAsync(PubsubMessage message, CancellationToken token)
    {
        var reply = _replies.Rent();
        var asynchronous = false;
        try
        {
            var operation = Receive(new(message, _native!) { Reply = reply, Clients = clients }, token);
            if (!operation.IsCompletedSuccessfully)
            {
                asynchronous = true;
                return AwaitAsync(operation, reply);
            }
            var disposition = operation.GetAwaiter().GetResult();
            return (reply.Value ?? Resolve(disposition)) == SubscriberClient.Reply.Ack ? Ack : Nack;
        }
        finally { if (!asynchronous) Return(reply); }
    }

    private async Task<SubscriberClient.Reply> AwaitAsync(ValueTask<Disposition> operation, GooglePubSubReply reply)
    {
        try
        {
            var disposition = await operation.ConfigureAwait(false);
            return reply.Value ?? Resolve(disposition);
        }
        finally { Return(reply); }
    }

    private void Return(GooglePubSubReply reply)
    {
        reply.Value = null;
        _replies.Return(reply);
    }

    internal ValueTask SettleContextAsync(MessageContext context)
    {
        var feature = context.Features.GetRequired<NativeMessage<GooglePubSubDelivery>>();
        var result = SettleAsync(feature.Message, feature.Disposition, context.CancellationToken);
        feature.Disposition = Disposition.SettledByHandler;
        return result;
    }

    internal ValueTask SettleAsync(GooglePubSubDelivery delivery, Disposition disposition, CancellationToken token)
    {
        token.ThrowIfCancellationRequested();
        var reply = Resolve(disposition);
        if (delivery.Reply is null) throw new InvalidOperationException("Settlement requires an active Pub/Sub callback.");
        if (delivery.Reply.Value is not null) throw new InvalidOperationException("This Pub/Sub callback already has a settlement decision.");
        // The native callback Reply queues acknowledgement; it does not confirm a broker RPC.
        using var activity = _ack.Start();
        delivery.Reply.Value = reply;
        return default;
    }

    internal static SubscriberClient.Reply Resolve(Disposition disposition) => disposition.Kind switch
    {
        DispositionKind.Complete or DispositionKind.SettledByHandler => SubscriberClient.Reply.Ack,
        DispositionKind.Redeliver when disposition.Delay == TimeSpan.Zero && disposition.Headers.IsEmpty => SubscriberClient.Reply.Nack,
        _ => throw new NotSupportedException("Pub/Sub callback settlement supports Ack or immediate Nack only. Persist failure copies before returning Ack; Nack cannot change attributes."),
    };

    internal async Task StopAsync(CancellationToken token)
    {
        if (!_started || _stopped) return;
        await _native!.StopAsync(new SubscriberClient.ShutdownOptions { Mode = SubscriberClient.ShutdownMode.WaitForProcessing }, token).ConfigureAwait(false);
        await Completion.ConfigureAwait(false);
        await _native.DisposeAsync().ConfigureAwait(false);
        _native = null;
        _stopped = true;
    }

    public async ValueTask DisposeAsync()
    {
        if (_native is not null) await _native.DisposeAsync().ConfigureAwait(false);
        _native = null;
    }
}
