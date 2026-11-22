using System.Diagnostics;
using Google.Cloud.PubSub.V1;
using Relayer.Transports;

namespace Relayer.GooglePubSub;

internal sealed class GooglePubSubConsumer(Func<SubscriberClient> create, MessageTracing tracing, string topic, IGooglePubSubClients clients, string? subscription = null) : IAsyncDisposable
{
    private static readonly Task<SubscriberClient.Reply> s_ack = Task.FromResult(SubscriberClient.Reply.Ack);
    private static readonly Task<SubscriberClient.Reply> s_nack = Task.FromResult(SubscriberClient.Reply.Nack);
    private readonly MessageOperationTracing _ack = new(tracing, new() { System = "gcp_pubsub", Destination = topic, Subscription = subscription }, MessagingOperation.Settle, "ack");
    private readonly Lock _lifecycle = new();
    private SubscriberClient? _native;
    private Task? _shutdown;
    private CancellationToken _stopDeadline;
    private readonly GooglePubSubPool<GooglePubSubReply> _replies = new(static () => new());

    internal Func<GooglePubSubDelivery, CancellationToken, ValueTask<Disposition>> Receive
    {
        get; set;
    } = null!;

    internal Task Completion
    {
        get; private set;
    } = Task.CompletedTask;

    internal void Start()
    {
        lock (_lifecycle)
        {
            if (_native is not null || _shutdown is { IsCompleted: false })
            {
                throw new InvalidOperationException("Pub/Sub subscriber is already running or shutting down.");
            }

            if (Completion.IsFaulted || Completion.IsCanceled || _shutdown is { IsCompletedSuccessfully: false })
            {
                throw new InvalidOperationException("Recreate the application host after a failed Pub/Sub subscriber.");
            }

            _native = create();
            _shutdown = null;

            if (ExecutionContext.IsFlowSuppressed())
            {
                Completion = _native.StartAsync(HandleAsync);
            }
            else
            {
                using (ExecutionContext.SuppressFlow())
                {
                    Completion = _native.StartAsync(HandleAsync);
                }
            }
        }
    }

    private Task<SubscriberClient.Reply> HandleAsync(PubsubMessage message, CancellationToken token)
    {
        var reply = _replies.Rent();
        var asynchronous = false;

        try
        {
            var operation = Receive(new(message, _native!)
            {
                Reply = reply,
                Clients = clients
            }, token);

            if (!operation.IsCompletedSuccessfully)
            {
                asynchronous = true;
                return AwaitAsync(operation, reply);
            }

            var disposition = operation.GetAwaiter().GetResult();
            return (reply.Value ?? Resolve(disposition)) == SubscriberClient.Reply.Ack ? s_ack : s_nack;
        }
        finally
        {
            if (!asynchronous)
            {
                Return(reply);
            }
        }
    }

    private async Task<SubscriberClient.Reply> AwaitAsync(ValueTask<Disposition> operation, GooglePubSubReply reply)
    {
        try
        {
            var disposition = await operation.ConfigureAwait(false);
            return reply.Value ?? Resolve(disposition);
        }
        finally
        {
            Return(reply);
        }
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

        if (delivery.Reply is null)
        {
            throw new InvalidOperationException("Settlement requires an active Pub/Sub callback.");
        }

        if (delivery.Reply.Value is not null)
        {
            throw new InvalidOperationException("This Pub/Sub callback already has a settlement decision.");
        }
        // The native callback Reply queues acknowledgement; it does not confirm a broker RPC.
        using var activity = _ack.Start();
        delivery.Reply.Value = reply;
        return default;
    }

    internal static SubscriberClient.Reply Resolve(Disposition disposition)
    {
        return disposition.Kind switch
        {
            DispositionKind.Complete or DispositionKind.SettledByHandler => SubscriberClient.Reply.Ack,
            DispositionKind.Redeliver when disposition.Delay == TimeSpan.Zero && disposition.Headers.IsEmpty => SubscriberClient.Reply.Nack,
            _ => throw new NotSupportedException("Pub/Sub callback settlement supports Ack or immediate Nack only. Persist failure copies before returning Ack; Nack cannot change attributes."),
        };
    }

    internal Task StopAsync(CancellationToken token)
    {
        lock (_lifecycle)
        {
            // Native cancellation signals the handler. Bound the caller's wait independently.
            return GetOrStartShutdown(token).WaitAsync(token);
        }
    }

    private Task GetOrStartShutdown(CancellationToken? stopDeadline)
    {
        if (_shutdown is not null)
        {
            return _shutdown;
        }

        if (_native is null)
        {
            return Task.CompletedTask;
        }

        _stopDeadline = stopDeadline.GetValueOrDefault();
        _shutdown = ShutdownAsync(_native, stopDeadline);
        return _shutdown;
    }

    private async Task ShutdownAsync(SubscriberClient native, CancellationToken? stopDeadline)
    {
        try
        {
            // The SDK's StopAsync and DisposeAsync both return the subscriber's shutdown completion.
            if (stopDeadline is { } token)
            {
                await native.StopAsync(new SubscriberClient.ShutdownOptions { Mode = SubscriberClient.ShutdownMode.WaitForProcessing }, token).ConfigureAwait(false);
            }
            else
            {
                await native.DisposeAsync().ConfigureAwait(false);
            }
        }
        catch when (!Completion.IsCompleted)
        {
            // The SDK can invoke cancellation callbacks before returning its shutdown task.
            // Keep the subscriber alive until those callbacks and native shutdown finish.
            await Task.WhenAny(Completion).ConfigureAwait(false);
            throw;
        }
        finally
        {
            lock (_lifecycle)
            {
                _native = null;
            }
        }
    }

    public async ValueTask DisposeAsync()
    {
        Task shutdown;
        CancellationToken stopDeadline;

        lock (_lifecycle)
        {
            shutdown = GetOrStartShutdown(null);
            stopDeadline = _stopDeadline;
        }

        try
        {
            await shutdown.ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (stopDeadline.IsCancellationRequested &&
            (Completion.IsCanceled || Completion.Exception?.InnerExceptions.All(static error => error is OperationCanceledException) == true))
        {
            // The SDK can report a hard stop as a canceled task or as cancellation exceptions from its channels.
        }
    }
}
