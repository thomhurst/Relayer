using System.Diagnostics;
using System.Runtime.CompilerServices;
using Azure.Messaging.ServiceBus;

namespace Relayer.AzureServiceBus;

// One operation set per endpoint, shared by automatic and explicit handler settlement.
internal sealed class ServiceBusSettlement : IConsumeMiddleware
{
    private readonly MessageOperationTracing _complete;
    private readonly MessageOperationTracing _abandon;
    private readonly MessageOperationTracing _defer;
    private readonly MessageOperationTracing _deadLetter;
    private readonly string? _subscription;

    internal ServiceBusSettlement(MessageTracing tracing, MessagingEndpoint endpoint)
    {
        _subscription = endpoint.Subscription;
        _complete = new(tracing, endpoint, MessagingOperation.Settle, "complete");
        _abandon = new(tracing, endpoint, MessagingOperation.Settle, "abandon");
        _defer = new(tracing, endpoint, MessagingOperation.Settle, "defer");
        _deadLetter = new(tracing, endpoint, MessagingOperation.Settle, "dead_letter");
    }

    public ValueTask InvokeAsync(MessageContext context, ConsumeDelegate next)
    {
        context.Features.Set(this);
        return next(context);
    }

    internal ValueTask SettleAsync(MessageContext context)
    {
        var delivery = ServiceBusDelivery.From(context);
        var disposition = delivery.Disposition;
        return SettleAsync(delivery, disposition, context.CancellationToken);
    }

    internal ValueTask SettleAsync(ProcessMessageEventArgs args, Disposition disposition, CancellationToken cancellationToken) =>
        SettleAsync(new ServiceBusDelivery(args), disposition, cancellationToken);

    internal ValueTask SettleAsync(ProcessSessionMessageEventArgs args, Disposition disposition, CancellationToken cancellationToken) =>
        SettleAsync(new ServiceBusDelivery(args), disposition, cancellationToken);

    private ValueTask SettleAsync(ServiceBusDelivery delivery, Disposition disposition, CancellationToken cancellationToken)
    {
        return disposition.Kind switch
        {
            DispositionKind.Complete => ExecuteAsync(delivery, ServiceBusDisposition.Complete, null, null, null, cancellationToken),
            DispositionKind.Redeliver when disposition.Delay == TimeSpan.Zero =>
                ExecuteAsync(delivery, ServiceBusDisposition.Abandon, Properties(disposition.Headers), null, null, cancellationToken),
            DispositionKind.Redeliver => ValueTask.FromException(new NotSupportedException("Service Bus abandon supports immediate redelivery only. Configure RetryDelays for in-process backoff.")),
            DispositionKind.DeadLetter => ExecuteAsync(delivery, ServiceBusDisposition.DeadLetter, Properties(disposition.DeadLetter.Headers),
                disposition.DeadLetter.Reason, disposition.DeadLetter.Description, cancellationToken),
            DispositionKind.SettledByHandler => default,
            _ => ValueTask.FromException(new InvalidOperationException("Unknown Service Bus settlement disposition.")),
        };
    }

    // Error-path metadata only. Successful handling does not materialize headers or dictionaries.
    private static IDictionary<string, object>? Properties(ReadOnlyMemory<KeyValuePair<string, WireString>> headers)
    {
        if (headers.IsEmpty) return null;
        var properties = new Dictionary<string, object>(headers.Length, StringComparer.Ordinal);
        foreach (var header in headers.Span) properties[header.Key] = header.Value.ToString();
        return properties;
    }

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    internal async ValueTask ExecuteAsync(ServiceBusDelivery delivery, ServiceBusDisposition operation,
        IDictionary<string, object>? properties, string? reason, string? description, CancellationToken cancellationToken)
    {
        var tracing = operation switch
        {
            ServiceBusDisposition.Complete => _complete,
            ServiceBusDisposition.Abandon => _abandon,
            ServiceBusDisposition.Defer => _defer,
            _ => _deadLetter,
        };
        using var activity = tracing.Start();
        if (activity is { IsAllDataRequested: true })
        {
            ServiceBusBinding.EnrichActivity(activity, delivery.Message);
            activity.SetTag("messaging.message.id", ServiceBusBinding.TryGetHeader(delivery.Message, "cloudEvents:id", out var id)
                ? id.ToString() : delivery.Message.MessageId);
            if (_subscription is not null) activity.SetTag("messaging.destination.subscription.name", _subscription);
            activity.SetTag("messaging.servicebus.disposition_status", operation switch
            {
                ServiceBusDisposition.Complete => "complete",
                ServiceBusDisposition.Abandon => "abandon",
                ServiceBusDisposition.Defer => "defer",
                _ => "dead_letter",
            });
        }
        try { await delivery.ExecuteAsync(operation, properties, reason, description, cancellationToken).ConfigureAwait(false); }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested) { throw; }
        catch { MessageOperationTracing.Fail(activity, "_OTHER"); throw; }
    }
}

internal enum ServiceBusDisposition { Complete, Abandon, Defer, DeadLetter }

internal readonly struct ServiceBusDelivery
{
    private readonly ProcessMessageEventArgs? _callback;
    private readonly ProcessSessionMessageEventArgs? _sessionCallback;
    private readonly NativeMessage<ProcessMessageEventArgs>? _native;
    private readonly NativeMessage<ProcessSessionMessageEventArgs>? _nativeSession;

    internal ServiceBusDelivery(ProcessMessageEventArgs callback) => _callback = callback;
    internal ServiceBusDelivery(ProcessSessionMessageEventArgs callback) => _sessionCallback = callback;
    private ServiceBusDelivery(NativeMessage<ProcessMessageEventArgs>? native, NativeMessage<ProcessSessionMessageEventArgs>? session)
    { _native = native; _nativeSession = session; _callback = native?.Message; _sessionCallback = session?.Message; }

    internal static ServiceBusDelivery From(MessageContext context)
    {
        var native = context.Features.Get<NativeMessage<ProcessMessageEventArgs>>();
        var session = native is null ? context.Features.GetRequired<NativeMessage<ProcessSessionMessageEventArgs>>() : null;
        return new(native, session);
    }

    internal ProcessMessageEventArgs Callback => _callback ?? throw new InvalidOperationException("This is a session delivery. Use SessionCallback.");
    internal ProcessSessionMessageEventArgs SessionCallback => _sessionCallback ?? throw new InvalidOperationException("This is not a session delivery. Use Callback.");
    internal ServiceBusReceivedMessage Message => _callback?.Message ?? _sessionCallback!.Message;
    internal Disposition Disposition => _native?.Disposition ?? _nativeSession?.Disposition ?? Disposition.Complete;
    internal void MarkSettled()
    {
        if (_native is { } native) native.Disposition = Disposition.SettledByHandler;
        else if (_nativeSession is { } session) session.Disposition = Disposition.SettledByHandler;
    }

    internal Task ExecuteAsync(ServiceBusDisposition operation, IDictionary<string, object>? properties,
        string? reason, string? description, CancellationToken cancellationToken)
    {
        if (_callback is { } callback) return operation switch
        {
            ServiceBusDisposition.Complete => callback.CompleteMessageAsync(callback.Message, cancellationToken),
            ServiceBusDisposition.Abandon => callback.AbandonMessageAsync(callback.Message, properties, cancellationToken),
            ServiceBusDisposition.Defer => callback.DeferMessageAsync(callback.Message, properties, cancellationToken),
            _ => properties is null ? callback.DeadLetterMessageAsync(callback.Message, reason!, description, cancellationToken)
                : callback.DeadLetterMessageAsync(callback.Message, properties, reason!, description, cancellationToken),
        };
        var session = _sessionCallback!;
        return operation switch
        {
            ServiceBusDisposition.Complete => session.CompleteMessageAsync(session.Message, cancellationToken),
            ServiceBusDisposition.Abandon => session.AbandonMessageAsync(session.Message, properties, cancellationToken),
            ServiceBusDisposition.Defer => session.DeferMessageAsync(session.Message, properties, cancellationToken),
            _ => properties is null ? session.DeadLetterMessageAsync(session.Message, reason!, description, cancellationToken)
                : session.DeadLetterMessageAsync(session.Message, properties as Dictionary<string, object> ?? new(properties), reason!, description, cancellationToken),
        };
    }
}
