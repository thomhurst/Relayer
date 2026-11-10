using System.Runtime.CompilerServices;
using Azure.Messaging.ServiceBus;
using Relayer.Transports;

namespace Relayer.AzureServiceBus;

/// <summary>Native Service Bus access for a processor delivery.</summary>
public static class ServiceBusMessageContextExtensions
{
    /// <summary>Returns a borrowed native view, valid only while this message is being processed.</summary>
    public static ServiceBusMessageContext ServiceBus(this MessageContext context)
    {
        ArgumentNullException.ThrowIfNull(context);
        return new(context);
    }
}

/// <summary>A borrowed view of the original Service Bus callback and its native settlement operations.</summary>
/// <remarks>
/// Disable the processor's AutoCompleteMessages and make the receive loop honor SettledByHandler.
/// These methods use the context's cancellation token and mark settlement only after native success.
/// Do not retain this view, its callback, or its message after processing. Direct callback settlement requires
/// setting the corresponding NativeMessage callback feature's Disposition to SettledByHandler after success.
/// </remarks>
public readonly struct ServiceBusMessageContext
{
    private readonly ServiceBusDelivery _delivery;
    private readonly ServiceBusSettlement? _settlement;
    private readonly ISettlementFeature? _request;
    private readonly CancellationToken _cancellationToken;

    internal ServiceBusMessageContext(MessageContext context)
    {
        _delivery = ServiceBusDelivery.From(context);
        _settlement = context.Features.Get<ServiceBusSettlement>();
        _request = context.Features.Get<ISettlementFeature>();
        _cancellationToken = context.CancellationToken;
    }

    /// <summary>The unchanged native non-session callback. For sessions, use SessionCallback.</summary>
    public ProcessMessageEventArgs Callback => _delivery.Callback;
    /// <summary>The unchanged native session callback, including session state and lock operations.</summary>
    public ProcessSessionMessageEventArgs SessionCallback => _delivery.SessionCallback;
    /// <summary>The unchanged native received message, including lock, session and sequence metadata.</summary>
    public ServiceBusReceivedMessage Message => _delivery.Message;

    /// <summary>Completes the message natively before marking it settled.</summary>
    [RuntimeAsyncMethodGeneration(false)]
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    public async ValueTask CompleteAsync()
    {
        EnsureUnsettled();
        await ExecuteAsync(ServiceBusDisposition.Complete).ConfigureAwait(false);
        _delivery.MarkSettled();
    }

    /// <summary>Abandons the message with the SDK's unchanged property modifications.</summary>
    [RuntimeAsyncMethodGeneration(false)]
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    public async ValueTask AbandonAsync(IDictionary<string, object>? propertiesToModify = null)
    {
        EnsureUnsettled();
        await ExecuteAsync(ServiceBusDisposition.Abandon, propertiesToModify).ConfigureAwait(false);
        _delivery.MarkSettled();
    }

    /// <summary>Defers the message natively. Retrieve it later by its native sequence number.</summary>
    [RuntimeAsyncMethodGeneration(false)]
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    public async ValueTask DeferAsync(IDictionary<string, object>? propertiesToModify = null)
    {
        EnsureUnsettled();
        await ExecuteAsync(ServiceBusDisposition.Defer, propertiesToModify).ConfigureAwait(false);
        _delivery.MarkSettled();
    }

    /// <summary>Dead-letters the message with the SDK's unchanged reason, description and property modifications.</summary>
    [RuntimeAsyncMethodGeneration(false)]
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    public async ValueTask DeadLetterAsync(string reason, string? description = null,
        IDictionary<string, object>? propertiesToModify = null)
    {
        EnsureUnsettled();
        await ExecuteAsync(ServiceBusDisposition.DeadLetter, propertiesToModify, reason, description).ConfigureAwait(false);
        _delivery.MarkSettled();
    }

    private ValueTask ExecuteAsync(ServiceBusDisposition operation, IDictionary<string, object>? properties = null,
        string? reason = null, string? description = null) => _settlement is { } settlement
        ? settlement.ExecuteAsync(_delivery, operation, properties, reason, description, _cancellationToken)
        : new(_delivery.ExecuteAsync(operation, properties, reason, description, _cancellationToken));

    private void EnsureUnsettled()
    {
        if (_request?.RequestedDisposition is not null)
            throw new InvalidOperationException("This delivery already has a settlement request. Do not combine native and portable settlement.");
        if (_delivery.Disposition.Kind == DispositionKind.SettledByHandler)
            throw new InvalidOperationException("This Service Bus delivery has already been settled by the handler.");
    }
}
