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
        return new(context.Features.GetRequired<NativeMessage<ProcessMessageEventArgs>>(), context.CancellationToken);
    }
}

/// <summary>A borrowed view of the original Service Bus callback and its native settlement operations.</summary>
/// <remarks>
/// Disable the processor's AutoCompleteMessages and make the receive loop honor SettledByHandler.
/// These methods use the context's cancellation token and mark settlement only after native success.
/// Do not retain this view, its callback, or its message after processing. Direct callback settlement requires
/// setting NativeMessage&lt;ProcessMessageEventArgs&gt;.Disposition to SettledByHandler after success.
/// </remarks>
public readonly struct ServiceBusMessageContext
{
    private readonly NativeMessage<ProcessMessageEventArgs> _native;
    private readonly CancellationToken _cancellationToken;

    internal ServiceBusMessageContext(NativeMessage<ProcessMessageEventArgs> native, CancellationToken cancellationToken)
    {
        _native = native;
        _cancellationToken = cancellationToken;
    }

    /// <summary>The unchanged native processor callback.</summary>
    public ProcessMessageEventArgs Callback => _native.Message;
    /// <summary>The unchanged native received message, including lock, session and sequence metadata.</summary>
    public ServiceBusReceivedMessage Message => Callback.Message;

    /// <summary>Completes the message natively before marking it settled.</summary>
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    public async ValueTask CompleteAsync()
    {
        EnsureUnsettled();
        await Callback.CompleteMessageAsync(Message, _cancellationToken).ConfigureAwait(false);
        _native.Disposition = Disposition.SettledByHandler;
    }

    /// <summary>Abandons the message with the SDK's unchanged property modifications.</summary>
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    public async ValueTask AbandonAsync(IDictionary<string, object>? propertiesToModify = null)
    {
        EnsureUnsettled();
        await Callback.AbandonMessageAsync(Message, propertiesToModify, _cancellationToken).ConfigureAwait(false);
        _native.Disposition = Disposition.SettledByHandler;
    }

    /// <summary>Defers the message natively. Retrieve it later by its native sequence number.</summary>
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    public async ValueTask DeferAsync(IDictionary<string, object>? propertiesToModify = null)
    {
        EnsureUnsettled();
        await Callback.DeferMessageAsync(Message, propertiesToModify, _cancellationToken).ConfigureAwait(false);
        _native.Disposition = Disposition.SettledByHandler;
    }

    /// <summary>Dead-letters the message with the SDK's unchanged reason, description and property modifications.</summary>
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    public async ValueTask DeadLetterAsync(string reason, string? description = null,
        IDictionary<string, object>? propertiesToModify = null)
    {
        EnsureUnsettled();
        await Callback.DeadLetterMessageAsync(Message, propertiesToModify, reason, description, _cancellationToken).ConfigureAwait(false);
        _native.Disposition = Disposition.SettledByHandler;
    }

    private void EnsureUnsettled()
    {
        if (_native.Disposition.Kind == DispositionKind.SettledByHandler)
            throw new InvalidOperationException("This Service Bus delivery has already been settled by the handler.");
    }
}
