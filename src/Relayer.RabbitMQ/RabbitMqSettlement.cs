using System.Runtime.CompilerServices;

namespace Relayer.RabbitMQ;

internal sealed class RabbitMqSettlement(MessageTracing tracing, MessagingEndpoint endpoint)
{
    private readonly MessageOperationTracing _ack = new(tracing, endpoint, MessagingOperation.Settle, "ack");
    private readonly MessageOperationTracing _nack = new(tracing, endpoint, MessagingOperation.Settle, "nack");

    [RuntimeAsyncMethodGeneration(false)]
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    internal async ValueTask SettleAsync(MessageContext context)
    {
        var native = context.Features.GetRequired<NativeMessage<RabbitMqDelivery>>();
        await (native.Message.Endpoint?.Settlement ?? this)
            .SettleAsync(native.Message, native.Disposition, context.CancellationToken).ConfigureAwait(false);
        native.Disposition = Disposition.SettledByHandler;
    }

    [RuntimeAsyncMethodGeneration(false)]
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    internal async ValueTask SettleAsync(RabbitMqDelivery delivery, Disposition disposition, CancellationToken token)
    {
        if (disposition.Kind == DispositionKind.SettledByHandler) return;
        if (disposition.Kind == DispositionKind.Redeliver && disposition.Delay != TimeSpan.Zero)
            throw new NotSupportedException("RabbitMQ nack can only requeue immediately. Use in-process retries or durable scheduled copies.");
        var ack = disposition.Kind == DispositionKind.Complete;
        if (!ack && disposition.Kind is not (DispositionKind.Redeliver or DispositionKind.DeadLetter))
            throw new InvalidOperationException("Unknown RabbitMQ settlement disposition.");
        using var activity = (ack ? _ack : _nack).Start();
        if (activity is { IsAllDataRequested: true }) RabbitMqMessageBinding.EnrichActivity(activity, delivery);
        try
        {
            if (ack) await delivery.Channel.BasicAckAsync(delivery.Message.DeliveryTag, false, token).ConfigureAwait(false);
            else await delivery.Channel.BasicNackAsync(delivery.Message.DeliveryTag, false,
                disposition.Kind == DispositionKind.Redeliver, token).ConfigureAwait(false);
        }
        catch { MessageOperationTracing.Fail(activity, "_OTHER"); throw; }
    }
}
