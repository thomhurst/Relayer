using System.Buffers;
using System.Diagnostics;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace Relayer.RabbitMQ;

/// <summary>A borrowed native delivery and its channel. Valid until the receive callback completes.</summary>
public readonly record struct RabbitMqDelivery(IChannel Channel, BasicDeliverEventArgs Message)
{
    internal RabbitMqReceiveEndpoint? Endpoint { get; init; }
}

/// <summary>Reads RabbitMQ bodies and binary CloudEvents headers without copying them.</summary>
public readonly struct RabbitMqMessageBinding : IMessageBinding<RabbitMqDelivery>
{
    /// <inheritdoc />
    public static ReadOnlySequence<byte> GetBody(RabbitMqDelivery delivery) => new(delivery.Message.Body);
    /// <inheritdoc />
    public static int? GetDeliveryCount(RabbitMqDelivery delivery) => null; // Redelivered is a flag, not an attempt count.
    /// <inheritdoc />
    public static bool TryGetHeader(RabbitMqDelivery delivery, string name, out WireString value)
    {
        var properties = delivery.Message.BasicProperties;
        if (name == "content-type" && properties.ContentType is { } contentType)
        { value = new(contentType); return true; }
        if (properties.Headers?.TryGetValue(name, out var native) == true)
        {
            switch (native)
            {
                case string text: value = new(text); return true;
                case byte[] bytes: value = new(bytes); return true;
                case ReadOnlyMemory<byte> memory: value = new(memory); return true;
            }
        }
        value = default;
        return false;
    }
    /// <inheritdoc />
    public static MessageOperationTracing? GetProcessTracing(RabbitMqDelivery delivery) => delivery.Endpoint?.Process;
    /// <inheritdoc />
    public static void EnrichActivity(Activity activity, RabbitMqDelivery delivery)
    {
        if (delivery.Message.RoutingKey.Length != 0)
            activity.SetTag("messaging.rabbitmq.destination.routing_key", delivery.Message.RoutingKey);
        activity.SetTag("messaging.rabbitmq.message.delivery_tag", delivery.Message.DeliveryTag);
    }
}

/// <summary>Native access to the current RabbitMQ delivery.</summary>
public static class RabbitMqMessageContextExtensions
{
    /// <summary>Returns the borrowed native channel and delivery. For direct native settlement, mark NativeMessage&lt;RabbitMqDelivery&gt;.Disposition as SettledByHandler after success.</summary>
    public static RabbitMqDelivery RabbitMQ(this MessageContext context) => context.Features.GetRequired<NativeMessage<RabbitMqDelivery>>().Message;
}
