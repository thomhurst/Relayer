using Relayer.Transports;
using Azure.Core.Amqp;
using Azure.Messaging.ServiceBus;

namespace Relayer.AzureServiceBus;

// Retain native message containers at the concurrency high-water mark, without a lock or per-send wrapper.
internal sealed class ServiceBusPool<T>(Func<T> create) where T : class
{
    private readonly SlotPool<T> _items = new();

    internal T Rent() => _items.TryRent() ?? create();
    internal void Return(T state) => _items.TryReturn(state);
}

internal sealed class ServiceBusSendState
{
    private readonly ReadOnlyMemory<byte>[] _data = new ReadOnlyMemory<byte>[1];
    private readonly AmqpMessageBody _body;
    internal ServiceBusMessage Message { get; }

    internal ServiceBusSendState()
    {
        _body = AmqpMessageBody.FromData(_data);
        Message = new(new AmqpAnnotatedMessage(_body));
    }

    internal void Prepare(OutgoingMessage outgoing)
    {
        _data[0] = outgoing.Body;
        if (outgoing.OrderingKey is { } key) Message.SessionId = key.ToString();
        if (outgoing.DeliverAt is { } scheduled) Message.ScheduledEnqueueTime = scheduled;
        outgoing.ApplyNative(Message);
        // The original envelope and creation context take precedence over a native callback.
        foreach (var header in outgoing.Headers.Span)
        {
            if (header.Key == "content-type") Message.ContentType = header.Value.ToString();
            else Message.ApplicationProperties[header.Key] = header.Value.ToString();
        }
    }

    internal void Clear()
    {
        var raw = Message.GetRawAmqpMessage();
        _data[0] = default;
        raw.Body = _body;
        raw.ApplicationProperties.Clear();
        raw.MessageAnnotations.Clear();
        raw.DeliveryAnnotations.Clear();
        raw.Footer.Clear();
        var header = raw.Header;
        header.DeliveryCount = null;
        header.Durable = null;
        header.FirstAcquirer = null;
        header.Priority = null;
        header.TimeToLive = null;
        var properties = raw.Properties;
        properties.AbsoluteExpiryTime = null;
        properties.ContentEncoding = null;
        properties.ContentType = null;
        properties.CorrelationId = null;
        properties.CreationTime = null;
        properties.GroupId = null;
        properties.GroupSequence = null;
        properties.MessageId = null;
        properties.ReplyTo = null;
        properties.ReplyToGroupId = null;
        properties.Subject = null;
        properties.To = null;
        properties.UserId = null;
    }
}
