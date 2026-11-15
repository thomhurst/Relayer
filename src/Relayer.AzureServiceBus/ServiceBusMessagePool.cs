using Relayer.Transports;
using Azure.Core.Amqp;
using Azure.Messaging.ServiceBus;

namespace Relayer.AzureServiceBus;

// Retain at most 256 idle containers; callers clear borrowed state before returning it.
internal sealed class ServiceBusPool<T>(Func<T> create) where T : class
{
    private readonly SlotPool<T> _items = new(capacity: 256);

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

    internal void Prepare(OutgoingMessage outgoing, string[]? subjectHeaders = null, string[]? idHeaders = null,
        ServiceBusRouteCallback[]? routeCallbacks = null)
    {
        _data[0] = outgoing.Body;
        if (outgoing.OrderingKey is { } key) Message.SessionId = key.ToString();
        if (outgoing.DeliverAt is { } scheduled) Message.ScheduledEnqueueTime = scheduled;
        // Route callbacks run first, so a per-send callback can override them.
        if (routeCallbacks is { Length: > 0 }) ApplyRouteCallback(outgoing, routeCallbacks);
        outgoing.ApplyNative(Message);
        // The original envelope and creation context take precedence over a native callback, except Subject and MessageId:
        // a callback that sets either keeps it. Otherwise the envelope id becomes the native MessageId so broker duplicate
        // detection drops a redispatched copy.
        foreach (var header in outgoing.Headers.Span)
        {
            if (header.Key == "content-type") Message.ContentType = header.Value.ToString();
            else
            {
                var value = header.Value.ToString();
                Message.ApplicationProperties[header.Key] = value;
                // A native callback's Subject wins; otherwise the message type drives subscription filters.
                if (Message.Subject is null && subjectHeaders is not null && Array.IndexOf(subjectHeaders, header.Key) >= 0)
                    Message.Subject = value;
                if (Message.MessageId is null && idHeaders is not null && Array.IndexOf(idHeaders, header.Key) >= 0)
                    Message.MessageId = value;
            }
        }
    }

    private void ApplyRouteCallback(OutgoingMessage outgoing, ServiceBusRouteCallback[] routeCallbacks)
    {
        foreach (var header in outgoing.Headers.Span)
        {
            foreach (var route in routeCallbacks)
            {
                if (header.Key != route.TypeHeader || !route.Matches(header.Value)) continue;
                route.Configure(outgoing, Message);
                return;
            }
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

/// <summary>A route's native callback and the CloudEvents type header that identifies the route's sends.</summary>
internal sealed class ServiceBusRouteCallback(string typeHeader, string type, Action<OutgoingMessage, ServiceBusMessage> configure)
{
    private readonly byte[] _typeUtf8 = System.Text.Encoding.UTF8.GetBytes(type);
    internal string TypeHeader { get; } = typeHeader;
    internal Action<OutgoingMessage, ServiceBusMessage> Configure { get; } = configure;

    internal bool Matches(WireString value) => value.Text is { } text
        ? string.Equals(text, type, StringComparison.Ordinal)
        : value.Utf8.Span.SequenceEqual(_typeUtf8);
}
