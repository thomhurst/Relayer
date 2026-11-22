using System.Buffers;
using System.Diagnostics;
using Azure.Messaging.ServiceBus;
using Relayer.Transports;

namespace Relayer.AzureServiceBus;

/// <summary>Reads a native processor delivery without enumerating its headers.</summary>
public readonly struct ServiceBusMessageBinding : IMessageBinding<ProcessMessageEventArgs>
{
    /// <inheritdoc />
    /// <remarks>
    /// Borrows a single indexed AMQP Data section. The SDK's Body conversion handles other
    /// Data bodies and may combine sections.
    /// </remarks>
    /// <exception cref="NotSupportedException">The underlying AMQP body is not a Data body.</exception>
    public static ReadOnlySequence<byte> GetBody(ProcessMessageEventArgs message)
    {
        return GetBody(message.Message);
    }

    internal static ReadOnlySequence<byte> GetBody(ServiceBusReceivedMessage message)
    {
        var body = message.GetRawAmqpMessage().Body;

        if (!body.TryGetData(out var data))
        {
            throw new UnsupportedNativeBodyException($"Relayer requires an AMQP Data body. This Service Bus message has an AMQP {body.BodyType} body.");
        }

        // The SDK's Body getter materializes a BinaryData. Borrow a single Data section directly.
        if (data is IReadOnlyList<ReadOnlyMemory<byte>> { Count: 1 } sections)
        {
            return new(sections[0]);
        }

        return new(message.Body.ToMemory());
    }

    /// <inheritdoc />
    public static int? GetDeliveryCount(ProcessMessageEventArgs message)
    {
        return message.Message.DeliveryCount;
    }

    /// <inheritdoc />
    /// <remarks>
    /// The logical content-type header uses the native AMQP ContentType when present, with application properties as fallback.
    /// Non-text application properties remain available through the native message.
    /// </remarks>
    public static bool TryGetHeader(ProcessMessageEventArgs message, string name, out WireString value)
    {
        return TryGetHeader(message.Message, name, out value);
    }

    internal static bool TryGetHeader(ServiceBusReceivedMessage message, string name, out WireString value)
    {
        if (name == "content-type" && message.ContentType is { } contentType)
        {
            value = new(contentType);
            return true;
        }

        if (message.GetRawAmqpMessage().ApplicationProperties.TryGetValue(name, out var native))
        {
            switch (native)
            {
                case string text:
                    value = new(text);
                    return true;
                case byte[] bytes:
                    value = new(bytes);
                    return true;
                case ReadOnlyMemory<byte> memory:
                    value = new(memory);
                    return true;
                case ArraySegment<byte> segment:
                    value = new(segment.AsMemory());
                    return true;
            }
        }

        value = default;
        return false;
    }

    /// <inheritdoc />
    public static void EnrichActivity(Activity activity, ProcessMessageEventArgs message)
    {
        EnrichActivity(activity, message.Message);
    }

    internal static void EnrichActivity(Activity activity, ServiceBusReceivedMessage message)
    {
        if (message.DeliveryCount > 0)
        {
            activity.SetTag("messaging.servicebus.message.delivery_count", message.DeliveryCount);
        }

        activity.SetTag("messaging.servicebus.message.enqueued_time", message.EnqueuedTime.ToUnixTimeSeconds());
    }
}

/// <summary>Reads the unchanged native session processor delivery through the same lazy Service Bus binding.</summary>
public readonly struct ServiceBusSessionMessageBinding : IMessageBinding<ProcessSessionMessageEventArgs>
{
    /// <inheritdoc />
    public static ReadOnlySequence<byte> GetBody(ProcessSessionMessageEventArgs message)
    {
        return ServiceBusMessageBinding.GetBody(message.Message);
    }

    /// <inheritdoc />
    public static int? GetDeliveryCount(ProcessSessionMessageEventArgs message)
    {
        return message.Message.DeliveryCount;
    }

    /// <inheritdoc />
    public static bool TryGetHeader(ProcessSessionMessageEventArgs message, string name, out WireString value)
    {
        return ServiceBusMessageBinding.TryGetHeader(message.Message, name, out value);
    }

    /// <inheritdoc />
    public static void EnrichActivity(Activity activity, ProcessSessionMessageEventArgs message)
    {
        ServiceBusMessageBinding.EnrichActivity(activity, message.Message);
    }
}
