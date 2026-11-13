using Relayer.Transports;
using System.Buffers;
using System.Diagnostics;
using Azure.Messaging.ServiceBus;

namespace Relayer.AzureServiceBus;

/// <summary>Reads a native processor delivery without enumerating its headers.</summary>
public readonly struct ServiceBusMessageBinding : IMessageBinding<ProcessMessageEventArgs>
{
    /// <inheritdoc />
    /// <remarks>
    /// Borrows a single indexed AMQP data section. Other body layouts use the SDK's Body conversion,
    /// which may allocate and combine sections; Relayer adds no body copy.
    /// </remarks>
    public static ReadOnlySequence<byte> GetBody(ProcessMessageEventArgs message) => GetBody(message.Message);

    internal static ReadOnlySequence<byte> GetBody(ServiceBusReceivedMessage message)
    {
        // The SDK's Body getter materializes a BinaryData on each access. Borrow a single AMQP data section directly.
        if (message.GetRawAmqpMessage().Body.TryGetData(out var data) &&
            data is IReadOnlyList<ReadOnlyMemory<byte>> { Count: 1 } sections)
            return new(sections[0]);
        // Preserve SDK handling of multi-section data and other native body forms.
        return new(message.Body.ToMemory());
    }

    /// <inheritdoc />
    public static int? GetDeliveryCount(ProcessMessageEventArgs message) => message.Message.DeliveryCount;

    /// <inheritdoc />
    /// <remarks>
    /// The logical content-type header uses the native AMQP ContentType when present, with application properties as fallback.
    /// Non-text application properties remain available through the native message.
    /// </remarks>
    public static bool TryGetHeader(ProcessMessageEventArgs message, string name, out WireString value) => TryGetHeader(message.Message, name, out value);

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
                case string text: value = new(text); return true;
                case byte[] bytes: value = new(bytes); return true;
                case ReadOnlyMemory<byte> memory: value = new(memory); return true;
                case ArraySegment<byte> segment: value = new(segment.AsMemory()); return true;
            }
        }
        value = default;
        return false;
    }

    /// <inheritdoc />
    public static void EnrichActivity(Activity activity, ProcessMessageEventArgs message) => EnrichActivity(activity, message.Message);

    internal static void EnrichActivity(Activity activity, ServiceBusReceivedMessage message)
    {
        if (message.DeliveryCount > 0) activity.SetTag("messaging.servicebus.message.delivery_count", message.DeliveryCount);
        activity.SetTag("messaging.servicebus.message.enqueued_time", message.EnqueuedTime.ToUnixTimeSeconds());
    }
}

/// <summary>Reads the unchanged native session processor delivery through the same lazy Service Bus binding.</summary>
public readonly struct ServiceBusSessionMessageBinding : IMessageBinding<ProcessSessionMessageEventArgs>
{
    /// <inheritdoc />
    public static ReadOnlySequence<byte> GetBody(ProcessSessionMessageEventArgs message) => ServiceBusMessageBinding.GetBody(message.Message);
    /// <inheritdoc />
    public static int? GetDeliveryCount(ProcessSessionMessageEventArgs message) => message.Message.DeliveryCount;
    /// <inheritdoc />
    public static bool TryGetHeader(ProcessSessionMessageEventArgs message, string name, out WireString value) => ServiceBusMessageBinding.TryGetHeader(message.Message, name, out value);
    /// <inheritdoc />
    public static void EnrichActivity(Activity activity, ProcessSessionMessageEventArgs message) => ServiceBusMessageBinding.EnrichActivity(activity, message.Message);
}
