using System.Buffers;
using Azure.Messaging.ServiceBus;

namespace Relayer.AzureServiceBus;

/// <summary>Reads a native processor delivery without enumerating its headers.</summary>
public readonly struct ServiceBusBinding : IMessageBinding<ProcessMessageEventArgs>
{
    /// <inheritdoc />
    /// <remarks>
    /// Borrows a single indexed AMQP data section. Other body layouts use the SDK's Body conversion,
    /// which may allocate and combine sections; Relayer adds no body copy.
    /// </remarks>
    public static ReadOnlySequence<byte> GetBody(ProcessMessageEventArgs message)
    {
        // The SDK's Body getter materializes a BinaryData on each access. Borrow a single AMQP data section directly.
        if (message.Message.GetRawAmqpMessage().Body.TryGetData(out var data) &&
            data is IReadOnlyList<ReadOnlyMemory<byte>> { Count: 1 } sections)
            return new(sections[0]);
        // Preserve SDK handling of multi-section data and other native body forms.
        return new(message.Message.Body.ToMemory());
    }

    /// <inheritdoc />
    public static int? GetDeliveryCount(ProcessMessageEventArgs message) => message.Message.DeliveryCount;

    /// <inheritdoc />
    /// <remarks>Non-text application properties remain available through the native message.</remarks>
    public static bool TryGetHeader(ProcessMessageEventArgs message, string name, out WireString value)
    {
        if (message.Message.GetRawAmqpMessage().ApplicationProperties.TryGetValue(name, out var native))
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
}
