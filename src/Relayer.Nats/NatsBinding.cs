using System.Buffers;
using NATS.Client.Core;
using NATS.Client.JetStream;

namespace Relayer.Nats;

/// <summary>A borrowed native JetStream delivery and the durable consumer that received it.</summary>
public readonly record struct NatsDelivery(INatsJSMsg<ReadOnlyMemory<byte>> Message, INatsJSConsumer Consumer);

/// <summary>Reads native payload memory and lazy native headers without copying.</summary>
public readonly struct NatsBinding : IMessageBinding<NatsDelivery>
{
    /// <inheritdoc />
    public static ReadOnlySequence<byte> GetBody(NatsDelivery message) => new(message.Message.Data);
    /// <inheritdoc />
    public static int? GetDeliveryCount(NatsDelivery message) => message.Message.Metadata is { } metadata
        ? checked((int)metadata.NumDelivered) : null;
    /// <inheritdoc />
    public static bool TryGetHeader(NatsDelivery message, string name, out WireString value)
    {
        if (message.Message.Headers is { } headers && headers.TryGetLastValue(name, out var text))
        { value = new(text); return true; }
        value = default;
        return false;
    }
}

/// <summary>Typed native access for JetStream handlers and sends.</summary>
public static class NatsMessageContextExtensions
{
    /// <summary>Returns the untouched native delivery. Do not retain it after processing.</summary>
    public static NatsDelivery Nats(this MessageContext context) => context.Features.GetRequired<NativeMessage<NatsDelivery>>().Message;
    /// <summary>Customizes native headers immediately before confirmed JetStream publication.</summary>
    public static SendOptions Nats(this SendOptions options, Action<NatsHeaders> configure) => options.ConfigureNative(configure);
}
