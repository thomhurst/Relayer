using System.Buffers;
using Google.Cloud.PubSub.V1;
using Relayer.Transports;

namespace Relayer.GooglePubSub;

/// <summary>A borrowed native delivery and its streaming subscriber.</summary>
public readonly record struct GooglePubSubDelivery(PubsubMessage Message, SubscriberClient Subscriber)
{
    internal GooglePubSubReply? Reply { get; init; }
    /// <summary>The named transport's unchanged native API clients and topic publishers.</summary>
    public IGooglePubSubClients Clients { get; internal init; } = null!;
}

internal sealed class GooglePubSubReply
{
    internal SubscriberClient.Reply? Value;
}

/// <summary>Reads Pub/Sub data and attributes without copying or enumerating them.</summary>
public readonly struct GooglePubSubBinding : IMessageBinding<GooglePubSubDelivery>
{
    /// <inheritdoc />
    public static ReadOnlySequence<byte> GetBody(GooglePubSubDelivery message) => new(message.Message.Data.Memory);
    /// <inheritdoc />
    public static int? GetDeliveryCount(GooglePubSubDelivery message) => message.Message.GetDeliveryAttempt();
    /// <inheritdoc />
    public static bool TryGetHeader(GooglePubSubDelivery message, string name, out WireString value)
    {
        if (message.Message.Attributes.TryGetValue(name, out var text))
        {
            value = new(text);
            return true;
        }
        if (name == FailureHeaders.History) return GooglePubSubFailureHeaders.TryRead(message.Message, out value);
        value = default;
        return false;
    }
}

/// <summary>Typed native access for Pub/Sub handlers and sends.</summary>
public static class GooglePubSubMessageContextExtensions
{
    /// <summary>Returns the original message and subscriber. Do not retain either after the handler returns.</summary>
    public static GooglePubSubDelivery GooglePubSub(this MessageContext context) =>
        context.Features.GetRequired<NativeMessage<GooglePubSubDelivery>>().Message;

    /// <summary>Customizes the native Pub/Sub message immediately before publication.</summary>
    public static SendOptions GooglePubSub(this SendOptions options, Action<PubsubMessage> configure) => options.ConfigureNative(configure);
}
