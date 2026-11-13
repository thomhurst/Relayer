using Relayer.Transports;
using System.Buffers;
using System.Buffers.Text;
using System.Diagnostics;
using System.Globalization;
using System.Text;
using Amazon.SQS;
using Amazon.SQS.Model;

namespace Relayer.AmazonSqs;

/// <summary>Receiver-owned SQS storage for transport authors. Applications use the borrowed SqsMessageContext view.</summary>
public sealed class SqsDelivery : IDisposable
{
    private byte[] _buffer = [];
    private readonly byte[] _redeliveries = new byte[11];
    private int _redeliveryLength;
    private string? _invalidRedeliveries;
    internal readonly DeleteMessageRequest Delete = new();
    internal readonly ChangeMessageVisibilityRequest Visibility = new();
    internal long ReceivedAt { get; private set; }
    internal TimeProvider Time { get; }
    internal int DeliveryCount { get; private set; }
    internal ReadOnlyMemory<byte> Body { get; private set; }
    /// <summary>The unchanged native message. Do not retain it after handling.</summary>
    public Message Message { get; private set; } = null!;
    /// <summary>The native client, owned by the transport or caller.</summary>
    public IAmazonSQS Client { get; }
    /// <summary>The resolved native queue URL.</summary>
    public string QueueUrl { get; }
    /// <summary>The request identifier of the native receive.</summary>
    public string? RequestId { get; private set; }

    internal SqsDelivery(IAmazonSQS client, string url, TimeProvider? time = null)
    {
        Client = client;
        QueueUrl = url;
        Time = time ?? TimeProvider.System;
        Delete.QueueUrl = Visibility.QueueUrl = url;
    }

    internal void Prepare(Message message, long receivedAt, string? requestId)
    {
        Message = message;
        ReceivedAt = receivedAt;
        RequestId = requestId;
        Delete.ReceiptHandle = Visibility.ReceiptHandle = message.ReceiptHandle;
        var body = message.Body ?? string.Empty;
        var needed = Encoding.UTF8.GetByteCount(body);
        if (_buffer.Length < needed)
        {
            if (_buffer.Length != 0) ArrayPool<byte>.Shared.Return(_buffer);
            _buffer = ArrayPool<byte>.Shared.Rent(needed);
        }
        Body = _buffer.AsMemory(0, Encoding.UTF8.GetBytes(body, _buffer));
        DeliveryCount = 1;
        _invalidRedeliveries = null;
        if (message.Attributes?.TryGetValue("ApproximateReceiveCount", out var count) == true)
        {
            if (int.TryParse(count, NumberStyles.None, CultureInfo.InvariantCulture, out var parsed) && parsed > 0)
                DeliveryCount = parsed;
            else _invalidRedeliveries = "invalid native receive count";
        }
        var prior = 0;
        if (message.MessageAttributes?.TryGetValue(FailureHeaders.Redeliveries, out var attribute) == true &&
            (!int.TryParse(attribute.StringValue, NumberStyles.None, CultureInfo.InvariantCulture, out prior) ||
             prior > int.MaxValue - DeliveryCount))
            _invalidRedeliveries = attribute.StringValue ?? "invalid";
        // Let the core classify malformed failure metadata and copy it to the poison path.
        if (_invalidRedeliveries is null)
            Utf8Formatter.TryFormat(prior + DeliveryCount - 1, _redeliveries, out _redeliveryLength);
    }

    internal WireString Redeliveries => _invalidRedeliveries is { } invalid ? new(invalid) : new(_redeliveries.AsMemory(0, _redeliveryLength));
    internal void Clear()
    {
        Message = null!;
        RequestId = null;
        _invalidRedeliveries = null;
        Body = default;
        Delete.ReceiptHandle = Visibility.ReceiptHandle = null;
    }
    /// <summary>Releases pooled body storage. Owned by the receiver; handlers must not dispose deliveries.</summary>
    public void Dispose()
    {
        Clear();
        if (_buffer.Length != 0) ArrayPool<byte>.Shared.Return(_buffer);
        _buffer = [];
    }
}

/// <summary>Reads a pooled SQS delivery without copying attributes or materializing headers.</summary>
public readonly struct SqsMessageBinding : IMessageBinding<SqsDelivery>
{
    /// <inheritdoc />
    public static ReadOnlySequence<byte> GetBody(SqsDelivery message) => new(message.Body);
    /// <inheritdoc />
    public static int? GetDeliveryCount(SqsDelivery message) => message.DeliveryCount;
    /// <inheritdoc />
    public static bool TryGetHeader(SqsDelivery message, string name, out WireString value)
    {
        if (name == FailureHeaders.Redeliveries) { value = message.Redeliveries; return true; }
        if (message.Message.MessageAttributes?.TryGetValue(name, out var attribute) == true && attribute.StringValue is { } text)
        { value = new(text); return true; }
        value = default;
        return false;
    }
    /// <inheritdoc />
    public static void EnrichActivity(Activity activity, SqsDelivery message)
    {
        activity.SetTag("aws.sqs.queue.url", message.QueueUrl);
        if (message.RequestId is { } id) activity.SetTag("aws.request_id", id);
    }
}

/// <summary>Access to the unchanged native delivery and client.</summary>
public static class AmazonSqsMessageContextExtensions
{
    /// <summary>Returns borrowed SQS native features for this delivery.</summary>
    public static SqsMessageContext AmazonSqs(this MessageContext context) =>
        new(context.Features.GetRequired<NativeMessage<SqsDelivery>>().Message);
}

/// <summary>A borrowed native SQS view, valid only while this message is being processed.</summary>
/// <remarks>Do not retain this view or its native objects after processing. The receiver owns delivery storage and the native client lifetime.</remarks>
public readonly struct SqsMessageContext
{
    private readonly SqsDelivery _delivery;

    internal SqsMessageContext(SqsDelivery delivery) => _delivery = delivery;

    /// <summary>The unchanged native message.</summary>
    public Message Message => _delivery.Message;
    /// <summary>The native client, owned by the transport or caller.</summary>
    public IAmazonSQS Client => _delivery.Client;
    /// <summary>The resolved native queue URL.</summary>
    public string QueueUrl => _delivery.QueueUrl;
    /// <summary>The request identifier of the native receive.</summary>
    public string? RequestId => _delivery.RequestId;
}
