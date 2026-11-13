using Relayer.Transports;
using System.Text;
using Amazon.SQS.Model;

namespace Relayer.AmazonSqs;

// Native requests borrow these objects until the SDK task completes. Each concurrent operation rents its own state.
internal sealed class SqsSendState
{
    private static readonly UTF8Encoding Utf8 = new(false, true);
    private readonly Dictionary<string, MessageAttributeValue> _attributes = new(10, StringComparer.Ordinal);
    private readonly MessageAttributeValue[] _values = new MessageAttributeValue[10];
    internal SendMessageRequest Request { get; } = new();
    internal SendMessageBatchRequestEntry Entry { get; }

    internal SqsSendState(string id)
    {
        Entry = new() { Id = id };
        for (var i = 0; i < _values.Length; i++) _values[i] = new() { DataType = "String" };
    }

    internal int Prepare(OutgoingMessage outgoing, string queueUrl, bool fifo, string identityHeader = "ce_id")
    {
        Request.QueueUrl = queueUrl;
        // SQS has a native text body. Reject invalid UTF-8 rather than silently replacing serializer bytes.
        Request.MessageBody = Utf8.GetString(outgoing.Body.Span);
        var groupId = outgoing.OrderingKey?.ToString();
        if (groupId is not null) Request.MessageGroupId = groupId;
        Request.MessageAttributes = _attributes;
        outgoing.ApplyNative(Request);
        // Keep caller-supplied attribute dictionaries caller-owned, while retaining reusable transport storage.
        if (!ReferenceEquals(Request.MessageAttributes, _attributes))
        {
            if (Request.MessageAttributes is { } native)
                foreach (var attribute in native) _attributes.Add(attribute.Key, attribute.Value);
            Request.MessageAttributes = _attributes;
        }
        var index = 0;
        foreach (var header in outgoing.Headers.Span)
        {
            if (index == _values.Length)
                throw new InvalidOperationException("SQS supports at most 10 message attributes, including CloudEvents and tracing headers.");
            var value = _values[index++];
            value.StringValue = header.Value.ToString();
            _attributes[header.Key] = value;
            if (fifo && header.Key == identityHeader) Request.MessageDeduplicationId = value.StringValue;
        }
        if (_attributes.Count > 10)
            throw new InvalidOperationException("SQS supports at most 10 message attributes, including CloudEvents and tracing headers.");
        Request.QueueUrl = queueUrl;
        if (groupId is not null) Request.MessageGroupId = groupId;
        if (outgoing.DeliverAt is { } deliverAt)
        {
            var delay = deliverAt - DateTimeOffset.UtcNow;
            if (delay > TimeSpan.Zero)
            {
                if (fifo || delay > TimeSpan.FromMinutes(15))
                    throw new NotSupportedException("SQS per-message timers require a standard queue and a delay at most 15 minutes. Use the durable outbox for longer delays or FIFO scheduling.");
                Request.DelaySeconds = (int)Math.Ceiling(delay.TotalSeconds);
            }
            else Request.DelaySeconds = fifo ? null : 0;
        }
        if (fifo && (string.IsNullOrWhiteSpace(Request.MessageGroupId) || Request.DelaySeconds is not null))
            throw new InvalidOperationException("SQS FIFO sends require MessageGroupId and cannot specify per-message DelaySeconds.");
        if (Request.DelaySeconds is < 0 or > 900)
            throw new ArgumentOutOfRangeException(nameof(Request.DelaySeconds), "SQS DelaySeconds must be between 0 and 900.");
        var size = Utf8.GetByteCount(Request.MessageBody);
        foreach (var attribute in _attributes)
        {
            var value = attribute.Value;
            size = checked(size + Utf8.GetByteCount(attribute.Key) + Utf8.GetByteCount(value.DataType));
            if (value.StringValue is { } text) size = checked(size + Utf8.GetByteCount(text));
            if (value.BinaryValue is { } binary) size = checked(size + (int)binary.Length);
        }
        return size;
    }

    internal void CopyToEntry()
    {
        Entry.MessageBody = Request.MessageBody;
        Entry.MessageAttributes = Request.MessageAttributes;
        Entry.MessageSystemAttributes = Request.MessageSystemAttributes;
        Entry.DelaySeconds = Request.DelaySeconds;
        Entry.MessageGroupId = Request.MessageGroupId;
        Entry.MessageDeduplicationId = Request.MessageDeduplicationId;
    }

    internal void Clear()
    {
        _attributes.Clear();
        foreach (var value in _values) value.StringValue = null;
        Request.QueueUrl = null;
        Request.MessageBody = null;
        Request.MessageAttributes = null;
        Request.MessageSystemAttributes = null;
        Request.MessageGroupId = null;
        Request.MessageDeduplicationId = null;
        Request.DelaySeconds = null;
        Entry.MessageBody = null;
        Entry.MessageAttributes = null;
        Entry.MessageSystemAttributes = null;
        Entry.MessageGroupId = null;
        Entry.MessageDeduplicationId = null;
        Entry.DelaySeconds = null;
    }
}

internal sealed class SqsOperationState
{
    internal readonly SqsSendState[] Messages = new SqsSendState[10];
    internal readonly string[] Ids = ["0", "1", "2", "3", "4", "5", "6", "7", "8", "9"];
    internal readonly SendMessageBatchRequest Batch = new() { Entries = new(10) };
    internal int Used;
    internal SqsOperationState()
    {
        for (var i = 0; i < Messages.Length; i++) Messages[i] = new(i.ToString(System.Globalization.CultureInfo.InvariantCulture));
    }

    internal void Clear()
    {
        for (var i = 0; i < Used; i++) Messages[i].Clear();
        Used = 0;
        Batch.Entries.Clear();
        Batch.QueueUrl = null;
    }
}

// Same lock-free high-water-mark strategy as the Service Bus adapter; no shared mutable static state.
internal sealed class SqsOperationPool
{
    private readonly SlotPool<SqsOperationState> _states = new(pageSize: 32);
    internal SqsOperationState Rent() => _states.TryRent() ?? new();

    internal void Return(SqsOperationState state)
    {
        state.Clear();
        _states.TryReturn(state);
    }
}
