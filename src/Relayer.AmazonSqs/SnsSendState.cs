using Relayer.Transports;
using System.Text;
using Amazon.SimpleNotificationService.Model;

namespace Relayer.AmazonSqs;

// Native requests borrow these objects until the SDK task completes. Each concurrent operation rents its own state.
internal sealed class SnsSendState
{
    private static readonly UTF8Encoding Utf8 = new(false, true);
    private readonly Dictionary<string, MessageAttributeValue> _attributes = new(10, StringComparer.Ordinal);
    private readonly MessageAttributeValue[] _values = new MessageAttributeValue[10];
    internal PublishRequest Request { get; } = new();
    internal PublishBatchRequestEntry Entry { get; }

    internal SnsSendState(string id)
    {
        Entry = new() { Id = id };
        for (var i = 0; i < _values.Length; i++) _values[i] = new() { DataType = "String" };
    }

    internal int Prepare(OutgoingMessage outgoing, string topicArn, bool fifo, string identityHeader = "ce_id")
    {
        Request.TopicArn = topicArn;
        // SNS has a native text body. Reject invalid UTF-8 rather than silently replacing serializer bytes.
        Request.Message = Utf8.GetString(outgoing.Body.Span);
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
                throw new InvalidOperationException("SNS supports at most 10 message attributes, including CloudEvents and tracing headers.");
            var value = _values[index++];
            value.StringValue = header.Value.ToString();
            _attributes[header.Key] = value;
            if (fifo && header.Key == identityHeader) Request.MessageDeduplicationId = value.StringValue;
        }
        if (_attributes.Count > 10)
            throw new InvalidOperationException("SNS supports at most 10 message attributes, including CloudEvents and tracing headers.");
        Request.TopicArn = topicArn;
        if (groupId is not null) Request.MessageGroupId = groupId;
        if (outgoing.DeliverAt > DateTimeOffset.UtcNow)
            throw new NotSupportedException("SNS has no native delivery timer. Use the durable outbox for scheduling.");
        if (fifo && string.IsNullOrWhiteSpace(Request.MessageGroupId))
            throw new InvalidOperationException("SNS FIFO sends require MessageGroupId.");
        if (Request.TargetArn is not null || Request.PhoneNumber is not null || Request.MessageStructure is not null)
            throw new InvalidOperationException("SNS topic routes require a plain message; TargetArn, PhoneNumber and MessageStructure are unsupported.");
        var size = Utf8.GetByteCount(Request.Message);
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
        Entry.Message = Request.Message;
        Entry.Subject = Request.Subject;
        Entry.MessageAttributes = Request.MessageAttributes;
        Entry.MessageGroupId = Request.MessageGroupId;
        Entry.MessageDeduplicationId = Request.MessageDeduplicationId;
    }

    internal void Clear()
    {
        _attributes.Clear();
        foreach (var value in _values) value.StringValue = null;
        Request.TopicArn = null;
        Request.Message = null;
        Request.TargetArn = null;
        Request.PhoneNumber = null;
        Request.Subject = null;
        Request.MessageStructure = null;
        Request.MessageAttributes = null;
        Request.MessageGroupId = null;
        Request.MessageDeduplicationId = null;
        Entry.Message = null;
        Entry.Subject = null;
        Entry.MessageAttributes = null;
        Entry.MessageGroupId = null;
        Entry.MessageDeduplicationId = null;
    }
}

internal sealed class SnsOperationState
{
    internal readonly SnsSendState[] Messages = new SnsSendState[10];
    internal readonly string[] Ids = ["0", "1", "2", "3", "4", "5", "6", "7", "8", "9"];
    internal readonly PublishBatchRequest Batch = new() { PublishBatchRequestEntries = new(10) };
    internal int Used;
    internal SnsOperationState()
    {
        for (var i = 0; i < Messages.Length; i++) Messages[i] = new(i.ToString(System.Globalization.CultureInfo.InvariantCulture));
    }

    internal void Clear()
    {
        for (var i = 0; i < Used; i++) Messages[i].Clear();
        Used = 0;
        Batch.PublishBatchRequestEntries.Clear();
        Batch.TopicArn = null;
    }
}

// Same lock-free high-water-mark strategy as the Service Bus adapter; no shared mutable static state.
internal sealed class SnsOperationPool
{
    private readonly Page _first = new();
    internal SnsOperationState Rent()
    {
        for (Page? page = _first; page is not null; page = Volatile.Read(ref page.Next))
            for (var i = 0; i < page.Slots.Length; i++)
                if (Interlocked.Exchange(ref page.Slots[i], null) is { } state) return state;
        return new();
    }

    internal void Return(SnsOperationState state)
    {
        state.Clear();
        var page = _first;
        while (true)
        {
            for (var i = 0; i < page.Slots.Length; i++)
                if (Interlocked.CompareExchange(ref page.Slots[i], state, null) is null) return;
            page = Volatile.Read(ref page.Next) ?? Interlocked.CompareExchange(ref page.Next, new Page(), null) ?? page.Next!;
        }
    }

    private sealed class Page
    {
        internal readonly SnsOperationState?[] Slots = new SnsOperationState?[32];
        internal Page? Next;
    }
}
