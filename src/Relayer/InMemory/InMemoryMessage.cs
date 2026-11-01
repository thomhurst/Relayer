using System.Buffers;

namespace Relayer.InMemory;

/// <summary>Owned native message storage. Do not retain it beyond the receive callback or native configurator.</summary>
public sealed class InMemoryMessage
{
    internal InMemoryMessage(bool isWakeup = false) => IsWakeup = isWakeup;
    internal bool IsWakeup { get; }
    private byte[] _buffer = [];
    private KeyValuePair<string, WireString>[] _headers = [];
    private int _bodyLength;
    private int _headerCount;
    internal long Size;
    internal int References;
    internal int QueueAttempt;
    internal int Partition;
    internal long Position;

    /// <summary>Serialized native body.</summary>
    public ReadOnlyMemory<byte> Body => _buffer.AsMemory(0, _bodyLength);
    /// <summary>Native headers, including the unmodified CloudEvents envelope.</summary>
    public ReadOnlyMemory<KeyValuePair<string, WireString>> Headers => _headers.AsMemory(0, _headerCount);
    /// <summary>Native ordering key, copied from the borrowed send.</summary>
    public WireString? OrderingKey { get; private set; }
    /// <summary>Earliest native delivery time. Native configurators may override it within endpoint limits.</summary>
    public DateTimeOffset? DeliverAt { get; set; }

    internal void Copy(in OutgoingMessage outgoing)
    {
        QueueAttempt = 1;
        var length = outgoing.Body.Length;
        Size = outgoing.Body.Length;
        foreach (var header in outgoing.Headers.Span)
        {
            length = checked(length + header.Value.Utf8.Length);
            Size += System.Text.Encoding.UTF8.GetByteCount(header.Key) + ByteCount(header.Value);
        }
        if (outgoing.OrderingKey is { } sizeKey) Size += ByteCount(sizeKey);
        if (outgoing.OrderingKey is { } key) length = checked(length + key.Utf8.Length);
        if (_buffer.Length < length) _buffer = new byte[Math.Max(length, 256)];
        if (_headers.Length < outgoing.Headers.Length) _headers = new KeyValuePair<string, WireString>[Math.Max(outgoing.Headers.Length, 16)];
        outgoing.Body.Span.CopyTo(_buffer);
        _bodyLength = outgoing.Body.Length;
        _headerCount = outgoing.Headers.Length;
        var offset = _bodyLength;
        for (var i = 0; i < _headerCount; i++)
        {
            var header = outgoing.Headers.Span[i];
            _headers[i] = new(header.Key, Copy(header.Value, ref offset));
        }
        OrderingKey = outgoing.OrderingKey is { } orderingKey ? Copy(orderingKey, ref offset) : null;
        DeliverAt = outgoing.DeliverAt;
    }

    private static int ByteCount(WireString value) => value.Text is { } text ? System.Text.Encoding.UTF8.GetByteCount(text) : value.Utf8.Length;

    private WireString Copy(WireString value, ref int offset)
    {
        if (value.Text is { } text) return new(text);
        var memory = _buffer.AsMemory(offset, value.Utf8.Length);
        value.Utf8.CopyTo(memory);
        offset += memory.Length;
        return new(memory);
    }

    internal void Clear()
    {
        Array.Clear(_headers, 0, _headerCount);
        _headerCount = 0;
        _bodyLength = 0;
        OrderingKey = null;
        DeliverAt = null;
    }
}

/// <summary>Native receive view. Its storage and settlement handle are valid only during processing.</summary>
public readonly struct InMemoryDelivery
{
    private readonly InMemoryReceiver _receiver;
    internal InMemoryDelivery(InMemoryReceiver receiver, InMemoryMessage message)
    {
        _receiver = receiver;
        Message = message;
        Attempt = receiver.Attempt;
    }
    /// <summary>Native message storage.</summary>
    public InMemoryMessage Message { get; }
    /// <summary>Native partition number.</summary>
    public int Partition => Message.Partition;
    internal string PartitionId => _receiver.PartitionId;
    /// <summary>Native position, starting at zero. Logs commit after processing succeeds.</summary>
    public long Position => Message.Position;
    /// <summary>Delivery count, starting at one.</summary>
    public int Attempt { get; }
    /// <summary>Manually completes this delivery. Return SettledByHandler to prevent another settlement.</summary>
    public void Complete() => _receiver.Settle(Disposition.Complete);
    /// <summary>Manually requests native redelivery.</summary>
    public void Redeliver(TimeSpan delay) => _receiver.Settle(Disposition.Redeliver(delay));
    /// <summary>Manually dead-letters this delivery.</summary>
    public void DeadLetter(DeadLetterInfo info) => _receiver.Settle(Disposition.DeadLettered(info));
}

/// <summary>Zero-copy native receive binding with lazy header access.</summary>
public readonly struct InMemoryMessageBinding : IMessageBinding<InMemoryDelivery>
{
    /// <inheritdoc />
    public static string? GetPartitionId(InMemoryDelivery message) => message.PartitionId;
    /// <inheritdoc />
    public static ReadOnlySequence<byte> GetBody(InMemoryDelivery message) => new(message.Message.Body);
    /// <inheritdoc />
    public static int? GetDeliveryCount(InMemoryDelivery message) => message.Attempt;
    /// <inheritdoc />
    public static bool TryGetHeader(InMemoryDelivery message, string name, out WireString value)
    {
        foreach (var header in message.Message.Headers.Span)
        {
            if (header.Key == name) { value = header.Value; return true; }
        }
        value = default;
        return false;
    }
}

/// <summary>An independently owned snapshot of a native dead-lettered delivery.</summary>
public sealed class InMemoryDeadLetter
{
    internal InMemoryDeadLetter(InMemoryDelivery delivery, DeadLetterInfo error)
    {
        Body = delivery.Message.Body.ToArray();
        Headers = Snapshot(delivery.Message.Headers.Span);
        OrderingKey = delivery.Message.OrderingKey?.ToString();
        Partition = delivery.Partition;
        Position = delivery.Position;
        Attempt = delivery.Attempt;
        Error = new(error.Reason, error.Description, Snapshot(error.Headers.Span));
    }
    private static KeyValuePair<string, WireString>[] Snapshot(ReadOnlySpan<KeyValuePair<string, WireString>> source)
    {
        var result = new KeyValuePair<string, WireString>[source.Length];
        for (var i = 0; i < result.Length; i++) result[i] = new(source[i].Key, new WireString(source[i].Value.ToString()));
        return result;
    }
    /// <summary>Original serialized body.</summary>
    public ReadOnlyMemory<byte> Body { get; }
    /// <summary>Original envelope and application headers.</summary>
    public ReadOnlyMemory<KeyValuePair<string, WireString>> Headers { get; }
    /// <summary>Original ordering key.</summary>
    public string? OrderingKey { get; }
    /// <summary>Original partition.</summary>
    public int Partition { get; }
    /// <summary>Original position.</summary>
    public long Position { get; }
    /// <summary>Delivery count at settlement.</summary>
    public int Attempt { get; }
    /// <summary>Native failure metadata, owned by this snapshot.</summary>
    public DeadLetterInfo Error { get; }
}
