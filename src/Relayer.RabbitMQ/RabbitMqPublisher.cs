using Relayer.Transports;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using RabbitMQ.Client;

namespace Relayer.RabbitMQ;

internal sealed class RabbitMqPublisher(IChannel channel)
{
    internal IChannel Channel { get; set; } = channel;
    internal readonly BasicProperties Properties = new();
    private readonly List<RabbitMqBatchSlot> _batchSlots = [];
    private ConfiguredValueTaskAwaitable.ConfiguredValueTaskAwaiter[] _batchSends = [];

    internal ConfiguredValueTaskAwaitable.ConfiguredValueTaskAwaiter[] BatchSends(int count)
    {
        if (_batchSends.Length < count) Array.Resize(ref _batchSends, count);
        return _batchSends;
    }

    internal RabbitMqBatchSlot BatchSlot(int index)
    {
        if (index == _batchSlots.Count) _batchSlots.Add(new(Channel));
        var slot = _batchSlots[index];
        slot.Publisher.Channel = Channel;
        return slot;
    }

    private readonly RabbitMqHeaders _headers = new();
    internal const int CacheCapacity = 256;
    private readonly Dictionary<(int Index, int Length), byte[]> _buffers = new(CacheCapacity);
    private readonly (int Index, int Length)[] _bufferOrder = new (int, int)[CacheCapacity];
    private int _nextBuffer;
    private int _nextContentType;
    private readonly List<(byte[] Utf8, string Text)> _contentTypes = new(CacheCapacity);
    // The native field-table API requires a boxed integer. This publisher exclusively owns and reuses its box.
    private readonly object _delay = 0L;

    internal void Prepare(OutgoingMessage message, long delay)
    {
        Properties.Persistent = true;
        Properties.Headers = _headers;
        message.ApplyNative(Properties);
        if (Properties.Headers is { } nativeHeaders && !ReferenceEquals(nativeHeaders, _headers))
            foreach (var header in nativeHeaders) _headers[header.Key] = header.Value;
        Properties.Headers = _headers;
        var index = 0;
        foreach (var header in message.Headers.Span)
        {
            if (header.Key == "content-type") Properties.ContentType = ContentType(header.Value);
            else _headers[header.Key] = HeaderValue(header.Value, index++);
        }
        // Portable scheduling and durability cannot be disabled by native message configuration.
        Properties.Persistent = true;
        if (delay > 0)
        {
            Unsafe.Unbox<long>(_delay) = delay;
            _headers["x-delay"] = _delay;
        }
        else _headers.Remove("x-delay");
        _headers.PreserveContext = _headers.ContainsKey("traceparent");
    }

    private object HeaderValue(WireString value, int index)
    {
        if (value.Text is { } text) return text;
        if (MemoryMarshal.TryGetArray(value.Utf8, out var array) && array.Offset == 0 && array.Count == array.Array!.Length)
            return array.Array;
        // RabbitMQ.Client 7.2.2 accepts byte[] and string, not UTF-8 slices (upstream #2144).
        // One bounded FIFO across all header positions; hits do not update recency (D35/D52).
        var key = (index, value.Utf8.Length);
        if (!_buffers.TryGetValue(key, out var buffer))
        {
            buffer = new byte[value.Utf8.Length];
            if (_buffers.Count == CacheCapacity) _buffers.Remove(_bufferOrder[_nextBuffer]);
            _buffers.Add(key, buffer);
            _bufferOrder[_nextBuffer] = key;
            _nextBuffer = (_nextBuffer + 1) % CacheCapacity;
        }
        value.Utf8.Span.CopyTo(buffer);
        return buffer;
    }

    private string ContentType(WireString value)
    {
        if (value.Text is { } text) return text;
        foreach (var cached in _contentTypes)
            if (value.Utf8.Span.SequenceEqual(cached.Utf8)) return cached.Text;
        var decoded = value.ToString();
        var entry = (value.Utf8.ToArray(), decoded);
        if (_contentTypes.Count < CacheCapacity) _contentTypes.Add(entry);
        else _contentTypes[_nextContentType] = entry;
        _nextContentType = (_nextContentType + 1) % CacheCapacity;
        return decoded;
    }

    internal void Clear()
    {
        _headers.PreserveContext = false;
        _headers.Clear();
        Properties.ContentType = null;
        Properties.ContentEncoding = null;
        Properties.Headers = null;
        Properties.DeliveryMode = default;
        Properties.Priority = 0;
        Properties.CorrelationId = null;
        Properties.ReplyTo = null;
        Properties.Expiration = null;
        Properties.MessageId = null;
        Properties.Timestamp = default;
        Properties.Type = null;
        Properties.UserId = null;
        Properties.AppId = null;
        Properties.ClusterId = null;
    }
}

// RabbitMQ.Client 7.2.2 injects transport span context through IDictionary's indexer (upstream #2143).
// Keep the original creation context while allowing native confirmation headers and other additions.
internal sealed class RabbitMqHeaders : Dictionary<string, object?>, IDictionary<string, object?>
{
    internal bool PreserveContext;
    object? IDictionary<string, object?>.this[string key]
    {
        get => this[key];
        set
        {
            if (PreserveContext && key is "traceparent" or "tracestate") return;
            this[key] = value;
        }
    }
}

// Retained at the leased channel's batch high-water mark, separate from its single-send properties.
internal sealed class RabbitMqBatchSlot(IChannel channel)
{
    internal readonly RabbitMqPublisher Publisher = new(channel);
    private readonly Dictionary<RabbitMqSendEndpoint, SendTracing> _tracing = [];
    private RabbitMqNativeTransport _transport = null!;
    private RabbitMqSendEndpoint _endpoint = null!;

    internal ValueTask Send(RabbitMqNativeTransport transport, RabbitMqSendEndpoint endpoint,
        OutgoingMessage message, CancellationToken token, bool trace)
    {
        _transport = transport;
        _endpoint = endpoint;
        if (!trace || !endpoint.TraceEnabled) return Publish(message, token);
        if (!_tracing.TryGetValue(endpoint, out var tracing))
        {
            tracing = endpoint.CreateTracing(Publish);
            _tracing.Add(endpoint, tracing);
        }
        return tracing.SendAsync(message, token);
    }

    private ValueTask Publish(OutgoingMessage message, CancellationToken token) =>
        _transport.SendBorrowedAsync(_endpoint, Publisher, message, token);
}
