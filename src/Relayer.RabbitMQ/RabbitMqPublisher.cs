using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using RabbitMQ.Client;

namespace Relayer.RabbitMQ;

internal sealed class RabbitMqPublisher(IChannel channel)
{
    internal IChannel Channel { get; set; } = channel;
    internal readonly BasicProperties Properties = new();
    private readonly RabbitMqHeaders _headers = new();
    private readonly List<Dictionary<int, byte[]>> _buffers = [];
    private readonly List<(byte[] Utf8, string Text)> _contentTypes = [];
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
        // Retain exact-length storage per header position/observed length across sends.
        while (_buffers.Count <= index) _buffers.Add([]);
        var buffers = _buffers[index];
        if (!buffers.TryGetValue(value.Utf8.Length, out var buffer))
            buffers.Add(value.Utf8.Length, buffer = new byte[value.Utf8.Length]);
        value.Utf8.Span.CopyTo(buffer);
        return buffer;
    }

    private string ContentType(WireString value)
    {
        if (value.Text is { } text) return text;
        foreach (var cached in _contentTypes)
            if (value.Utf8.Span.SequenceEqual(cached.Utf8)) return cached.Text;
        var decoded = value.ToString();
        _contentTypes.Add((value.Utf8.ToArray(), decoded));
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
