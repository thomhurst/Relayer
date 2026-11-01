using System.Runtime.CompilerServices;
using Relayer.Transports;

namespace Relayer;

// Owned by a pooled consume context, never shared across concurrent attempts.
internal sealed class AttemptSendBuffer
{
    private readonly Segment _entries = new();
    private int _count;

    internal void Add(ITransport transport, OutgoingMessage message, TimeSpan timeout)
    {
        var index = Interlocked.Increment(ref _count) - 1;
        var segment = _entries;
        for (var remaining = index; remaining >= Segment.Size; remaining -= Segment.Size)
        {
            var next = Volatile.Read(ref segment.Next);
            if (next is null)
                next = Interlocked.CompareExchange(ref segment.Next, new Segment(), null) ?? segment.Next;
            segment = next!;
        }
        // Each concurrent send owns one unique index. Flush starts only after the handler awaits its sends.
        var slot = index % Segment.Size;
        var entry = segment.Items[slot] ??= new Entry();
        entry.Copy(transport, message, timeout);
    }

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    internal async ValueTask FlushAsync(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var segment = _entries;
        for (var i = 0; i < _count; i++)
        {
            if (i > 0 && i % Segment.Size == 0) segment = segment.Next!;
            if (segment.Items[i % Segment.Size] is { Ready: true } entry)
                await entry.SendAsync(cancellationToken).ConfigureAwait(false);
        }
    }

    internal void Clear()
    {
        var segment = _entries;
        for (var i = 0; i < _count; i++)
        {
            if (i > 0 && i % Segment.Size == 0) segment = segment.Next!;
            segment.Items[i % Segment.Size]?.Clear();
        }
        _count = 0;
    }

    private sealed class Segment
    {
        internal const int Size = 16;
        internal readonly Entry?[] Items = new Entry[Size];
        internal Segment? Next;
    }

    private sealed class Entry
    {
        internal bool Ready { get; private set; }
        private byte[] _bytes = [];
        private KeyValuePair<string, WireString>[] _headers = [];
        private int _headerCount;
        private readonly SendOptions _options = new();
        private CancellationTokenSource _timeout = new();
        private TimeSpan _timeoutDelay;
        private ITransport? _transport;
        private OutgoingMessage _message;

        internal void Copy(ITransport transport, OutgoingMessage message, TimeSpan timeout)
        {
            var length = message.Body.Length;
            foreach (var header in message.Headers.Span) length = checked(length + header.Value.Utf8.Length);
            length = checked(length + (message.OrderingKey?.Utf8.Length ?? 0));
            if (_bytes.Length < length) _bytes = new byte[Math.Max(256, length)];
            if (_headers.Length < message.Headers.Length) _headers = new KeyValuePair<string, WireString>[message.Headers.Length];
            _headerCount = message.Headers.Length;
            message.Body.Span.CopyTo(_bytes);
            var offset = message.Body.Length;
            for (var i = 0; i < message.Headers.Length; i++)
            {
                var header = message.Headers.Span[i];
                _headers[i] = new(header.Key, CopyValue(header.Value, ref offset));
            }
            var key = message.OrderingKey is { } orderingKey ? CopyValue(orderingKey, ref offset) : (WireString?)null;
            message.CopyNativeOptionsTo(_options);
            _message = new(message.Destination, _bytes.AsMemory(0, message.Body.Length),
                _headers.AsMemory(0, message.Headers.Length), key, message.DeliverAt, _options);
            _transport = transport;
            _timeoutDelay = timeout;
            Ready = true;
        }

        private WireString CopyValue(WireString value, ref int offset)
        {
            if (value.Text is not null) return value;
            var data = value.Utf8;
            data.Span.CopyTo(_bytes.AsSpan(offset));
            var result = new WireString(_bytes.AsMemory(offset, data.Length));
            offset += data.Length;
            return result;
        }

        [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
        internal async ValueTask SendAsync(CancellationToken cancellationToken)
        {
            // A source that was cancelled is replaced only on the cancellation/error path.
            if (!_timeout.TryReset())
            {
                _timeout.Dispose();
                _timeout = new();
            }
            using var registration = cancellationToken.UnsafeRegister(static state => ((CancellationTokenSource)state!).Cancel(), _timeout);
            _timeout.CancelAfter(_timeoutDelay);
            try
            {
                _timeout.Token.ThrowIfCancellationRequested();
                await _transport!.SendAsync(_message, _timeout.Token).ConfigureAwait(false);
                _timeout.Token.ThrowIfCancellationRequested();
            }
            finally
            {
                _timeout.CancelAfter(Timeout.InfiniteTimeSpan);
            }
        }

        internal void Clear()
        {
            Array.Clear(_headers, 0, _headerCount);
            _headerCount = 0;
            Ready = false;
            _message = default;
            _transport = null;
            _options.Reset();
        }
    }
}
