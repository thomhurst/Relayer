using System.Buffers;
using System.Globalization;

namespace Relayer;

internal interface IOrderingKeySelector<TMessage>
{
    WireString? Select(TMessage message, ArrayBufferWriter<byte> buffer);
}

internal sealed class StringOrderingKeySelector<TMessage>(Func<TMessage, string> selector) : IOrderingKeySelector<TMessage>
{
    public WireString? Select(TMessage message, ArrayBufferWriter<byte> buffer) =>
        selector(message) is { } key ? new WireString(key) : (WireString?)null;
}

internal sealed class FormattedOrderingKeySelector<TMessage, TKey>(Func<TMessage, TKey> selector) : IOrderingKeySelector<TMessage>
    where TKey : IUtf8SpanFormattable
{
    public WireString? Select(TMessage message, ArrayBufferWriter<byte> buffer)
    {
        var key = selector(message);
        var size = 64;
        while (true)
        {
            if (key.TryFormat(buffer.GetSpan(size), out var written, default, CultureInfo.InvariantCulture))
            {
                buffer.Advance(written);
                return new WireString(buffer.WrittenMemory);
            }
            size = checked(size * 2);
        }
    }
}
