using System.Text;

namespace Relayer;

/// <summary>A native header value, kept as UTF-16 text or UTF-8 memory until text is requested.</summary>
/// <remarks>The memory belongs to the native message and must not outlive it.</remarks>
public readonly struct WireString
{
    private readonly string? _text;
    private readonly ReadOnlyMemory<byte> _utf8;

    /// <summary>Wraps native UTF-16 text without copying it.</summary>
    public WireString(string text)
    {
        ArgumentNullException.ThrowIfNull(text);
        _text = text;
    }

    /// <summary>Wraps native UTF-8 memory without copying or decoding it.</summary>
    public WireString(ReadOnlyMemory<byte> utf8) => _utf8 = utf8;

    /// <summary>Whether the value contains no characters.</summary>
    public bool IsEmpty => _text is null ? _utf8.IsEmpty : _text.Length == 0;

    /// <summary>The original UTF-16 text, or null for a UTF-8 value.</summary>
    public string? Text => _text;

    /// <summary>The original UTF-8 memory, empty for a UTF-16 value.</summary>
    public ReadOnlyMemory<byte> Utf8 => _utf8;

    /// <summary>Copies or decodes the value into caller-owned storage without allocating.</summary>
    public bool TryFormat(Span<char> destination, out int charsWritten)
    {
        if (_text is not null)
        {
            if (!_text.AsSpan().TryCopyTo(destination))
            {
                charsWritten = 0;
                return false;
            }

            charsWritten = _text.Length;
            return true;
        }

        return Encoding.UTF8.TryGetChars(_utf8.Span, destination, out charsWritten);
    }

    /// <summary>Returns native text, decoding and allocating only for a nonempty UTF-8 value.</summary>
    public override string ToString() => _text ?? Encoding.UTF8.GetString(_utf8.Span);
}
