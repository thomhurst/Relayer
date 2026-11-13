using System.Globalization;
using System.Text;
using Google.Cloud.PubSub.V1;
using Encoding = System.Text.Encoding;

namespace Relayer.GooglePubSub;

// Error history is logical Relayer metadata, not a broker feature. Frame it losslessly within Pub/Sub's
// 1,024-byte attribute values. Ordinary application headers and CloudEvents retain their native mapping.
internal static class GooglePubSubFailureHeaders
{
    internal const string Parts = FailureHeaders.History + ".parts";
    private const string Prefix = FailureHeaders.History + ".part.";
    private const int MaxAttributes = 100;
    private const int MaxValueBytes = 1024;

    internal static bool IsHistory(string name) => name == FailureHeaders.History || name == Parts ||
        name.StartsWith(Prefix, StringComparison.Ordinal);

    internal static void Write(PubsubMessage message, string history)
    {
        List<string>? previous = null;
        foreach (var header in message.Attributes)
            if (header.Key.StartsWith(Prefix, StringComparison.Ordinal)) (previous ??= []).Add(header.Key);
        if (previous is not null)
            foreach (var name in previous) message.Attributes.Remove(name);
        message.Attributes.Remove(Parts);
        message.Attributes.Remove(FailureHeaders.History);
        if (Encoding.UTF8.GetByteCount(history) <= MaxValueBytes)
        {
            message.Attributes[FailureHeaders.History] = history;
            return;
        }
        var offset = 0;
        var count = 0;
        while (offset < history.Length)
        {
            if (count >= MaxAttributes) throw new InvalidOperationException("Pub/Sub failure history exceeds the native attribute capacity. The original delivery remains unacknowledged.");
            var bytes = 0;
            var chars = 0;
            foreach (var rune in history.AsSpan(offset).EnumerateRunes())
            {
                if (bytes + rune.Utf8SequenceLength > MaxValueBytes) break;
                bytes += rune.Utf8SequenceLength;
                chars += rune.Utf16SequenceLength;
            }
            message.Attributes[Part(count++)] = history.Substring(offset, chars);
            offset += chars;
        }
        message.Attributes[Parts] = count.ToString(CultureInfo.InvariantCulture);
    }

    internal static bool TryRead(PubsubMessage message, out WireString value)
    {
        value = default;
        if (!message.Attributes.TryGetValue(Parts, out var text) ||
            !int.TryParse(text, NumberStyles.None, CultureInfo.InvariantCulture, out var count) || count is < 1 or > MaxAttributes)
            return false;
        var length = 0;
        for (var i = 0; i < count; i++)
        {
            if (!message.Attributes.TryGetValue(Part(i), out var part) || Encoding.UTF8.GetByteCount(part) > MaxValueBytes) return false;
            length += part.Length;
        }
        var history = new StringBuilder(length);
        for (var i = 0; i < count; i++) history.Append(message.Attributes[Part(i)]);
        value = new(history.ToString());
        return true;
    }

    internal static void Validate(PubsubMessage message)
    {
        if (message.Attributes.Count > MaxAttributes)
            throw new InvalidOperationException("Pub/Sub failure copy needs more than 100 native attributes. Reserve attribute capacity for error history; the original delivery remains unacknowledged.");
        foreach (var header in message.Attributes)
            if (Encoding.UTF8.GetByteCount(header.Key) > 256 || Encoding.UTF8.GetByteCount(header.Value) > MaxValueBytes)
                throw new InvalidOperationException($"Pub/Sub failure copy attribute '{header.Key}' exceeds the native key/value byte limit. The original delivery remains unacknowledged.");
    }

    private static string Part(int index) => Prefix + index.ToString(CultureInfo.InvariantCulture);
}
