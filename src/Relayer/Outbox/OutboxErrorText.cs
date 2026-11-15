namespace Relayer.Transports;

// Formats a dispatch failure for an outbox row's last_error. Runs only on the failed-send path.
internal static class OutboxErrorText
{
    internal const int MaxChars = 4096;

    // "Type: message", plus " ---> InnerType: message" for the innermost exception, so an operator
    // can tell failure kinds apart from the row alone.
    internal static string Describe(Exception error)
    {
        var inner = error;
        while (inner.InnerException is { } next) inner = next;
        var text = Format(error);
        if (!ReferenceEquals(inner, error)) text += " ---> " + Format(inner);
        if (text.Length <= MaxChars) return text;
        return text[..(char.IsHighSurrogate(text[MaxChars - 1]) ? MaxChars - 1 : MaxChars)];
    }

    private static string Format(Exception error) => (error.GetType().FullName ?? error.GetType().Name) + ": " + error.Message;
}
