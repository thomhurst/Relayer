using System.Buffers.Text;
using System.Globalization;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace Relayer;

/// <summary>Stable headers owned by the failure pipeline. Native redelivery preserves them where the broker can update attributes; SQS visibility retries retain native attributes and count receives (D67).</summary>
public static class FailureHeaders
{
    /// <summary>Number of failed processing attempts, including immediate retries.</summary>
    public const string Attempts = "relayer.failure.attempts";
    /// <summary>Number of scheduled/native redeliveries already requested.</summary>
    public const string Redeliveries = "relayer.failure.redeliveries";
    /// <summary>Bounded JSON error history, including original identity, location and retained exceptions.</summary>
    public const string History = "relayer.failure.history";
    /// <summary>The handler's reason when manual dead-lettering requires a durable copy.</summary>
    public const string DeadLetterReason = "relayer.dead_letter.reason";
}

internal sealed class FailureHistory
{
    public string? OriginalMessageId { get; set; }
    public string Consumer { get; set; } = "";
    public string Host { get; set; } = "";
    public DateTimeOffset FirstFailureTime { get; set; }
    public DateTimeOffset LastFailureTime { get; set; }
    public int AttemptCount { get; set; }
    public bool Truncated { get; set; }
    public List<FailureAttempt> Attempts { get; set; } = [];
}

internal sealed class FailureAttempt
{
    public int Attempt { get; set; }
    public DateTimeOffset Time { get; set; }
    public string Consumer { get; set; } = "";
    public string Host { get; set; } = "";
    public string ExceptionType { get; set; } = "";
    public string Message { get; set; } = "";
    public string? StackTrace { get; set; }
}

[JsonSerializable(typeof(FailureHistory))]
internal partial class FailureJsonContext : JsonSerializerContext;

internal static class FailureMetadata
{
    internal const int MaxAttempts = 8;
    internal const int MaxHistoryBytes = 16 * 1024;
    internal const int MaxStackTraceChars = 2048;
    private const int MaxFieldChars = 1024;

    internal static int ReadCount(MessageHeaders headers, string name)
    {
        if (!headers.TryGetValue(name, out var value)) return 0;
        var parsed = value.Text is { } text
            ? int.TryParse(text, NumberStyles.None, CultureInfo.InvariantCulture, out var count)
            : Utf8Parser.TryParse(value.Utf8.Span, out count, out var consumed) && consumed == value.Utf8.Length;
        if (!parsed || count < 0 || count == int.MaxValue)
            throw new MessageEnvelopeException($"Invalid '{name}' failure metadata. Expected a non-negative attempt count below Int32.MaxValue.");
        return count;
    }

    internal static FailureHistory ReadHistory(MessageContext context, string consumer)
    {
        if (context.Headers.TryGetValue(FailureHeaders.History, out var value) && ByteCount(value) <= MaxHistoryBytes)
        {
            try
            {
                var history = JsonSerializer.Deserialize(value.ToString(), FailureJsonContext.Default.FailureHistory);
                if (history is not null && history.Attempts is not null)
                {
                    Normalize(history);
                    return history;
                }
            }
            catch (JsonException) { }
            // Invalid error metadata must not prevent a poison message from reaching the dead-letter path.
        }
        string? id = null;
        try { id = context.MessageId; }
        catch (MessageEnvelopeException) { }
        return new() { OriginalMessageId = id, Consumer = consumer, Host = Environment.MachineName,
            Truncated = context.Headers.TryGetValue(FailureHeaders.History, out _) };
    }

    internal static void Record(DefaultMessageContext context, Exception error, string consumer, TimeProvider time)
    {
        var history = context.FailureHistory ??= ReadHistory(context, consumer);
        var now = time.GetUtcNow();
        if (history.Attempts.Count == 0) history.FirstFailureTime = now;
        history.LastFailureTime = now;
        history.AttemptCount = context.Attempt;
        if (history.Attempts.Count == MaxAttempts)
        {
            history.Attempts.RemoveAt(1);
            history.Truncated = true;
        }
        history.Attempts.Add(new()
        {
            Attempt = context.Attempt, Time = now, Consumer = consumer, Host = Environment.MachineName,
            // Exception metadata is an error-path diagnostic, never used for dispatch or classification.
            ExceptionType = error.GetType().FullName ?? error.GetType().Name,
            Message = error.Message, StackTrace = error.StackTrace,
        });
        Normalize(history);
    }

    internal static KeyValuePair<string, WireString>[] SettlementHeaders(DefaultMessageContext context, int redeliveries,
        string? reason = null, long? maxBytes = null)
    {
        var headers = new List<KeyValuePair<string, WireString>>(4)
        {
            new(FailureHeaders.Attempts, new(context.Attempt.ToString(CultureInfo.InvariantCulture))),
            new(FailureHeaders.Redeliveries, new(redeliveries.ToString(CultureInfo.InvariantCulture))),
        };
        if (reason is not null) headers.Add(new(FailureHeaders.DeadLetterReason, new(reason)));
        var history = context.FailureHistory;
        if (history is null && context.Headers.TryGetValue(FailureHeaders.History, out _))
            history = context.FailureHistory = ReadHistory(context, "");
        if (history is not null)
        {
            var remaining = maxBytes ?? long.MaxValue;
            foreach (var header in headers) remaining -= Encoding.UTF8.GetByteCount(header.Key) + ByteCount(header.Value);
            remaining -= Encoding.UTF8.GetByteCount(FailureHeaders.History);
            var json = Serialize(history, remaining);
            if (json is not null) headers.Add(new(FailureHeaders.History, new(json)));
        }
        return headers.ToArray();
    }

    internal static KeyValuePair<string, WireString>[] Headers(DefaultMessageContext context, int redeliveries, long? maxBytes = null)
        => SettlementHeaders(context, redeliveries, maxBytes: maxBytes);

    // Copies know all portable fields. Native settlement only exposes lazy named headers: its broker
    // remains authoritative for envelope overhead and other native properties.
    internal static void FitCopyHistory(List<KeyValuePair<string, WireString>> headers, in OutgoingMessage original, long? maximum, FailureHistory? history)
    {
        if (maximum is null) return;
        var remaining = maximum.Value - original.Body.Length;
        if (original.OrderingKey is { } key) remaining -= ByteCount(key);
        var historyIndex = -1;
        for (var i = 0; i < headers.Count; i++)
        {
            var header = headers[i];
            remaining -= Encoding.UTF8.GetByteCount(header.Key);
            if (header.Key == FailureHeaders.History) historyIndex = i;
            else remaining -= ByteCount(header.Value);
        }
        if (historyIndex < 0 || history is null || ByteCount(headers[historyIndex].Value) <= remaining) return;
        var json = Serialize(history, remaining);
        if (json is null) headers.RemoveAt(historyIndex);
        else headers[historyIndex] = new(FailureHeaders.History, new(json));
    }

    internal static string Description(Exception error)
    {
        var message = error.Message;
        if (message.Length <= MaxFieldChars) return message;
        var length = char.IsHighSurrogate(message[MaxFieldChars - 1]) ? MaxFieldChars - 1 : MaxFieldChars;
        return message[..length];
    }

    internal static int ByteCount(WireString value) => value.Text is { } text ? Encoding.UTF8.GetByteCount(text) : value.Utf8.Length;

    private static void Normalize(FailureHistory history)
    {
        while (history.Attempts.Count > MaxAttempts)
        {
            history.Attempts.RemoveAt(1);
            history.Truncated = true;
        }
        history.OriginalMessageId = Limit(history.OriginalMessageId, MaxFieldChars, history);
        history.Consumer = Limit(history.Consumer, MaxFieldChars, history) ?? "";
        history.Host = Limit(history.Host, MaxFieldChars, history) ?? "";
        for (var i = history.Attempts.Count - 1; i >= 0; i--)
        {
            var attempt = history.Attempts[i];
            if (attempt is null)
            {
                history.Attempts.RemoveAt(i);
                history.Truncated = true;
                continue;
            }
            attempt.Consumer = Limit(attempt.Consumer, MaxFieldChars, history) ?? "";
            attempt.Host = Limit(attempt.Host, MaxFieldChars, history) ?? "";
            attempt.ExceptionType = Limit(attempt.ExceptionType, MaxFieldChars, history) ?? "";
            attempt.Message = Limit(attempt.Message, MaxFieldChars, history) ?? "";
            attempt.StackTrace = Limit(attempt.StackTrace, MaxStackTraceChars, history);
        }
    }

    private static string? Limit(string? value, int chars, FailureHistory history)
    {
        if (value is null || value.Length <= chars) return value;
        history.Truncated = true;
        // Do not split a UTF-16 surrogate pair at the truncation boundary.
        if (chars > 0 && char.IsHighSurrogate(value[chars - 1])) chars--;
        return value[..chars];
    }

    private static string? Serialize(FailureHistory history, long budget)
    {
        Normalize(history);
        budget = Math.Min(budget, MaxHistoryBytes);
        while (budget > 0)
        {
            var json = JsonSerializer.Serialize(history, FailureJsonContext.Default.FailureHistory);
            if (Encoding.UTF8.GetByteCount(json) <= budget) return json;
            history.Truncated = true;
            // Retain the first and latest exceptions ahead of intermediate attempts.
            if (history.Attempts.Count > 2) history.Attempts.RemoveAt(1);
            else
            {
                var changed = false;
                foreach (var attempt in history.Attempts)
                {
                    changed |= !string.IsNullOrEmpty(attempt.StackTrace) || attempt.Message.Length > 0 ||
                        attempt.Consumer.Length > 0 || attempt.Host.Length > 0 || attempt.ExceptionType.Length > 0;
                    attempt.StackTrace = Limit(attempt.StackTrace, (attempt.StackTrace?.Length ?? 0) / 2, history);
                    attempt.Message = Limit(attempt.Message, attempt.Message.Length / 2, history)!;
                    attempt.Consumer = Limit(attempt.Consumer, attempt.Consumer.Length / 2, history)!;
                    attempt.Host = Limit(attempt.Host, attempt.Host.Length / 2, history)!;
                    attempt.ExceptionType = Limit(attempt.ExceptionType, attempt.ExceptionType.Length / 2, history)!;
                }
                changed |= !string.IsNullOrEmpty(history.OriginalMessageId) || history.Consumer.Length > 0 || history.Host.Length > 0;
                history.OriginalMessageId = Limit(history.OriginalMessageId, (history.OriginalMessageId?.Length ?? 0) / 2, history);
                history.Consumer = Limit(history.Consumer, history.Consumer.Length / 2, history)!;
                history.Host = Limit(history.Host, history.Host.Length / 2, history)!;
                if (!changed) return null;
            }
        }
        return null;
    }
}
