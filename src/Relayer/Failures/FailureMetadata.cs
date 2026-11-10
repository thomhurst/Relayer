using System.Buffers.Text;
using System.Globalization;
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
    /// <summary>JSON error history, including original identity, location and every attempt's exception.</summary>
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
        if (context.Headers.TryGetValue(FailureHeaders.History, out var value))
        {
            try
            {
                var history = JsonSerializer.Deserialize(value.ToString(), FailureJsonContext.Default.FailureHistory);
                if (history is not null && history.Attempts is not null) return history;
            }
            catch (JsonException) { }
            // Invalid error metadata must not prevent a poison message from reaching the dead-letter path.
        }
        string? id = null;
        try { id = context.MessageId; }
        catch (MessageEnvelopeException) { }
        return new() { OriginalMessageId = id, Consumer = consumer, Host = Environment.MachineName };
    }

    internal static void Record(DefaultMessageContext context, Exception error, string consumer, TimeProvider time)
    {
        var history = context.FailureHistory ??= ReadHistory(context, consumer);
        var now = time.GetUtcNow();
        if (history.Attempts.Count == 0) history.FirstFailureTime = now;
        history.LastFailureTime = now;
        history.AttemptCount = context.Attempt;
        history.Attempts.Add(new()
        {
            Attempt = context.Attempt, Time = now, Consumer = consumer, Host = Environment.MachineName,
            // Exception metadata is an error-path diagnostic, never used for dispatch or classification.
            ExceptionType = error.GetType().FullName ?? error.GetType().Name,
            Message = error.Message, StackTrace = error.StackTrace,
        });
    }

    internal static KeyValuePair<string, WireString>[] SettlementHeaders(DefaultMessageContext context, int redeliveries,
        string? reason = null)
    {
        var headers = new List<KeyValuePair<string, WireString>>(4)
        {
            new(FailureHeaders.Attempts, new(context.Attempt.ToString(CultureInfo.InvariantCulture))),
            new(FailureHeaders.Redeliveries, new(redeliveries.ToString(CultureInfo.InvariantCulture))),
        };
        if (context.FailureHistory is { } history)
            headers.Add(new(FailureHeaders.History, new(JsonSerializer.Serialize(history, FailureJsonContext.Default.FailureHistory))));
        else if (context.Headers.TryGetValue(FailureHeaders.History, out var prior))
            headers.Add(new(FailureHeaders.History, prior));
        if (reason is not null) headers.Add(new(FailureHeaders.DeadLetterReason, new(reason)));
        return headers.ToArray();
    }

    internal static KeyValuePair<string, WireString>[] Headers(DefaultMessageContext context, int redeliveries)
        => [new(FailureHeaders.Attempts, new(context.Attempt.ToString(CultureInfo.InvariantCulture))),
            new(FailureHeaders.Redeliveries, new(redeliveries.ToString(CultureInfo.InvariantCulture))),
            new(FailureHeaders.History, new(JsonSerializer.Serialize(context.FailureHistory!, FailureJsonContext.Default.FailureHistory)))];
}
