using Relayer.Pipeline;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Globalization;
using System.Text;
using Microsoft.Extensions.Logging;

namespace Relayer.Transports;

/// <summary>An endpoint operation's allocation-free metric recorder. All dimensions are computed at startup.</summary>
/// <remarks>
/// Failed measurements use the conventions' bounded error.type value _OTHER; logs preserve the actual exception.
/// Counters describe actual attempts/actions: do not count outbox enqueue as a send, or redelivery as immediate retry.
/// </remarks>
public sealed class EndpointTelemetry
{
    private readonly RelayerInstrumentation _instruments;
    private readonly TagList _tags;
    private readonly TagList _failedTags;
    private readonly Histogram<double> _duration;
    internal ILogger Logger { get; }
    internal bool IsProcess { get; }
    internal bool CriticalTimeEnabled => _instruments.CriticalTime.Enabled;
    internal bool DurationEnabled => _duration.Enabled;

    internal EndpointTelemetry(RelayerInstrumentation instruments, EndpointTelemetryOptions options, ILogger logger)
    {
        _instruments = instruments;
        Logger = logger;
        IsProcess = options.OperationName == "process";
        _duration = IsProcess ? instruments.ProcessDuration : instruments.OperationDuration;
        _tags.Add("messaging.system", options.System);
        _tags.Add("messaging.operation.name", options.OperationName);
        if (options.Destination is { } destination) _tags.Add("messaging.destination.name", destination);
        if (options.ConsumerGroup is { } group) _tags.Add("messaging.consumer.group.name", group);
        if (options.Subscription is { } subscription) _tags.Add("messaging.destination.subscription.name", subscription);
        if (options.ServerAddress is { } address) _tags.Add("server.address", address);
        if (options.ServerPort is { } port) _tags.Add("server.port", port);
        // Seven constant tags plus error.type fit TagList's inline storage, including on the failure path.
        _failedTags = _tags;
        _failedTags.Add("error.type", "_OTHER");
    }

    /// <summary>Records process duration for process, or client operation duration for other operation names.</summary>
    public void RecordDuration(TimeSpan duration, bool failed = false) => _duration.Record(duration.TotalSeconds, failed ? _failedTags : _tags);
    /// <summary>Counts messages attempted at the broker, including failed send attempts.</summary>
    public void RecordSent(long count = 1, bool failed = false) => _instruments.Sent.Add(count, failed ? _failedTags : _tags);
    /// <summary>Counts native deliveries once, independently of immediate retries.</summary>
    public void RecordConsumed(long count = 1) => _instruments.Consumed.Add(count, in _tags);
    /// <summary>Records processing end minus CloudEvents creation time, in seconds.</summary>
    public void RecordCriticalTime(TimeSpan elapsed) => _instruments.CriticalTime.Record(elapsed.TotalSeconds, in _tags);
    /// <summary>Counts immediate in-process retries.</summary>
    public void RecordRetry(long count = 1) => _instruments.Retries.Add(count, in _tags);
    /// <summary>Counts messages durably dead-lettered.</summary>
    public void RecordDeadLettered(long count = 1) => _instruments.DeadLettered.Add(count, in _tags);
    /// <summary>Updates pending outbox messages; removal uses a negative delta.</summary>
    public void AddOutboxPending(long delta) => _instruments.OutboxPending.Add(delta, in _tags);
    /// <summary>Records dispatch time minus enqueue time, in seconds.</summary>
    public void RecordDispatchLag(TimeSpan elapsed) => _instruments.DispatchLag.Record(elapsed.TotalSeconds, in _tags);
    /// <summary>Counts inbox duplicates that were suppressed.</summary>
    public void RecordInboxDuplicate(long count = 1) => _instruments.Duplicates.Add(count, in _tags);
    /// <summary>Records native consumer lag. Kafka adapters reuse Dekaf's lag calculation.</summary>
    public void RecordConsumerLag(long lag) => _instruments.ConsumerLag.Record(lag, in _tags);
    /// <summary>Records circuit state: 0 closed, 1 open, 2 half-open.</summary>
    public void RecordCircuitState(int state) => _instruments.CircuitState.Record(state, in _tags);

    internal void RecordCriticalTime(DefaultMessageContext context)
    {
        if (!CriticalTimeEnabled || !context.Headers.TryGetValue(context.EnvelopeNames.Time, out var time)) return;
        Span<char> buffer = stackalloc char[64];
        scoped ReadOnlySpan<char> text = time.Text;
        if (time.Text is null)
        {
            if (time.Utf8.Length > buffer.Length) return;
            var length = Encoding.UTF8.GetChars(time.Utf8.Span, buffer);
            text = buffer[..length];
        }
        if (DateTimeOffset.TryParse(text, CultureInfo.InvariantCulture, DateTimeStyles.None, out var created))
            RecordCriticalTime(DateTimeOffset.UtcNow - created);
    }
}
