using Relayer.Transports;
namespace Relayer;

/// <summary>Settings captured when a send route is composed.</summary>
public sealed class SendRouteOptions
{
    /// <summary>The maximum duration of a send, including middleware and outbox enlistment.</summary>
    public TimeSpan Timeout { get; init; } = TimeSpan.FromSeconds(30);

    /// <summary>The CloudEvents source URI reference.</summary>
    public string Source { get; init; } = "/";

    /// <summary>The transport's binary-mode header binding.</summary>
    public HeaderNames HeaderNames { get; init; } = new();

    /// <summary>Whether to write CloudEvents attributes or only the serialized body.</summary>
    public MessageEnvelopeMode EnvelopeMode { get; init; } = MessageEnvelopeMode.CloudEvents;

    /// <summary>The longest scheduled-send delay this route must support. Null declares no startup requirement.</summary>
    public TimeSpan? RequiredDeliveryDelay { get; init; }

    /// <summary>The required transaction mechanism. Native availability never falls back to database durability.</summary>
    public SendTransactionRequirement TransactionRequirement { get; init; }

    internal void Validate(string location, List<string> failures)
    {
        if (RequiredDeliveryDelay < TimeSpan.Zero)
            failures.Add($"{location}: RequiredDeliveryDelay must be null or non-negative.");
        if (!Enum.IsDefined(TransactionRequirement))
            failures.Add($"{location}: TransactionRequirement must be a defined value.");
        if (Timeout <= TimeSpan.Zero || Timeout.TotalMilliseconds > uint.MaxValue - 1)
            failures.Add($"{location}: requires a finite positive send timeout of at most {uint.MaxValue - 1} ms.");
        if (string.IsNullOrWhiteSpace(Source) || !Uri.TryCreate(Source, UriKind.RelativeOrAbsolute, out _))
            failures.Add($"{location}: CloudEvents source must be a nonempty URI reference.");
        if (HeaderNames is null) failures.Add($"{location}: header names must not be null.");
        if (EnvelopeMode is not (MessageEnvelopeMode.CloudEvents or MessageEnvelopeMode.Raw))
            failures.Add($"{location}: envelope mode is not a defined value.");
    }
}
