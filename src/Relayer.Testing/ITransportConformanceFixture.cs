using Relayer.Transports;

namespace Relayer.Testing;

/// <summary>Bridges conformance scenarios to real native transport operations on an isolated endpoint.</summary>
/// <remarks>
/// Create a fresh fixture per scenario, with the supplied MessageTracing owner and native instrumentation enabled.
/// Provision an empty endpoint with capacity for at least 32 small messages. Dispose must stop receivers and remove owned resources.
/// Do not implement delivery, ordering, settlement, reconnect or tracing in the bridge: call the adapter and broker.
/// </remarks>
public interface ITransportConformanceFixture<TIncoming> : IAsyncDisposable
{
    /// <summary>The adapter under test, including its production send tracing.</summary>
    ITransport Transport { get; }
    /// <summary>The isolated native destination and source.</summary>
    string Destination { get; }
    /// <summary>The expected OpenTelemetry messaging.system, for example kafka or servicebus.</summary>
    string MessagingSystem { get; }
    /// <summary>The native completion operation, for example ack, complete or commit.</summary>
    string SettlementOperation { get; }
    /// <summary>The adapter's CloudEvents binding names.</summary>
    HeaderNames HeaderNames { get; }
    /// <summary>Registers the supplied core receive pipeline before the first start.</summary>
    void RegisterConsumer(Func<TIncoming, CancellationToken, ValueTask<Disposition>> receive);
    /// <summary>Applies a decision using production native settlement, including its instrumentation.</summary>
    ValueTask SettleAsync(TIncoming incoming, Disposition disposition, CancellationToken cancellationToken);
    /// <summary>Reads an owned snapshot from the actual native dead-letter destination.</summary>
    ValueTask<ConformanceDeadLetter> ReadDeadLetterAsync(CancellationToken cancellationToken);
    /// <summary>Interrupts the broker connection. In-memory fixtures stop their transport instead.</summary>
    Task DisconnectAsync(CancellationToken cancellationToken);
    /// <summary>Restores the broker connection and waits for native recovery without replacing the endpoint.</summary>
    Task ReconnectAsync(CancellationToken cancellationToken);
}

/// <summary>An owned native dead-letter snapshot; header values must outlive the receive operation.</summary>
public sealed record ConformanceDeadLetter(ReadOnlyMemory<byte> Body,
    IReadOnlyDictionary<string, string> Headers, string Reason, string? Description);

/// <summary>A separately runnable transport contract.</summary>
public enum TransportConformanceCase
{
    /// <summary>Send, receive, borrowed storage and completion survive a consumer restart.</summary>
    SendReceiveSettle,
    /// <summary>Native redelivery preserves body and creation context.</summary>
    Retry,
    /// <summary>Native dead-lettering preserves error metadata and does not stall healthy messages.</summary>
    DeadLetter,
    /// <summary>Native scheduled delivery never runs before its due time.</summary>
    Delay,
    /// <summary>Native delayed redelivery never runs before its due time.</summary>
    RedeliveryDelay,
    /// <summary>A native key preserves order under outstanding sends.</summary>
    OrderingByKey,
    /// <summary>Producer, process and settlement spans follow the messaging conventions.</summary>
    TracePropagation,
    /// <summary>Stored creation headers survive dispatch and subsequent message copies unchanged.</summary>
    CreationContextCopies,
    /// <summary>Delivery resumes after the native connection is restored.</summary>
    Reconnect,
    /// <summary>Graceful stop drains in-flight processing without failure logs or native lifecycle violations.</summary>
    CleanShutdown,
}

/// <summary>A successful scenario, or an explicit capability-based skip. Contract failures throw.</summary>
public sealed record TransportConformanceResult(TransportConformanceCase Case, string? SkipReason = null)
{
    /// <summary>Whether the endpoint lacks the native capability required by this scenario.</summary>
    public bool Skipped => SkipReason is not null;
}

/// <summary>Required native lifecycle evidence for the clean-shutdown case.</summary>
/// <remarks>Wire Logs to all adapter and native-client logging before starting the fixture.</remarks>
public interface ITransportShutdownConformanceFixture
{
    /// <summary>Capture owned by this fixture, kept alive through disposal.</summary>
    ConformanceLogCapture Logs { get; }
    /// <summary>
    /// Verifies native lifecycle instrumentation saw no settlement, renewal or send after closure,
    /// and that the in-flight delivery completed durably or remains available for native redelivery.
    /// Must use real adapter/native state, never assume StopAsync implies these guarantees.
    /// </summary>
    ValueTask VerifyShutdownAsync(CancellationToken cancellationToken);
}
