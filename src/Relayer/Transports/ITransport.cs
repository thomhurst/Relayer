namespace Relayer.Transports;

/// <summary>An adapter over one native messaging client.</summary>
public interface ITransport : IAsyncDisposable
{
    /// <summary>The transport name, unique within the application.</summary>
    string Name { get; }

    /// <summary>The native capabilities of this transport.</summary>
    TransportCapabilities Capabilities { get; }

    /// <summary>Sends a serialized message using the native client.</summary>
    /// <remarks>
    /// Observe cancellation and preserve native delivery semantics. Apply the message's native callbacks immediately
    /// before sending. Borrowed body, headers, and options remain valid until completion and must not be retained afterward.
    /// </remarks>
    ValueTask SendAsync(OutgoingMessage message, CancellationToken cancellationToken);

    /// <summary>Sends a batch using the native client's batching and ordering semantics.</summary>
    /// <remarks>
    /// Observe cancellation. Apply each message's native callbacks before sending. The batch memory and each message's
    /// borrowed storage remain valid until completion and must not be retained afterward. Completion does not imply
    /// atomicity: a native batch failure or cancellation can leave some messages delivered.
    /// </remarks>
    ValueTask SendBatchAsync(ReadOnlyMemory<OutgoingMessage> messages, CancellationToken cancellationToken);

    /// <summary>Connects and starts consumers.</summary>
    Task StartAsync(CancellationToken cancellationToken);

    /// <summary>Stops consumers, drains in-flight work and settles it.</summary>
    Task StopAsync(CancellationToken cancellationToken);
}
