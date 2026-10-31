namespace Relayer.Transports;

/// <summary>An adapter over one native messaging client.</summary>
public interface ITransport : IAsyncDisposable
{
    /// <summary>The transport name, unique within the application.</summary>
    string Name { get; }

    /// <summary>The native capabilities of this transport.</summary>
    TransportCapabilities Capabilities { get; }

    /// <summary>Connects and starts consumers.</summary>
    Task StartAsync(CancellationToken cancellationToken);

    /// <summary>Stops consumers, drains in-flight work and settles it.</summary>
    Task StopAsync(CancellationToken cancellationToken);
}
