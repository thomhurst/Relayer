namespace Relayer.Transports;

/// <summary>
/// Starts and stops a named transport's consumers while the host runs. Resolve it as a keyed service with the transport
/// name. Sending is unaffected. Transports that support it document the option that defers their consumers' start.
/// </summary>
public interface IConsumerControl
{
    /// <summary>Starts the native consumers. Does nothing when they already run. Fails until the host completes startup and after it stops.</summary>
    Task StartAsync(CancellationToken cancellationToken);

    /// <summary>Stops the native consumers and drains in-flight work, as host shutdown does. Does nothing when they are stopped.</summary>
    Task StopAsync(CancellationToken cancellationToken);
}
