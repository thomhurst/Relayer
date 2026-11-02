namespace Relayer.Transports;

/// <summary>Resolves broker-dependent capabilities before route validation and pipeline composition.</summary>
public interface ITransportCapabilityInitializer
{
    /// <summary>Initializes native capability evidence. None and Validate must not mutate broker topology.</summary>
    ValueTask InitializeCapabilitiesAsync(TopologyMode mode, CancellationToken cancellationToken);
}
