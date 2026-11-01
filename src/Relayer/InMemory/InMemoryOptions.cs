using Relayer.Transports;

namespace Relayer.InMemory;

/// <summary>Native in-memory endpoint settings, captured when the transport is built.</summary>
public sealed record InMemoryEndpointOptions
{
    /// <summary>Native channel capacity per partition. Capacity times Partitions bounds outstanding unsettled messages.</summary>
    /// <remarks>Dead-letter snapshots are independently owned and unbounded; drain DeadLetters to release their copied bodies.</remarks>
    public int Capacity { get; init; } = 256;
    /// <summary>Native partitions. Ordered queues and logs use one serial receiver per partition.</summary>
    public int Partitions { get; init; } = 1;
    /// <summary>Native messages retained for allocation-free reuse after warmup.</summary>
    public int MessagePoolCapacity { get; init; } = 256;
    /// <summary>Capabilities to emulate. Replay, transactions and pause/resume are not implemented.</summary>
    public TransportCapabilities Capabilities { get; init; } = new()
    {
        DeadLetter = true,
        MaxDeliveryDelay = TimeSpan.MaxValue,
        MaxRedeliveryDelay = TimeSpan.MaxValue,
        MaxBatchSize = 100,
    };
}

/// <summary>Named Options configuration for an in-memory transport.</summary>
public sealed class InMemoryTransportOptions
{
    /// <summary>Overrides for declared endpoints. Unspecified declared endpoints use queue defaults.</summary>
    public IDictionary<string, InMemoryEndpointOptions> Endpoints { get; } = new Dictionary<string, InMemoryEndpointOptions>(StringComparer.Ordinal);
    /// <summary>Clock used for native scheduling and redelivery.</summary>
    public TimeProvider TimeProvider { get; init; } = TimeProvider.System;
}

/// <summary>In-memory routes have no additional native settings.</summary>
public sealed class InMemoryRouteOptions;

/// <summary>Native consumer settings. Portable handlers and concurrency use the common builder.</summary>
public sealed class InMemoryConsumerOptions
{
    /// <summary>Required for logs. Each registration has a unique group; different groups receive independent copies.</summary>
    public string? Group { get; set; }
    /// <summary>An explicit core failure executor. By default failures are dead-lettered natively.</summary>
    public ConsumeFailureDelegate? Failure { get; set; }
}
