using RabbitMQ.Stream.Client;

namespace Relayer.RabbitMQ.Streams;

/// <summary>Native stream-system configuration and explicit topology declarations.</summary>
public sealed class RabbitMqStreamsOptions
{
    /// <summary>Passed unchanged to the owned native StreamSystem. Authentication, TLS and recovery remain native.</summary>
    public StreamSystemConfig Configuration
    {
        get; set;
    } = new();

    /// <summary>Streams to validate or provision. Name-only declarations check existence only.</summary>
    public IList<RabbitMqStream> Streams
    {
        get;
    } = [];

    /// <summary>Super streams to validate or provision. Name-only declarations check for native partitions.</summary>
    public IList<RabbitMqSuperStream> SuperStreams
    {
        get;
    } = [];
}

/// <summary>A stream name and optional native create-only specification.</summary>
public sealed record RabbitMqStream(string Name)
{
    /// <summary>Passed unchanged to CreateStream in Provision mode. Its Name must match this declaration.</summary>
    /// <remarks>Validate cannot read retention or other settings and reports configuration equivalence as unavailable.</remarks>
    public StreamSpec? Configuration
    {
        get; init;
    }
}

/// <summary>A super-stream name and optional native create-only specification.</summary>
public sealed record RabbitMqSuperStream(string Name)
{
    /// <summary>Passed unchanged to CreateSuperStream in Provision mode. Its Name must match this declaration.</summary>
    /// <remarks>The native API cannot verify configuration equivalence for an existing super stream.</remarks>
    public SuperStreamSpec? Configuration
    {
        get; init;
    }
}
