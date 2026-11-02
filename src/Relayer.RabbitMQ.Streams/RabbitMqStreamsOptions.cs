using RabbitMQ.Stream.Client;

namespace Relayer.RabbitMQ.Streams;

/// <summary>Native stream-system configuration and explicit topology declarations.</summary>
public sealed class RabbitMqStreamsOptions
{
    /// <summary>Passed unchanged to the owned native StreamSystem. Authentication, TLS and recovery remain native.</summary>
    public StreamSystemConfig Configuration { get; set; } = new();

    /// <summary>Streams to validate or provision. Name-only declarations check existence only.</summary>
    public IList<RabbitMqStream> Streams { get; } = [];
}

/// <summary>A stream name and optional native create-only specification.</summary>
public sealed record RabbitMqStream(string Name)
{
    /// <summary>Passed unchanged to CreateStream in Provision mode. Its Name must match this declaration.</summary>
    /// <remarks>Validate cannot read retention or other settings and reports configuration equivalence as unavailable.</remarks>
    public StreamSpec? Configuration { get; init; }
}
