using NATS.Client.Core;
using NATS.Client.JetStream;
using NATS.Client.JetStream.Models;

namespace Relayer.Nats;

/// <summary>Native NATS connection and JetStream topology configuration.</summary>
public sealed class NatsTransportOptions
{
    /// <summary>Native connection options, passed unchanged to the owned NatsConnection.</summary>
    public NatsOpts ConnectionOptions { get; set; } = new();

    /// <summary>Native JetStream options. Null uses the client's defaults for ConnectionOptions.</summary>
    public NatsJSOpts? JetStreamOptions { get; set; }

    /// <summary>Unchanged native publication options, including native retry policy. Applies to every subject on this named transport.</summary>
    public NatsJSPubOpts? PublishOptions { get; set; }

    /// <summary>Explicit stream declarations. Name-only entries validate existence without comparing settings.</summary>
    public IList<NatsStream> Streams { get; } = [];

    /// <summary>Explicit consumer declarations. Name-only entries validate existence without comparing settings.</summary>
    public IList<NatsConsumer> Consumers { get; } = [];
}

/// <summary>A named JetStream stream, with optional unchanged native provisioning configuration.</summary>
public sealed record NatsStream(string Name)
{
    /// <summary>Native create configuration. Its Name must match this declaration. Validate compares native settings read-only.</summary>
    public StreamConfig? Configuration { get; init; }
}

/// <summary>A named JetStream consumer, with optional unchanged native provisioning configuration.</summary>
public sealed record NatsConsumer(string Stream, string Name)
{
    /// <summary>Native create-only configuration. Its Name must match this declaration. Validate compares native settings read-only.</summary>
    public ConsumerConfig? Configuration { get; init; }
}
