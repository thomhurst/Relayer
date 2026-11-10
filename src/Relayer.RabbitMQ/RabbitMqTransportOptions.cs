using RabbitMQ.Client;

namespace Relayer.RabbitMQ;

/// <summary>Options for the RabbitMq transport. Native client configuration is passed through unchanged (docs/decisions.md D1).</summary>
public sealed class RabbitMqTransportOptions
{
    /// <summary>Native connection factory, passed straight to RabbitMQ.Client.</summary>
    public ConnectionFactory ConnectionFactory { get; set; } = new();

    /// <summary>Number of exclusively leased native publisher channels. Defaults to 16.</summary>
    public int PublisherCount { get; set; } = 16;

    /// <summary>Native channel options. Both publisher confirmation flags must remain enabled.</summary>
    public CreateChannelOptions PublisherChannelOptions { get; set; } = new(true, true);

    /// <summary>Optional delayed exchange declarations keyed by the ordinary exchange name (empty for the default exchange).</summary>
    /// <remarks>Only Provision verifies these through native x-delayed-message declarations. Configure bindings externally.</remarks>
    public IDictionary<string, RabbitMqExchange> DelayedExchanges { get; } = new Dictionary<string, RabbitMqExchange>(StringComparer.Ordinal);

    /// <summary>Native exchange declarations. Unspecified properties mean existence-only validation.</summary>
    public IList<RabbitMqExchange> Exchanges { get; } = [];

    /// <summary>Native queue declarations, including DLX arguments.</summary>
    public IList<RabbitMqQueue> Queues { get; } = [];

    /// <summary>Native queue-to-exchange bindings. AMQP cannot inspect bindings in Validate mode.</summary>
    public IList<RabbitMqBinding> Bindings { get; } = [];
}

/// <summary>An AMQP exchange declaration. Provision defaults to a durable, non-auto-delete direct exchange.</summary>
public sealed record RabbitMqExchange(string Name)
{
    /// <summary>Native exchange type. Null requests no type verification in Validate mode.</summary>
    public string? Type { get; init; }
    /// <summary>Native durability. Null requests no durability verification in Validate mode.</summary>
    public bool? Durable { get; init; }
    /// <summary>Native auto-delete flag. Null requests no flag verification in Validate mode.</summary>
    public bool? AutoDelete { get; init; }
    /// <summary>Native arguments, passed unchanged. AMQP cannot read them back in Validate mode.</summary>
    public IDictionary<string, object?>? Arguments { get; init; }
}

/// <summary>An explicitly named AMQP queue. Provision defaults to durable, non-exclusive and non-auto-delete.</summary>
public sealed record RabbitMqQueue(string Name)
{
    /// <summary>Native durability. Null requests no durability verification in Validate mode.</summary>
    public bool? Durable { get; init; }
    /// <summary>Native exclusivity. Null requests no exclusivity verification in Validate mode.</summary>
    public bool? Exclusive { get; init; }
    /// <summary>Native auto-delete flag. Null requests no flag verification in Validate mode.</summary>
    public bool? AutoDelete { get; init; }
    /// <summary>Native arguments, including x-dead-letter-exchange and x-dead-letter-routing-key, passed unchanged.</summary>
    public IDictionary<string, object?>? Arguments { get; init; }
}

/// <summary>A native queue binding. Empty routing keys are valid.</summary>
public sealed record RabbitMqBinding(string Queue, string Exchange, string RoutingKey)
{
    /// <summary>Native binding arguments, passed unchanged.</summary>
    public IDictionary<string, object?>? Arguments { get; init; }
}
