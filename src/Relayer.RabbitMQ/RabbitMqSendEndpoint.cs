using RabbitMQ.Client;
using RabbitMQ.Client.Exceptions;
using Relayer.Transports;

namespace Relayer.RabbitMQ;

internal sealed class RabbitMqSendEndpoint
{
    internal readonly string Exchange;
    internal readonly string RoutingKey;
    internal readonly SendTracing? Tracing;
    internal readonly EndpointTelemetry? Telemetry;
    internal string? DelayedExchange;
    internal TransportCapabilities Capabilities = RabbitMqTransport.GetCapabilities("endpoint");

    internal RabbitMqSendEndpoint(string destination, ConnectionFactory factory, MessageTracing? tracing, RelayerInstrumentation? instrumentation,
        Func<OutgoingMessage, CancellationToken, ValueTask> send,
        Func<ReadOnlyMemory<OutgoingMessage>, CancellationToken, ValueTask> batch)
    {
        var separator = destination.IndexOf(':');
        Exchange = separator < 0 ? "" : destination[..separator];
        RoutingKey = separator < 0 ? destination : destination[(separator + 1)..];
        if (tracing is null) return;
        var name = Exchange + ":" + RoutingKey;
        Tracing = new(tracing, new()
        {
            System = "rabbitmq", Destination = name, ServerAddress = factory.HostName,
            ServerPort = factory.Port == -1 ? (factory.Ssl.Enabled ? 5671 : 5672) : factory.Port,
            Attributes = RoutingKey.Length == 0 ? default : new KeyValuePair<string, object?>[] { new("messaging.rabbitmq.destination.routing_key", RoutingKey) },
        }, send, batch, headerNames: new("cloudEvents:"));
        Telemetry = instrumentation?.CreateEndpoint(new()
        {
            System = "rabbitmq", Destination = name, OperationName = "send", ServerAddress = factory.HostName,
            ServerPort = factory.Port == -1 ? (factory.Ssl.Enabled ? 5671 : 5672) : factory.Port,
        });
    }

    internal async ValueTask InitializeAsync(IConnection connection, RabbitMqTransportOptions options,
        TopologyMode mode, CancellationToken token)
    {
        // AMQP cannot inspect exchange types read-only. Only explicit Provision may verify native plugin support.
        if (mode != TopologyMode.Provision || !options.DelayedExchanges.TryGetValue(Exchange, out var declaration)) return;
        var channel = await connection.CreateChannelAsync(cancellationToken: token).ConfigureAwait(false);
        await using var lifetime = channel.ConfigureAwait(false);
        try
        {
            await channel.ExchangeDeclareAsync(declaration.Name, "x-delayed-message", declaration.Durable ?? true,
                declaration.AutoDelete ?? false, declaration.Arguments, cancellationToken: token).ConfigureAwait(false);
            DelayedExchange = declaration.Name;
            Capabilities = Capabilities with { MaxDeliveryDelay = TimeSpan.FromMilliseconds(uint.MaxValue - 1L) };
        }
        catch (OperationInterruptedException error) when (error.ShutdownReason is { ReplyCode: 406 or 503 } reason &&
            reason.ReplyText.Contains("unknown exchange type", StringComparison.OrdinalIgnoreCase))
        {
            // An absent plugin has no native delay capability. The core selects its existing durable outbox fallback.
        }
    }

    internal long Delay(DateTimeOffset? deliverAt)
    {
        if (deliverAt is null) return 0;
        var milliseconds = Math.Ceiling((deliverAt.Value - DateTimeOffset.UtcNow).TotalMilliseconds);
        if (milliseconds <= 0) return 0;
        if (DelayedExchange is null || milliseconds > uint.MaxValue - 1L)
            throw new NotSupportedException("RabbitMQ native delayed delivery is unavailable or exceeds the plugin limit. Use an active outbox transaction and a due-row dispatcher.");
        return (long)milliseconds;
    }
}
