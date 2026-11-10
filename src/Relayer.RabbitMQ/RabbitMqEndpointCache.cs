using System.Collections.Concurrent;

namespace Relayer.RabbitMQ;

internal sealed class RabbitMqReceiveEndpoint
{
    internal MessageOperationTracing Process { get; }
    internal RabbitMqSettlement Settlement { get; }
    internal RabbitMqReceiveEndpoint(MessageTracing tracing, MessagingEndpoint endpoint)
    {
        Process = new(tracing, endpoint, MessagingOperation.Process);
        Settlement = new(tracing, endpoint);
    }
}

// Native routing keys are unconstrained user data. Retain at most 256 endpoint operations (D52).
// Warm hits need one concurrent dictionary lookup; serialize only cold composition and FIFO eviction.
internal sealed class RabbitMqEndpointCache(MessageTracing tracing, string queue, string? serverAddress = null, int? serverPort = null)
{
    internal const int Capacity = 256;
    private readonly ConcurrentDictionary<(string Exchange, string Key), RabbitMqReceiveEndpoint> _entries = new();
    private readonly Queue<(string Exchange, string Key)> _order = new(Capacity);
    private readonly object _gate = new();
    internal RabbitMqReceiveEndpoint Get(string exchange, string key)
    {
        var route = (exchange, key);
        if (_entries.TryGetValue(route, out var endpoint)) return endpoint;
        lock (_gate)
        {
            if (_entries.TryGetValue(route, out endpoint)) return endpoint;
            endpoint = new(tracing, new MessagingEndpoint
            {
                System = "rabbitmq", Destination = exchange + ":" + key + ":" + queue,
                ServerAddress = serverAddress, ServerPort = serverPort,
                Attributes = key.Length == 0 ? default : new KeyValuePair<string, object?>[]
                { new("messaging.rabbitmq.destination.routing_key", key) },
            });
            if (_entries.Count == Capacity) _entries.TryRemove(_order.Dequeue(), out _);
            _entries.TryAdd(route, endpoint);
            _order.Enqueue(route);
            return endpoint;
        }
    }
}
