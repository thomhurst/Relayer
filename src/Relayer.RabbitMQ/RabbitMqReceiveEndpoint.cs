using Relayer.Transports;

namespace Relayer.RabbitMQ;

// One queue-scoped operation set per consumer; native routing keys are unconstrained user data.
internal sealed class RabbitMqReceiveEndpoint
{
    internal MessageOperationTracing Process { get; }
    internal RabbitMqSettlement Settlement { get; }
    internal RabbitMqReceiveEndpoint(MessageTracing tracing, string queue, string? serverAddress = null, int? serverPort = null)
    {
        var endpoint = new MessagingEndpoint
        {
            System = "rabbitmq", Destination = queue,
            ServerAddress = serverAddress, ServerPort = serverPort,
        };
        Process = new(tracing, endpoint, MessagingOperation.Process);
        Settlement = new(tracing, endpoint);
    }
}
