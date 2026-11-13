using Relayer.Transports;
using Relayer.Pipeline;
namespace Relayer;

internal interface IRequestRegistration
{
    void Validate(RelayerConfiguration configuration, MessageTypeRegistry messages, List<string> failures);
}

internal sealed class RequestRegistration<TRequest, TResponse>(RequestReplies<TRequest, TResponse> replies) : IRequestRegistration
{
    public void Validate(RelayerConfiguration configuration, MessageTypeRegistry messages, List<string> failures)
    {
        if (!messages.TryGetMessageType(typeof(TResponse), out _))
            failures.Add($"Request replies on '{replies.Address}': register the stable message identity for '{typeof(TResponse)}'.");
        foreach (var transport in configuration.Transports)
        {
            if (transport.Name != replies.Transport) continue;
            foreach (var route in transport.Routes)
            {
                if (route.MessageType != typeof(TRequest) || route is not ISendRouteDefinition send) continue;
                if (route.Destination == replies.Address)
                    failures.Add($"Request route to '{route.Destination}': use a dedicated reply endpoint distinct from the request destination.");
                if (send.SendOptions.EnvelopeMode != MessageEnvelopeMode.CloudEvents || send.SendOptions.TransactionRequirement == SendTransactionRequirement.Outbox)
                    failures.Add($"Request route to '{route.Destination}': use CloudEvents and an immediate send route without an outbox transaction requirement.");
            }
            var replyConsumers = 0;
            foreach (var consumer in transport.Consumers)
            {
                if (consumer.Source != replies.Address) continue;
                replyConsumers++;
                if (consumer.Handlers.Count != 1)
                    failures.Add($"Request replies on '{replies.Address}': use a dedicated reply consumer without additional handlers.");
                if (consumer.BatchSize != 1)
                    failures.Add($"Request replies on '{replies.Address}': batch replies are unsupported. Use a single-message reply consumer.");
            }
            if (replyConsumers != 1)
                failures.Add($"Request replies on '{replies.Address}': declare exactly one dedicated reply consumer on this endpoint.");
        }
    }
}
