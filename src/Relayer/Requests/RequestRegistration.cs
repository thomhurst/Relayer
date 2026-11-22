using Relayer.Pipeline;
using Relayer.Transports;

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
        {
            failures.Add($"Request replies on '{replies.Address}': register the stable message identity for '{typeof(TResponse)}'.");
        }

        var requestRoutes = 0;
        var responseRoutes = 0;
        ISendRouteDefinition? localResponseRoute = null;

        foreach (var transport in configuration.Transports)
        {
            foreach (var route in transport.Routes)
            {
                if (route.MessageType == typeof(TRequest))
                {
                    requestRoutes++;
                }

                if (route.MessageType != typeof(TResponse) || route is not ISendRouteDefinition send)
                {
                    continue;
                }

                if (!send.IsAdditionalDestination)
                {
                    responseRoutes++;
                }

                if (transport.Name == replies.Transport && route.Destination == replies.Address)
                {
                    localResponseRoute = send;
                }
            }
        }

        if (requestRoutes > 1)
        {
            failures.Add($"Request '{typeof(TRequest)}': requests need exactly one send route, but {requestRoutes} are declared. Remove the extra Send<TRequest> declarations.");
        }

        if (localResponseRoute is not null)
        {
            var location = $"Request replies on '{replies.Address}' on transport '{replies.Transport}'";

            if (responseRoutes != 1)
            {
                failures.Add($"{location}: replies need exactly one send route for '{typeof(TResponse)}', but {responseRoutes} are declared. Remove the extra Send<TResponse> declarations.");
            }

            if (localResponseRoute.HasDestinationSelector)
            {
                failures.Add($"{location}: replies require a fixed destination. Declare Send<TResponse> with the reply address and without a destination selector.");
            }

            if (localResponseRoute.SendOptions.EnvelopeMode != MessageEnvelopeMode.CloudEvents)
            {
                failures.Add($"{location}: use a CloudEvents response route to preserve correlationid.");
            }
        }

        foreach (var transport in configuration.Transports)
        {
            if (transport.Name != replies.Transport)
            {
                continue;
            }

            foreach (var route in transport.Routes)
            {
                if (route.MessageType != typeof(TRequest) || route is not ISendRouteDefinition send)
                {
                    continue;
                }

                if (route.Destination == replies.Address)
                {
                    failures.Add($"Request route to '{route.Destination}': use a dedicated reply endpoint distinct from the request destination.");
                }

                if (send.SendOptions.EnvelopeMode != MessageEnvelopeMode.CloudEvents || send.SendOptions.TransactionRequirement == SendTransactionRequirement.Outbox)
                {
                    failures.Add($"Request route to '{route.Destination}': use CloudEvents and an immediate send route without an outbox transaction requirement.");
                }
            }

            var replyConsumers = 0;

            foreach (var consumer in transport.Consumers)
            {
                if (consumer.Source != replies.Address)
                {
                    continue;
                }

                replyConsumers++;

                if (consumer.Handlers.Count != 1)
                {
                    failures.Add($"Request replies on '{replies.Address}': use a dedicated reply consumer without additional handlers.");
                }

                if (consumer.BatchSize != 1)
                {
                    failures.Add($"Request replies on '{replies.Address}': batch replies are unsupported. Use a single-message reply consumer.");
                }
            }

            if (replyConsumers != 1)
            {
                failures.Add($"Request replies on '{replies.Address}': declare exactly one dedicated reply consumer on this endpoint.");
            }
        }
    }
}
