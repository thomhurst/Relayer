using System.Collections.Frozen;
using Microsoft.Extensions.Options;
using Relayer.Serialization;
using Relayer.Transports;

namespace Relayer;

internal static class SendRouteKey<TMessage>
{
    // Identity only; all configuration and mutable state belong to the service provider.
    internal static readonly object Value = new();
}

internal abstract class SendRouteRegistration
{
    internal abstract object Key { get; }
    internal abstract object Build(MessageTypeRegistry types, Dictionary<string, ITransport> transports,
        IMessageSerializer serializer, SendDelegate pipeline, EndpointCapabilityCache capabilitiesCache, OutboxCapabilities outbox);
}

internal interface ISendRouteDefinition
{
    SendRouteOptions SendOptions { get; }
    SendRouteRegistration CreateSendRegistration(string transportName);
}

internal sealed class SendRouteRegistration<TMessage>(string transportName, string destination, SendRouteOptions options,
    IOrderingKeySelector<TMessage>? orderingKey)
    : SendRouteRegistration
{
    internal override object Key => SendRouteKey<TMessage>.Value;

    internal override object Build(MessageTypeRegistry types, Dictionary<string, ITransport> transports,
        IMessageSerializer serializer, SendDelegate pipeline, EndpointCapabilityCache capabilitiesCache, OutboxCapabilities outbox)
    {
        if (!types.TryGetMessageType(typeof(TMessage), out var messageType))
        {
            throw new InvalidOperationException($"Register the message identity for '{typeof(TMessage)}' before routing it.");
        }
        if (!transports.TryGetValue(transportName, out var transport))
        {
            throw new InvalidOperationException($"Send route '{messageType}' references unknown transport '{transportName}'. Register a named ITransport service.");
        }
        var failures = new List<string>();
        options.Validate($"Send route '{messageType}'", failures);
        var capabilities = capabilitiesCache.GetCapabilities(transport, destination);
        SendCapabilityValidator.Validate(options, capabilities, orderingKey is not null, outbox,
            $"Send route '{messageType}' on transport '{transportName}' to endpoint '{destination}'", failures);
        if (orderingKey is not null && !capabilities.OrderedByKey)
        {
            failures.Add($"Send route '{messageType}' on transport '{transportName}' to endpoint '{destination}' declares an OrderingKey, but the endpoint does not support OrderedByKey. Configure an endpoint with native ordering or remove the OrderingKey declaration.");
        }
        if (failures.Count != 0) throw new OptionsValidationException(Options.DefaultName, typeof(SendRouteOptions), failures);
        return new SendRoute<TMessage>(transport, destination, messageType, options, orderingKey, serializer, pipeline, capabilities);
    }
}

internal sealed class SendPipeline
{
    private readonly FrozenDictionary<object, object> _routes;

    public SendPipeline(MessageTypeRegistry types, RelayerConfiguration configuration,
        IEnumerable<ITransport> transports, IEnumerable<ISendMiddleware> middleware, IMessageSerializer serializer, EndpointCapabilityCache capabilitiesCache, OutboxCapabilities outbox)
    {
        var namedTransports = new Dictionary<string, ITransport>(StringComparer.Ordinal);
        foreach (var transport in transports)
        {
            if (!namedTransports.TryAdd(transport.Name, transport))
            {
                throw new InvalidOperationException($"Transport name '{transport.Name}' is registered more than once.");
            }
        }

        SendDelegate pipeline = static context => ((ISendOperation)context).DispatchAsync();
        var steps = middleware.ToArray();
        for (var i = steps.Length - 1; i >= 0; i--)
        {
            var step = steps[i];
            var next = pipeline;
            pipeline = context => step.InvokeAsync(context, next);
        }

        var routes = new Dictionary<object, object>(ReferenceEqualityComparer.Instance);
        foreach (var transport in configuration.Transports)
        {
            foreach (var definition in transport.Routes)
            {
                var registration = ((ISendRouteDefinition)definition).CreateSendRegistration(transport.Name);
                if (!routes.TryAdd(registration.Key, registration.Build(types, namedTransports, serializer, pipeline, capabilitiesCache, outbox)))
                {
                    throw new InvalidOperationException("A message has more than one send route. Configure one destination per message type.");
                }
            }
        }
        _routes = routes.ToFrozenDictionary(ReferenceEqualityComparer.Instance);
    }

    internal SendRoute<TMessage> GetRoute<TMessage>() =>
        _routes.TryGetValue(SendRouteKey<TMessage>.Value, out var route)
            ? (SendRoute<TMessage>)route
            : throw new InvalidOperationException("No send route is configured for this message. Declare a Send<TMessage> route on its named transport inside AddRelayer.");
}

internal interface ISendOperation
{
    string TransportName { get; }
    string Destination { get; }
    bool UsesOutbox { get; }
    ValueTask DispatchAsync();
}
