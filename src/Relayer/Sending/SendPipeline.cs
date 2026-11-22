using System.Collections.Frozen;
using Microsoft.Extensions.Options;
using Relayer.Pipeline;
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
    internal abstract object Key
    {
        get;
    }

    internal abstract object TombstoneKey
    {
        get;
    }

    internal abstract object? BuildTombstone(object route);

    internal abstract object CreateTarget(List<object> routes);

    internal abstract object CreateTombstoneTarget(List<object> routes);

    internal abstract object Build(MessageTypeRegistry types, Dictionary<string, ITransport> transports,
        IMessageSerializer serializer, SendDelegate pipeline, EndpointCapabilityCache capabilitiesCache, OutboxCapabilities outbox);
}

internal interface ISendRouteDefinition
{
    SendRouteOptions SendOptions
    {
        get;
    }

    /// <summary>A route-level native callback the transport applies to every send on this route, including outbox dispatch.</summary>
    Delegate? NativeConfigurator
    {
        get;
    }

    /// <summary>The transport options type whose transport understands <see cref="NativeConfigurator"/>.</summary>
    Type? NativeTransportOptionsType
    {
        get;
    }

    /// <summary>Whether this declaration is a further destination of a route that another declaration registers.</summary>
    bool IsAdditionalDestination
    {
        get;
    }

    bool HasDestinationSelector
    {
        get;
    }

    SendRouteRegistration? CreateSendRegistration(string transportName);
}

internal sealed class SendRouteRegistration<TMessage>(string transportName, string[] destinations, Func<TMessage, string>? selector,
    SendRouteOptions options, IOrderingKeySelector<TMessage>? orderingKey)
    : SendRouteRegistration
{
    internal override object Key => SendRouteKey<TMessage>.Value;

    internal override object TombstoneKey => SendRouteKey<Tombstone<TMessage>>.Value;

    internal override object? BuildTombstone(object route)
    {
        return ((SendRoute<TMessage>)route).CreateTombstoneRoute();
    }

    internal override object CreateTarget(List<object> routes)
    {
        return CreateTarget<TMessage>(routes);
    }

    internal override object CreateTombstoneTarget(List<object> routes)
    {
        return CreateTarget<Tombstone<TMessage>>(routes);
    }

    private static SendTarget<T> CreateTarget<T>(List<object> routes)
    {
        if (routes.Count == 1)
        {
            return (SendRoute<T>)routes[0];
        }

        var array = new SendRoute<T>[routes.Count];

        for (var i = 0; i < array.Length; i++)
        {
            array[i] = (SendRoute<T>)routes[i];
        }

        return new FanOutSendTarget<T>(array);
    }

    internal override object Build(MessageTypeRegistry types, Dictionary<string, ITransport> transports,
        IMessageSerializer serializer, SendDelegate pipeline, EndpointCapabilityCache capabilitiesCache, OutboxCapabilities outbox)
    {
        if (!types.TryGetMessageType(typeof(TMessage), out var messageType))
        {
            throw new InvalidOperationException($"Register the message identity for '{typeof(TMessage)}' before routing it.");
        }

        if (types.TryGetClrType(messageType, out var originalType) && originalType != typeof(TMessage))
        {
            throw new InvalidOperationException($"Configure the send route for '{originalType}'. Typed tombstones automatically reuse the original message's route; do not declare Send<Tombstone<T>>.");
        }

        if (!transports.TryGetValue(transportName, out var transport))
        {
            throw new InvalidOperationException($"Send route '{messageType}' references unknown transport '{transportName}'. Register a named ITransport service.");
        }

        var failures = new List<string>();
        options.Validate($"Send route '{messageType}'", failures);
        var capabilities = new TransportCapabilities[destinations.Length];

        for (var i = 0; i < destinations.Length; i++)
        {
            var destination = destinations[i];
            var capability = capabilitiesCache.GetCapabilities(transport, destination);
            capabilities[i] = capability;
            var executor = SendCapabilityValidator.NeedsOutbox(options, capability) ? outbox.Get() : null;
            SendCapabilityValidator.Validate(options, capability, orderingKey is not null, executor,
                $"Send route '{messageType}' on transport '{transportName}' to endpoint '{destination}'", failures);

            if (orderingKey is not null && !capabilities[i].OrderedByKey)
            {
                failures.Add($"Send route '{messageType}' on transport '{transportName}' to endpoint '{destination}' declares an OrderingKey, but the endpoint does not support OrderedByKey. Configure an endpoint with native ordering or remove the OrderingKey declaration.");
            }
        }

        if (failures.Count != 0)
        {
            throw new OptionsValidationException(Options.DefaultName, typeof(SendRouteOptions), failures);
        }

        var routes = new SendRoute<TMessage>[destinations.Length];

        for (var i = 0; i < routes.Length; i++)
        {
            routes[i] = new SendRoute<TMessage>(transport, destinations[i], messageType, options, orderingKey, serializer, pipeline, capabilities[i]);
        }

        if (selector is not null)
        {
            routes[0].RouteBy(selector, routes);
        }

        return routes[0];
    }
}

internal sealed class SendPipeline
{
    private readonly FrozenDictionary<object, object> _routes;

    public SendPipeline(MessageTypeRegistry types, RelayerConfiguration configuration,
        IEnumerable<ITransport> transports, IEnumerable<ISendMiddleware> middleware, IServiceProvider services, EndpointCapabilityCache capabilitiesCache, OutboxCapabilities outbox)
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

        // Several Send<T> declarations fan one message out. Reject a repeated destination here too, because a
        // sender resolved without the hosted startup validator would otherwise enlist duplicate rows.
        var declared = new Dictionary<object, (SendRouteRegistration Registration, List<object> Routes, bool Tombstone)>(ReferenceEqualityComparer.Instance);
        var destinations = new HashSet<(Type, string, string)>();

        foreach (var transport in configuration.Transports)
        {
            if (transport.Routes.Count == 0)
            {
                continue;
            }

            var serializer = services.GetMessageSerializer(transport.Name);

            foreach (var definition in transport.Routes)
            {
                if (!destinations.Add((definition.MessageType, transport.Name, definition.Destination)))
                {
                    throw new InvalidOperationException($"Route for '{definition.MessageType}' on transport '{transport.Name}': a message type can have only one route to each destination; '{definition.Destination}' is declared more than once.");
                }

                var send = (ISendRouteDefinition)definition;

                if (SendCapabilityValidator.GetNativeCallbackError(send, transport.OptionsType) is { } callbackError)
                {
                    throw new OptionsValidationException(Options.DefaultName, typeof(SendRouteOptions),
                        [$"Route for '{definition.MessageType}' on transport '{transport.Name}': {callbackError}"]);
                }

                if (send.CreateSendRegistration(transport.Name) is not { } registration)
                {
                    continue;
                }

                var route = registration.Build(types, namedTransports, serializer, pipeline, capabilitiesCache, outbox);
                Add(declared, registration, registration.Key, route, tombstone: false);

                if (registration.BuildTombstone(route) is { } tombstone)
                {
                    Add(declared, registration, registration.TombstoneKey, tombstone, tombstone: true);
                }
            }
        }

        var routes = new Dictionary<object, object>(declared.Count, ReferenceEqualityComparer.Instance);

        foreach (var (key, entry) in declared)
        {
            routes.Add(key, entry.Tombstone ? entry.Registration.CreateTombstoneTarget(entry.Routes) : entry.Registration.CreateTarget(entry.Routes));
        }

        _routes = routes.ToFrozenDictionary(ReferenceEqualityComparer.Instance);
    }

    private static void Add(Dictionary<object, (SendRouteRegistration, List<object>, bool)> declared,
        SendRouteRegistration registration, object key, object route, bool tombstone)
    {
        if (declared.TryGetValue(key, out var entry))
        {
            entry.Item2.Add(route);
        }
        else
        {
            declared.Add(key, (registration, [route], tombstone));
        }
    }

    internal SendTarget<TMessage> GetTarget<TMessage>()
    {
        return _routes.TryGetValue(SendRouteKey<TMessage>.Value, out var route)
                    ? (SendTarget<TMessage>)route
                    : throw new InvalidOperationException("No send route is configured for this message. Declare a Send<TMessage> route on its named transport inside AddRelayer.");
    }

    /// <summary>Requests and replies correlate with one native destination, so they need exactly one route.</summary>
    internal SendRoute<TMessage> GetRoute<TMessage>()
    {
        return GetTarget<TMessage>() as SendRoute<TMessage>
                ?? throw new InvalidOperationException("Requests and replies need exactly one send route. Remove the extra Send<TMessage> declarations for this message type.");
    }
}

internal interface ISendOperation
{
    string TransportName
    {
        get;
    }

    string Destination
    {
        get;
    }

    bool UsesOutbox
    {
        get;
    }

    ValueTask DispatchAsync();
}
