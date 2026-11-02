using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Relayer.Transports;

namespace Relayer;

/// <summary>Reports all invalid Relayer declarations together before consumers start.</summary>
internal sealed class RelayerConfigurationValidator(RelayerConfiguration configuration, MessageTypeRegistry messages,
    IServiceProvider services)
{
    public void Validate()
    {
        var failures = new List<string>();
        if (!Enum.IsDefined(configuration.TopologyMode)) failures.Add("TopologyMode is not a defined value.");
        var names = new HashSet<string>(StringComparer.Ordinal);
        var routes = new HashSet<Type>();
        foreach (var transport in configuration.Transports)
        {
            if (transport.TopologyMode is { } mode && !Enum.IsDefined(mode))
                failures.Add($"Transport '{transport.Name}': TopologyMode is not a defined value.");
            if (string.IsNullOrWhiteSpace(transport.Name)) failures.Add("A transport name must not be blank.");
            else if (!names.Add(transport.Name)) failures.Add($"Transport '{transport.Name}' is declared more than once.");
            transport.ValidateOptions(services, failures);
            foreach (var route in transport.Routes)
            {
                var location = $"Route for '{route.MessageType}' on transport '{transport.Name}'";
                if (string.IsNullOrWhiteSpace(route.Destination)) failures.Add($"{location}: destination must not be blank.");
                if (!routes.Add(route.MessageType)) failures.Add($"{location}: a message type can have only one route.");
                if (!messages.TryGetMessageType(route.MessageType, out _))
                    failures.Add($"{location}: register a stable message identity before startup.");
                if (route is ISendRouteDefinition send) send.SendOptions.Validate(location, failures);
            }
            foreach (var consumer in transport.Consumers)
            {
                var location = $"Consumer '{consumer.Source}' on transport '{transport.Name}'";
                if (string.IsNullOrWhiteSpace(consumer.Source)) failures.Add($"{location}: source must not be blank.");
                if (consumer.Handlers.Count == 0) failures.Add($"{location}: declare at least one handler.");
                if (consumer.Concurrency < 1) failures.Add($"{location}: concurrency must be at least 1.");
                if (consumer.BatchSize < 1) failures.Add($"{location}: batch size must be at least 1.");
                if (consumer.BatchWait <= TimeSpan.Zero) failures.Add($"{location}: batch wait must be positive.");
            }
        }
        foreach (var registration in services.GetServices<IRequestRegistration>())
            registration.Validate(configuration, messages, failures);
        try
        {
            var capabilities = new EndpointCapabilityValidator(services.GetServices<ITransport>(), failures, services.GetRequiredService<EndpointCapabilityCache>(), services.GetRequiredService<OutboxCapabilities>());
            foreach (var transport in configuration.Transports) capabilities.Validate(transport);
        }
        catch (OptionsValidationException error)
        {
            // Native adapter factories can consume named options. Preserve declaration errors if their construction fails.
            failures.Add($"ITransport adapter construction: {error.Message}");
        }
        if (failures.Count != 0)
            throw new OptionsValidationException(Options.DefaultName, typeof(RelayerConfiguration), failures);
    }
}
