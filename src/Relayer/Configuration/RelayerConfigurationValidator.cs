using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Relayer.Pipeline;
using Relayer.Transports;

namespace Relayer;

/// <summary>Reports all invalid Relayer declarations together before consumers start.</summary>
internal sealed class RelayerConfigurationValidator(RelayerConfiguration configuration, MessageTypeRegistry messages,
    IServiceProvider services)
{
    private readonly object _initialization = new();
    private Task? _startupValidation;
    private CancellationToken _startupToken;
    private bool _capabilitiesInitialized;

    internal Task InitializeCapabilitiesAsync(CancellationToken cancellationToken)
    {
        // The host passes the same token to its StartingAsync callbacks and creates a new token for each start.
        lock (_initialization)
        {
            if (_startupValidation is null ||
                (_startupValidation.IsCompleted && _startupToken != cancellationToken && !cancellationToken.IsCancellationRequested))
            {
                _startupToken = cancellationToken;
                _startupValidation = ValidateStartupAsync(cancellationToken);
            }

            return _startupValidation;
        }
    }

    private async Task ValidateStartupAsync(CancellationToken cancellationToken)
    {
        if (_capabilitiesInitialized)
        {
            // Topology can fail after capabilities initialize. Recheck reloaded options without rebuilding adapters.
            var failures = new List<string>();

            foreach (var transport in configuration.Transports)
            {
                transport.ValidateOptions(services, failures);
            }

            ThrowIfInvalid(failures);
            return;
        }

        await ValidateAsync(deferCapabilityInitialization: true).ConfigureAwait(false);
        var adapters = services.GetServices<ITransport>().ToDictionary(transport => transport.Name, StringComparer.Ordinal);

        foreach (var declaration in configuration.Transports)
        {
            if (adapters.TryGetValue(declaration.Name, out var transport) &&
                ITransportWrapper.Unwrap(transport) is ITransportCapabilityInitializer initializer)
            {
                await initializer.InitializeCapabilitiesAsync(declaration.TopologyMode ?? configuration.TopologyMode, cancellationToken).ConfigureAwait(false);
            }
        }

        await ValidateAsync().ConfigureAwait(false);
        _capabilitiesInitialized = true;
    }

    internal async ValueTask ValidateAsync(bool deferCapabilityInitialization = false)
    {
        var failures = new List<string>();

        if (!Enum.IsDefined(configuration.TopologyMode))
        {
            failures.Add("TopologyMode is not a defined value.");
        }

        var names = new HashSet<string>(StringComparer.Ordinal);
        var routes = new HashSet<(Type, string, string)>();

        foreach (var transport in configuration.Transports)
        {
            if (transport.TopologyMode is { } mode && !Enum.IsDefined(mode))
            {
                failures.Add($"Transport '{transport.Name}': TopologyMode is not a defined value.");
            }

            if (string.IsNullOrWhiteSpace(transport.Name))
            {
                failures.Add("A transport name must not be blank.");
            }
            else if (!names.Add(transport.Name))
            {
                failures.Add($"Transport '{transport.Name}' is declared more than once.");
            }

            transport.ValidateOptions(services, failures);

            foreach (var route in transport.Routes)
            {
                var location = $"Route for '{route.MessageType}' to '{route.Destination}' on transport '{transport.Name}'";

                if (string.IsNullOrWhiteSpace(route.Destination))
                {
                    failures.Add($"{location}: destination must not be blank.");
                }

                if (!routes.Add(transport.GetRouteKey(route)))
                {
                    failures.Add($"{location}: a message type can have only one route to each destination; '{route.Destination}' is declared more than once.");
                }

                if (!messages.TryGetMessageType(route.MessageType, out _))
                {
                    failures.Add($"{location}: register a stable message identity before startup.");
                }

                if (route is ISendRouteDefinition send)
                {
                    send.SendOptions.Validate(location, failures);

                    if (SendCapabilityValidator.GetNativeCallbackError(send, transport.OptionsType) is { } callbackError)
                    {
                        failures.Add($"{location}: {callbackError}");
                    }
                }
            }

            foreach (var consumer in transport.Consumers)
            {
                var location = $"Consumer '{consumer.Source}' on transport '{transport.Name}'";

                if (string.IsNullOrWhiteSpace(consumer.Source))
                {
                    failures.Add($"{location}: source must not be blank.");
                }

                if (consumer.Handlers.Count == 0)
                {
                    failures.Add($"{location}: declare at least one handler.");
                }

                if (consumer.Concurrency < 1)
                {
                    failures.Add($"{location}: concurrency must be at least 1.");
                }

                if (consumer.BatchSize < 1)
                {
                    failures.Add($"{location}: batch size must be at least 1.");
                }

                if (consumer.BatchWait <= TimeSpan.Zero)
                {
                    failures.Add($"{location}: batch wait must be positive.");
                }
            }
        }

        foreach (var registration in services.GetServices<IRequestRegistration>())
        {
            registration.Validate(configuration, messages, failures);
        }

        try
        {
            var capabilities = new EndpointCapabilityValidator(ResolveAdapters(failures), failures,
                services.GetRequiredService<EndpointCapabilityCache>(), services.GetRequiredService<OutboxCapabilities>());

            foreach (var transport in configuration.Transports)
            {
                await capabilities.ValidateAsync(transport, deferCapabilityInitialization).ConfigureAwait(false);
            }
        }
        catch (OptionsValidationException error)
        {
            // Native adapter factories can consume named options. Preserve declaration errors if their construction fails.
            failures.Add($"ITransport adapter construction: {error.Message}");
        }

        ThrowIfInvalid(failures);
    }

    private IEnumerable<ITransport> ResolveAdapters(List<string> failures)
    {
        try
        {
            return services.GetServices<ITransport>();
        }
        catch (Exception error) when (failures.Count != 0 &&
            error is ArgumentException or (InvalidOperationException and not ObjectDisposedException))
        {
            // An adapter can throw while consuming an invalid declaration or malformed bound options.
            failures.Add($"ITransport adapter construction: {error.Message}");
            return [];
        }
    }

    private static void ThrowIfInvalid(List<string> failures)
    {
        if (failures.Count != 0)
        {
            throw new OptionsValidationException(Options.DefaultName, typeof(RelayerConfiguration), failures);
        }
    }
}
