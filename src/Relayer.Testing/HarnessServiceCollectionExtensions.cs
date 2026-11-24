using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Relayer.Pipeline;
using Relayer.Transports;

namespace Relayer.Testing;

/// <summary>Registers host-local test observation.</summary>
public static class HarnessServiceCollectionExtensions
{
    /// <summary>Adds a harness after AddRelayer and all transport registrations. Call once per service collection.</summary>
    public static IServiceCollection AddRelayerHarness(this IServiceCollection services)
    {
        ArgumentNullException.ThrowIfNull(services);

        if (!services.Any(item => item.ServiceType == typeof(IRelayerBuilder)))
        {
            throw new InvalidOperationException("Call AddRelayerHarness after AddRelayer and all transport registrations.");
        }

        if (services.Any(item => item.ServiceType == typeof(RelayerHarness)))
        {
            throw new InvalidOperationException("AddRelayerHarness has already been called.");
        }

        if (services.Any(item => item.ServiceType == typeof(ITransport) && !item.IsKeyedService && item.Lifetime != ServiceLifetime.Singleton))
        {
            throw new InvalidOperationException("Harness observation requires singleton ITransport registrations.");
        }

        RelayerInstrumentation? callerOwnedInstrumentation = null;

        // The provider resolves the last unkeyed registration for a single service.
        for (int i = services.Count - 1; i >= 0; i--)
        {
            var original = services[i];

            if (original.ServiceType != typeof(RelayerInstrumentation) || original.IsKeyedService)
            {
                continue;
            }

            if (original.ImplementationInstance is RelayerInstrumentation instance)
            {
                // A constant registration keeps disposal with the caller.
                callerOwnedInstrumentation = instance;
                break;
            }

            // Outbox stores report committed and settled rows through the provider's instrumentation.
            services[i] = ServiceDescriptor.Singleton(provider =>
            {
                var instrumentation = (RelayerInstrumentation)(original.ImplementationFactory?.Invoke(provider)
                    ?? ActivatorUtilities.CreateInstance(provider, original.ImplementationType!));
                instrumentation.Observer = provider.GetRequiredService<RelayerHarness>();
                return instrumentation;
            });
            break;
        }

        if (callerOwnedInstrumentation is null)
        {
            services.AddSingleton(CreateHarness);
        }
        else
        {
            services.AddSingleton(provider => new InstrumentationObservation(CreateHarness(provider), callerOwnedInstrumentation));
            services.AddSingleton(static provider => provider.GetRequiredService<InstrumentationObservation>().Harness);
            services.Insert(0, ServiceDescriptor.Singleton<IHostedService>(static provider =>
            {
                // The host resolves this service before running any startup callbacks.
                _ = provider.GetRequiredService<RelayerHarness>();
                return new HarnessStartup();
            }));
        }

        services.AddSingleton<IRelayerObserver>(static provider => provider.GetRequiredService<RelayerHarness>());
        services.Insert(0, ServiceDescriptor.Singleton<ISendMiddleware>(static provider => provider.GetRequiredService<RelayerHarness>()));

        int registrationCount = services.Count;

        for (var i = 0; i < registrationCount; i++)
        {
            var original = services[i];

            if (original.ServiceType != typeof(ITransport) || original.IsKeyedService)
            {
                continue;
            }

            // Let DI track the original instance for disposal. Use a separate service type
            // so keyed transport enumeration does not expose the undecorated transport.
            var transportKey = new object();
            ServiceDescriptor innerRegistration;

            if (original.ImplementationType is { } implementationType)
            {
                // Keep the implementation type visible to DI for constructor selection and ValidateOnBuild.
                innerRegistration = ServiceDescriptor.KeyedSingleton(typeof(IAsyncDisposable), transportKey, implementationType);
            }
            else if (original.ImplementationFactory is { } factory)
            {
                innerRegistration = ServiceDescriptor.KeyedSingleton<IAsyncDisposable>(transportKey, (provider, _) => (ITransport)factory(provider));
            }
            else
            {
                innerRegistration = ServiceDescriptor.KeyedSingleton<IAsyncDisposable>(transportKey, (ITransport)original.ImplementationInstance!);
            }

            services.Add(innerRegistration);
            services[i] = ServiceDescriptor.Singleton<ITransport>(provider =>
            {
                var transport = (ITransport)provider.GetRequiredKeyedService<IAsyncDisposable>(transportKey);
                return new HarnessTransport(transport, provider.GetRequiredService<RelayerHarness>(),
                    provider.GetRequiredService<RelayerConfiguration>());
            });
        }

        return services;
    }

    private static RelayerHarness CreateHarness(IServiceProvider provider)
    {
        return new(provider, provider.GetRequiredService<MessageTypeRegistry>());
    }

    private sealed class HarnessStartup : IHostedService
    {
        public Task StartAsync(CancellationToken cancellationToken)
        {
            return Task.CompletedTask;
        }

        public Task StopAsync(CancellationToken cancellationToken)
        {
            return Task.CompletedTask;
        }
    }

    private sealed class InstrumentationObservation : IDisposable
    {
        private readonly RelayerInstrumentation _instrumentation;

        internal InstrumentationObservation(RelayerHarness harness, RelayerInstrumentation instrumentation)
        {
            Harness = harness;
            _instrumentation = instrumentation;

            if (!instrumentation.TryAttachObserver(harness))
            {
                throw new InvalidOperationException(
                    "This RelayerInstrumentation is already observed by another harness. Use a separate instance for each active provider.");
            }
        }

        internal RelayerHarness Harness
        {
            get;
        }

        public void Dispose()
        {
            _instrumentation.DetachObserver(Harness);
        }
    }
}

internal sealed class HarnessTransport : ITransport, ITransportSendTracing, ITransportWrapper, ITransportSendTargetWrapper, ITransportFailureCopyObserver
{
    private readonly ITransport _inner;
    private readonly RelayerHarness _harness;
    private readonly Dictionary<string, List<HeaderNames>> _names;
    private readonly List<HeaderNames> _defaultNames;

    internal HarnessTransport(ITransport inner, RelayerHarness harness, RelayerConfiguration configuration)
    {
        _inner = inner;
        _harness = harness;
        _names = new(StringComparer.Ordinal);
        _defaultNames = [new()];

        foreach (var transport in configuration.Transports)
        {
            if (transport.Name == inner.Name)
            {
                foreach (var route in transport.Routes)
                {
                    if (!_names.TryGetValue(route.Destination, out var names))
                    {
                        _names.Add(route.Destination, names = []);
                    }

                    var routeNames = ((ISendRouteDefinition)route).SendOptions.HeaderNames;

                    if (!names.Any(existing => existing.Id == routeNames.Id))
                    {
                        names.Add(routeNames);
                    }
                }
            }
        }
    }

    private HarnessTransport(ITransport target, HarnessTransport original)
    {
        _inner = target;
        _harness = original._harness;
        _names = original._names;
        _defaultNames = original._defaultNames;
    }

    public string Name => _inner.Name;

    public ITransport InnerTransport => _inner;

    public ITransport WrapSendTarget(ITransport target)
    {
        return new HarnessTransport(target, this);
    }

    public SendTracing? GetSendTracing(string destination)
    {
        return (_inner as ITransportSendTracing)?.GetSendTracing(destination);
    }

    public TransportCapabilities GetCapabilities(string endpoint)
    {
        return _inner.GetCapabilities(endpoint);
    }

    public ValueTask<TopologyReport> ApplyTopologyAsync(TopologyMode mode, CancellationToken cancellationToken)
    {
        return _inner.ApplyTopologyAsync(mode, cancellationToken);
    }

    public Task StartAsync(CancellationToken cancellationToken)
    {
        return _inner.StartAsync(cancellationToken);
    }

    public Task StopAsync(CancellationToken cancellationToken)
    {
        return _inner.StopAsync(cancellationToken);
    }

    public ValueTask DisposeAsync()
    {
        return default;
    }

    public ValueTask SendAsync(OutgoingMessage message, CancellationToken cancellationToken)
    {
        return SendAsync(message, null, cancellationToken);
    }

    ValueTask ITransportFailureCopyObserver.SendCopyAsync(OutgoingMessage message, HeaderNames sourceNames, CancellationToken cancellationToken)
    {
        return SendAsync(message, sourceNames, cancellationToken);
    }

    private async ValueTask SendAsync(OutgoingMessage message, HeaderNames? sourceNames, CancellationToken cancellationToken)
    {
        _harness.WorkStarted();

        try
        {
            var snapshot = Snapshot(message, sourceNames);
            _harness.Remember(snapshot);

            try
            {
                await _inner.SendAsync(message, cancellationToken).ConfigureAwait(false);
                _harness.RecordDispatch(snapshot);
            }
            catch (Exception error)
            {
                _harness.RecordFailure(snapshot, error, HarnessOperation.Dispatch);
                throw;
            }
        }
        finally
        {
            _harness.WorkCompleted();
        }
    }

    public async ValueTask SendBatchAsync(ReadOnlyMemory<OutgoingMessage> messages, CancellationToken cancellationToken)
    {
        _harness.WorkStarted();

        try
        {
            var snapshots = new HarnessMessage[messages.Length];

            for (var i = 0; i < snapshots.Length; i++)
            {
                snapshots[i] = Snapshot(messages.Span[i]);
                _harness.Remember(snapshots[i]);
            }

            try
            {
                await _inner.SendBatchAsync(messages, cancellationToken).ConfigureAwait(false);

                foreach (var snapshot in snapshots)
                {
                    _harness.RecordDispatch(snapshot);
                }
            }
            catch (Exception error)
            {
                foreach (var snapshot in snapshots)
                {
                    _harness.RecordFailure(snapshot, error, HarnessOperation.Dispatch);
                }

                throw;
            }
        }
        finally
        {
            _harness.WorkCompleted();
        }
    }

    private HarnessMessage Snapshot(OutgoingMessage message, HeaderNames? sourceNames = null)
    {
        var (id, type) = sourceNames is null ? default : ReadIdentity(message.Headers.Span, sourceNames);

        if (id is null || type is null)
        {
            foreach (var names in _names.GetValueOrDefault(message.Destination, _defaultNames))
            {
                var (candidateId, candidateType) = ReadIdentity(message.Headers.Span, names);

                // Keep each binding's ID and type together, preferring a complete envelope.
                if (candidateId is not null && candidateType is not null)
                {
                    (id, type) = (candidateId, candidateType);
                    break;
                }

                if (id is null && type is null)
                {
                    (id, type) = (candidateId, candidateType);
                }
            }
        }

        return new(id, type, message.Body.ToArray(), transportName: Name, destination: message.Destination,
            isTombstone: message.IsTombstone, tombstoneKey: message.IsTombstone ? message.OrderingKey : null);
    }

    private static (string? Id, string? Type) ReadIdentity(ReadOnlySpan<KeyValuePair<string, WireString>> headers, HeaderNames names)
    {
        string? id = null;
        string? type = null;

        foreach (var header in headers)
        {
            if (header.Key == names.Id)
            {
                id = header.Value.ToString();
            }

            if (header.Key == names.Type)
            {
                type = header.Value.ToString();
            }
        }

        return (id, type);
    }
}
