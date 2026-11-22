using Microsoft.Extensions.DependencyInjection;
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

        services.AddSingleton(static provider => new RelayerHarness(provider, provider.GetRequiredService<MessageTypeRegistry>()));

        for (var i = 0; i < services.Count; i++)
        {
            var original = services[i];

            if (original.ServiceType != typeof(RelayerInstrumentation) || original.IsKeyedService)
            {
                continue;
            }
            // Outbox stores report committed and settled rows through the provider's instrumentation.
            services[i] = ServiceDescriptor.Singleton(provider =>
            {
                var instrumentation = (RelayerInstrumentation)(original.ImplementationInstance
                    ?? original.ImplementationFactory?.Invoke(provider)
                    ?? ActivatorUtilities.CreateInstance(provider, original.ImplementationType!));
                instrumentation.Observer = provider.GetRequiredService<RelayerHarness>();
                return instrumentation;
            });
        }

        services.AddSingleton<IRelayerObserver>(static provider => provider.GetRequiredService<RelayerHarness>());
        services.Insert(0, ServiceDescriptor.Singleton<ISendMiddleware>(static provider => provider.GetRequiredService<RelayerHarness>()));

        for (var i = 0; i < services.Count; i++)
        {
            var original = services[i];

            if (original.ServiceType != typeof(ITransport) || original.IsKeyedService)
            {
                continue;
            }

            services[i] = ServiceDescriptor.Singleton<ITransport>(provider =>
            {
                var transport = (ITransport)(original.ImplementationInstance ?? original.ImplementationFactory?.Invoke(provider)
                    ?? ActivatorUtilities.CreateInstance(provider, original.ImplementationType!));
                return new HarnessTransport(transport, provider.GetRequiredService<RelayerHarness>(),
                    provider.GetRequiredService<RelayerConfiguration>(), original.ImplementationInstance is null);
            });
        }

        return services;
    }
}

internal sealed class HarnessTransport : ITransport, ITransportSendTracing, ITransportWrapper
{
    private readonly ITransport _inner;
    private readonly RelayerHarness _harness;
    private readonly bool _ownsTransport;
    private readonly Dictionary<string, List<HeaderNames>> _names;
    private readonly List<HeaderNames> _defaultNames;

    internal HarnessTransport(ITransport inner, RelayerHarness harness, RelayerConfiguration configuration, bool ownsTransport)
    {
        _inner = inner;
        _harness = harness;
        _ownsTransport = ownsTransport;
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
        _ownsTransport = false;
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
        return _ownsTransport ? _inner.DisposeAsync() : default;
    }

    public async ValueTask SendAsync(OutgoingMessage message, CancellationToken cancellationToken)
    {
        _harness.WorkStarted();

        try
        {
            var snapshot = Snapshot(message);
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

    private HarnessMessage Snapshot(OutgoingMessage message)
    {
        var candidates = _names.GetValueOrDefault(message.Destination, _defaultNames);
        string? id = null;
        string? type = null;

        foreach (var names in candidates)
        {
            string? candidateId = null;
            string? candidateType = null;

            foreach (var header in message.Headers.Span)
            {
                if (header.Key == names.Id)
                {
                    candidateId = header.Value.ToString();
                }

                if (header.Key == names.Type)
                {
                    candidateType = header.Value.ToString();
                }
            }
            // Keep each binding's ID and type together, preferring a complete envelope.
            if ((id is null && type is null) || (candidateId is not null && candidateType is not null))
            {
                id = candidateId;
                type = candidateType;
            }

            if (candidateId is not null && candidateType is not null)
            {
                break;
            }
        }

        return new(id, type, message.Body.ToArray(), transportName: Name, destination: message.Destination,
            isTombstone: message.IsTombstone, tombstoneKey: message.IsTombstone ? message.OrderingKey : null);
    }
}
