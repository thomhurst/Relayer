using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Relayer.Serialization;

namespace Relayer;

internal sealed class MessageSerializerStartupValidator(IServiceProvider services, RelayerConfigurationValidator validator) : IHostedLifecycleService
{
    private bool _initialized;

    public Task StartingAsync(CancellationToken cancellationToken) => StartAsync(cancellationToken);

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        if (_initialized) return;
        // StartingAsync callbacks may run concurrently. Do not cache conservative capabilities while native evidence is pending.
        try { await validator.InitializeCapabilitiesAsync(cancellationToken).ConfigureAwait(false); }
        catch (Exception)
        {
            // TopologyStartupService owns reporting this shared failure. Reporting it twice changes a concurrent
            // startup's OptionsValidationException into an AggregateException and hides the actionable diagnostic.
            return;
        }
        var transports = services.GetRequiredService<RelayerConfiguration>().Transports;
        var serializers = new HashSet<IMessageSerializer>(ReferenceEqualityComparer.Instance);
        if (transports.Count == 0) serializers.Add(services.GetRequiredService<IMessageSerializer>());
        foreach (var transport in transports) serializers.Add(services.GetMessageSerializer(transport.Name));
        foreach (var serializer in serializers)
            if (serializer is IMessageSerializerInitializer initializer)
                await initializer.InitializeAsync(cancellationToken).ConfigureAwait(false);
        // Declaration-only hosts remain usable without adapters. A runtime adapter composes all send routes at startup.
        if (services.GetServices<Transports.ITransport>().Any())
        {
            _ = services.GetRequiredService<SendPipeline>();
        }
        _initialized = true;
    }

    public Task StartedAsync(CancellationToken cancellationToken) => Task.CompletedTask;
    public Task StoppingAsync(CancellationToken cancellationToken) => Task.CompletedTask;
    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
    public Task StoppedAsync(CancellationToken cancellationToken) => Task.CompletedTask;
}
