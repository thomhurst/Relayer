using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Relayer.Pipeline;
using Relayer.Serialization;
using Relayer.Transports;

namespace Relayer;

/// <summary>Registers Relayer with dependency injection.</summary>
public static class RelayerServiceCollectionExtensions
{
    /// <summary>Adds Relayer and configures it. Call once per service collection.</summary>
    /// <exception cref="InvalidOperationException">Relayer has already been added.</exception>
    public static IServiceCollection AddRelayer(this IServiceCollection services, Action<IRelayerBuilder> configure)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configure);

        if (services.Any(d => d.ServiceType == typeof(IRelayerBuilder) || d.ServiceType == typeof(RegistrationInProgress)))
        {
            throw new InvalidOperationException("AddRelayer has already been called. Configure everything in a single call.");
        }

        var builder = new RelayerBuilder(services);
        builder.Telemetry();
        services.TryAddSingleton<RelayerInstrumentation>();
        var registration = ServiceDescriptor.Singleton(new RegistrationInProgress());
        services.Add(registration);

        try
        {
            configure(builder);
        }
        finally
        {
            services.Remove(registration);
        }

        builder.MessageTypes.Freeze();
        builder.Handlers.Freeze();
        builder.Upcasters.Freeze(builder.MessageTypes);

        services.TryAddSingleton<IMessageSerializer>(static provider =>
        {
            var registrations = provider.GetServices<JsonSerializerRegistration>().ToArray();
            var configuration = provider.GetRequiredService<RelayerConfiguration>();
            // Without declarations the default serializer serves standalone callers across the registry.
            HashSet<Type>? missingMetadata = configuration.Transports.Count == 0 ? null : [];
            var types = provider.GetRequiredService<MessageTypeRegistry>().CreateJsonTypes(registrations, missingMetadata);
            return new JsonMessageSerializer(types, missingMetadata);
        });
        services.TryAddEnumerable(ServiceDescriptor.Singleton<IHostedService, MessageSerializerStartupValidator>());
        services.TryAddSingleton<EndpointCapabilityCache>();
        services.TryAddSingleton<OutboxCapabilities>();
        services.TryAddSingleton<SendPipeline>();
        services.TryAddSingleton<SenderPool>();
        services.TryAddScoped<ISender>(static provider => provider.GetRequiredService<SenderPool>()
            .Rent(provider.GetService<IOutboxEnlistment>(), provider.GetService<TenantContext>()));

        services.AddSingleton(builder.MessageTypes);
        services.AddSingleton(builder.Handlers);
        services.AddSingleton(builder.Upcasters);
        services.AddSingleton<IRelayerBuilder>(builder);
        services.AddSingleton(new RelayerConfiguration(builder.TopologyMode, builder.Transports.ToArray()));
        services.AddSingleton<ITopology, Topology>();
        services.AddSingleton<RelayerConfigurationValidator>();
        // The host completes StartingAsync before any receiver's StartAsync, including concurrent startup.
        services.Insert(0, ServiceDescriptor.Singleton<IHostedService, TopologyStartupService>());

        return services;
    }

    private sealed class RegistrationInProgress;
}
