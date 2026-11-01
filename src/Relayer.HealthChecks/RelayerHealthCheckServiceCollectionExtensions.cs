using System.Globalization;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Options;

namespace Relayer.HealthChecks;

/// <summary>Registers the optional health integration without adding health dependencies to the core.</summary>
public static class RelayerHealthCheckServiceCollectionExtensions
{
    /// <summary>
    /// Adds transport connectivity, consumer liveness and outbox backlog checks once. Call AddRelayer too,
    /// in either order. Register native adapters as keyed services; missing adapters fail health.
    /// </summary>
    /// <param name="services">The application services.</param>
    /// <param name="options">Optional immutable defaults, also configurable through the Options pattern.</param>
    public static IHealthChecksBuilder AddRelayerHealthChecks(this IServiceCollection services,
        RelayerHealthCheckOptions? options = null) => AddChecks(services, options is null ? null : () => new()
        {
            ConsumerStallTimeout = options.ConsumerStallTimeout,
            OutboxDegradedThreshold = options.OutboxDegradedThreshold,
            OutboxUnhealthyThreshold = options.OutboxUnhealthyThreshold,
            OutboxNames = options.OutboxNames?.ToArray()!
        });

    /// <summary>Binds immutable options from configuration without reflection; validates at startup and on reload.</summary>
    public static IHealthChecksBuilder AddRelayerHealthChecks(this IServiceCollection services, IConfiguration configuration)
    {
        ArgumentNullException.ThrowIfNull(configuration);
        var builder = AddChecks(services, () => ReadConfiguration(configuration));
        services.AddSingleton<IOptionsChangeTokenSource<RelayerHealthCheckOptions>>(
            new ConfigurationChangeTokenSource<RelayerHealthCheckOptions>(configuration));
        return builder;
    }

    private static IHealthChecksBuilder AddChecks(IServiceCollection services, Func<RelayerHealthCheckOptions>? createOptions)
    {
        ArgumentNullException.ThrowIfNull(services);
        if (services.Any(static descriptor => descriptor.ServiceType == typeof(HealthRegistration)))
        {
            if (createOptions is not null) throw new InvalidOperationException("Relayer health checks are already configured.");
            return services.AddHealthChecks();
        }
        services.AddSingleton(new HealthRegistration());
        if (createOptions is not null)
        {
            services.AddSingleton<IOptionsFactory<RelayerHealthCheckOptions>>(provider =>
                new HealthOptionsFactory(createOptions,
                    provider.GetServices<IConfigureOptions<RelayerHealthCheckOptions>>(),
                    provider.GetServices<IPostConfigureOptions<RelayerHealthCheckOptions>>(),
                    provider.GetServices<IValidateOptions<RelayerHealthCheckOptions>>()));
        }
        services.AddOptions<RelayerHealthCheckOptions>()
            .Validate(static value => value.ConsumerStallTimeout > TimeSpan.Zero,
                "ConsumerStallTimeout must be positive.")
            .Validate(static value => value.OutboxDegradedThreshold > 0,
                "OutboxDegradedThreshold must be positive.")
            .Validate(static value => value.OutboxUnhealthyThreshold > value.OutboxDegradedThreshold,
                "OutboxUnhealthyThreshold must exceed OutboxDegradedThreshold.")
            .Validate(static value => value.OutboxNames is not null &&
                value.OutboxNames.All(static name => !string.IsNullOrWhiteSpace(name)) &&
                value.OutboxNames.Distinct(StringComparer.Ordinal).Count() == value.OutboxNames.Count,
                "OutboxNames must contain unique, nonblank names.")
            .ValidateOnStart();
        services.TryAddSingleton(TimeProvider.System);
        return services.AddHealthChecks()
            .AddCheck<TransportConnectivityHealthCheck>("relayer.transport", tags: ["relayer", "ready"])
            .AddCheck<ConsumerLivenessHealthCheck>("relayer.consumer", tags: ["relayer", "ready"])
            .AddCheck<OutboxBacklogHealthCheck>("relayer.outbox", tags: ["relayer", "ready"]);
    }

    private sealed class HealthRegistration;

    // OptionsFactory keeps standard configuration, post-configuration and validation for immutable defaults.
    private sealed class HealthOptionsFactory(Func<RelayerHealthCheckOptions> createOptions,
        IEnumerable<IConfigureOptions<RelayerHealthCheckOptions>> setups,
        IEnumerable<IPostConfigureOptions<RelayerHealthCheckOptions>> postConfigures,
        IEnumerable<IValidateOptions<RelayerHealthCheckOptions>> validations)
        : OptionsFactory<RelayerHealthCheckOptions>(setups, postConfigures, validations)
    {
        protected override RelayerHealthCheckOptions CreateInstance(string name) => createOptions();
    }

    // The configuration binding generator skips init-only properties. Keep this small binding explicit and AOT safe.
    private static RelayerHealthCheckOptions ReadConfiguration(IConfiguration configuration)
    {
        var defaults = new RelayerHealthCheckOptions();
        var outboxes = configuration.GetSection(nameof(RelayerHealthCheckOptions.OutboxNames));
        if (outboxes.Value is not null)
            throw new OptionsValidationException(Options.DefaultName, typeof(RelayerHealthCheckOptions),
                ["OutboxNames must be an array of store names."]);
        return new RelayerHealthCheckOptions
        {
            ConsumerStallTimeout = Read(nameof(RelayerHealthCheckOptions.ConsumerStallTimeout), defaults.ConsumerStallTimeout),
            OutboxDegradedThreshold = Read(nameof(RelayerHealthCheckOptions.OutboxDegradedThreshold), defaults.OutboxDegradedThreshold),
            OutboxUnhealthyThreshold = Read(nameof(RelayerHealthCheckOptions.OutboxUnhealthyThreshold), defaults.OutboxUnhealthyThreshold),
            OutboxNames = outboxes.GetChildren().Select(static child => child.Value!).ToArray()
        };

        T Read<T>(string key, T fallback) where T : IParsable<T>
        {
            var value = configuration[key];
            if (value is null) return fallback;
            if (T.TryParse(value, CultureInfo.InvariantCulture, out var parsed)) return parsed;
            throw new OptionsValidationException(Options.DefaultName, typeof(RelayerHealthCheckOptions),
                [$"{key}: '{value}' is not a valid {typeof(T).Name}."]);
        }
    }
}
