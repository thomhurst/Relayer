using System.Globalization;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;
using MongoDB.Driver;

namespace Relayer.Outbox.MongoDB;

/// <summary>Explicit registration of MongoDB storage and optional scoped send enlistment.</summary>
public static class MongoDBOutboxRegistration
{
    /// <summary>Registers a named read/claim store and the scoped outbox used by ISender.</summary>
    public static IServiceCollection AddRelayerMongoDBOutbox(this IServiceCollection services, string name,
        IMongoDatabase database, MongoDBOutboxOptions? options = null) => Register(services, name, database, options ?? new(), null, enlist: true);

    /// <summary>Binds immutable, named options without reflection, reloading for new scopes.</summary>
    public static IServiceCollection AddRelayerMongoDBOutbox(this IServiceCollection services, string name,
        IMongoDatabase database, IConfiguration configuration) => FromConfiguration(services, name, database, configuration, enlist: true);

    /// <summary>Registers only a keyed scoped IOutboxStore. Does not deploy schema or start workers.</summary>
    public static IServiceCollection AddRelayerMongoDBOutboxStore(this IServiceCollection services, string name,
        IMongoDatabase database, MongoDBOutboxOptions? options = null) => Register(services, name, database, options ?? new(), null, enlist: false);

    /// <summary>Binds a named read/claim store's options from configuration.</summary>
    public static IServiceCollection AddRelayerMongoDBOutboxStore(this IServiceCollection services, string name,
        IMongoDatabase database, IConfiguration configuration) => FromConfiguration(services, name, database, configuration, enlist: false);

    private static IServiceCollection FromConfiguration(IServiceCollection services, string name, IMongoDatabase database,
        IConfiguration configuration, bool enlist)
    {
        ArgumentNullException.ThrowIfNull(configuration);
        return Register(services, name, database, new(), configuration, enlist);
    }

    private static IServiceCollection Register(IServiceCollection services, string name, IMongoDatabase database,
        MongoDBOutboxOptions options, IConfiguration? configuration, bool enlist)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        ArgumentNullException.ThrowIfNull(database);
        options.Validate();
        if (services.Any(descriptor => descriptor.ServiceType == typeof(Settings) && descriptor.ImplementationInstance is Settings settings && settings.Name == name))
            throw new InvalidOperationException($"MongoDB outbox store '{name}' is already registered.");
        if (enlist && services.Any(static descriptor => descriptor.ServiceType == typeof(IOutboxEnlistment)))
            throw new InvalidOperationException("Configure only one scoped IOutboxEnlistment integration.");
        services.AddSingleton(new Settings(name, options, configuration));
        services.AddOptions<MongoDBOutboxOptions>(name).ValidateOnStart();
        services.TryAddSingleton<IValidateOptions<MongoDBOutboxOptions>, Validator>();
        services.TryAddSingleton<IOptionsFactory<MongoDBOutboxOptions>, Factory>();
        if (configuration is not null)
            services.AddSingleton<IOptionsChangeTokenSource<MongoDBOutboxOptions>>(new ConfigurationChangeTokenSource<MongoDBOutboxOptions>(name, configuration));
        services.AddKeyedScoped<IOutboxStore>(name, (provider, _) => new MongoDBOutboxStore(database,
            provider.GetRequiredService<IOptionsSnapshot<MongoDBOutboxOptions>>().Get(name)));
        if (enlist)
        {
            services.AddScoped(provider => new MongoDBOutbox(database, provider.GetRequiredService<IOptionsSnapshot<MongoDBOutboxOptions>>().Get(name)));
            services.AddScoped<IOutboxEnlistment>(static provider => provider.GetRequiredService<MongoDBOutbox>());
        }
        return services;
    }

    private sealed record Settings(string Name, MongoDBOutboxOptions Options, IConfiguration? Configuration);

    private sealed class Factory(IEnumerable<Settings> settings, IEnumerable<IConfigureOptions<MongoDBOutboxOptions>> setups,
        IEnumerable<IPostConfigureOptions<MongoDBOutboxOptions>> postConfigures, IEnumerable<IValidateOptions<MongoDBOutboxOptions>> validations)
        : OptionsFactory<MongoDBOutboxOptions>(setups, postConfigures, validations)
    {
        protected override MongoDBOutboxOptions CreateInstance(string name)
        {
            foreach (var setting in settings)
            {
                if (setting.Name != name) continue;
                var lanes = setting.Configuration?[nameof(MongoDBOutboxOptions.Lanes)];
                var laneCount = setting.Options.Lanes;
                if (lanes is not null && !int.TryParse(lanes, NumberStyles.Integer, CultureInfo.InvariantCulture, out laneCount))
                    throw new OptionsValidationException(name, typeof(MongoDBOutboxOptions), ["Lanes must be an integer."]);
                return new()
                {
                    CollectionPrefix = setting.Configuration?[nameof(MongoDBOutboxOptions.CollectionPrefix)] ?? setting.Options.CollectionPrefix,
                    Lanes = laneCount
                };
            }
            return new();
        }
    }

    private sealed class Validator : IValidateOptions<MongoDBOutboxOptions>
    {
        public ValidateOptionsResult Validate(string? name, MongoDBOutboxOptions options)
        {
            try { options.Validate(); return ValidateOptionsResult.Success; }
            catch (ArgumentException error) { return ValidateOptionsResult.Fail(error.Message); }
        }
    }
}
