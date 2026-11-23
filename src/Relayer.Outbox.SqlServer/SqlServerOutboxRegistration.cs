using System.Diagnostics.CodeAnalysis;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;
using Relayer.Outbox;
using Relayer.Transports;

namespace Relayer.Outbox.SqlServer;

/// <summary>Registers scoped SQL Server transaction enlistment. Stores and dispatch are configured separately.</summary>
public static class SqlServerOutboxRegistration
{
    /// <summary>Adds the SQL Server outbox and the scoped IOutboxEnlistment used by ISender.</summary>
    [RequiresUnreferencedCode("SqlClient runtime trimming is unsupported; see https://github.com/dotnet/SqlClient/issues/1947.")]
    [RequiresDynamicCode("SqlClient runtime Native AOT is unsupported; see https://github.com/dotnet/SqlClient/issues/1947.")]
    public static IServiceCollection AddRelayerSqlServerOutbox(this IServiceCollection services, SqlServerOutboxOptions? options = null)
    {
        ArgumentNullException.ThrowIfNull(services);

        if (services.Any(static descriptor => !descriptor.IsKeyedService && descriptor.ServiceType == typeof(IOutboxEnlistment)) &&
            !services.Any(static descriptor => !descriptor.IsKeyedService && descriptor.ServiceType == typeof(SqlServerOutbox)))
        {
            throw new InvalidOperationException("A different IOutboxEnlistment is already registered. Configure one scoped outbox integration.");
        }

        if (options is not null)
        {
            SqlServerOutbox.ValidateOptions(options);
        }

        services.TryAddScoped<SqlServerOutbox>(provider =>
        {
            var snapshot = provider.GetRequiredService<IOptionsSnapshot<SqlServerOutboxOptions>>();
            var dispatcher = snapshot.Value.DispatchStoreName is { } name
                ? provider.GetRequiredKeyedService<SqlServerDirectDispatcher>(name) : null;
            return new(snapshot, provider.GetService<RelayerInstrumentation>(), dispatcher);
        });
        services.TryAddScoped<IOutboxEnlistment>(static provider => provider.GetRequiredService<SqlServerOutbox>());
        services.TryAddEnumerable(ServiceDescriptor.Singleton<IValidateOptions<SqlServerOutboxOptions>, Validator>());
        services.AddOptions<SqlServerOutboxOptions>().ValidateOnStart();

        if (options is not null)
        {
            AddFactory(services, options, null);
        }

        return services;
    }

    /// <summary>Binds immutable options explicitly through the Options pattern, including startup validation and reload for new scopes.</summary>
    [RequiresUnreferencedCode("SqlClient runtime trimming is unsupported; see https://github.com/dotnet/SqlClient/issues/1947.")]
    [RequiresDynamicCode("SqlClient runtime Native AOT is unsupported; see https://github.com/dotnet/SqlClient/issues/1947.")]
    public static IServiceCollection AddRelayerSqlServerOutbox(this IServiceCollection services, IConfiguration configuration)
    {
        ArgumentNullException.ThrowIfNull(configuration);
        services.AddRelayerSqlServerOutbox();
        AddFactory(services, new(), configuration);
        return services;
    }

    private static void AddFactory(IServiceCollection services, SqlServerOutboxOptions options, IConfiguration? configuration)
    {
        var sections = OutboxOptionsConfiguration<SqlServerOutboxOptions>.Register(services, configuration);
        services.Add(new ServiceDescriptor(typeof(IOptionsFactory<SqlServerOutboxOptions>),
            provider => new Factory(options, sections,
                provider.GetServices<IConfigureOptions<SqlServerOutboxOptions>>(),
                provider.GetServices<IPostConfigureOptions<SqlServerOutboxOptions>>(),
                provider.GetServices<IValidateOptions<SqlServerOutboxOptions>>()),
            configuration is null ? ServiceLifetime.Singleton : ServiceLifetime.Transient));
    }

    private sealed class Validator : IValidateOptions<SqlServerOutboxOptions>
    {
        public ValidateOptionsResult Validate(string? name, SqlServerOutboxOptions options)
        {
            try
            {
                SqlServerOutbox.ValidateOptions(options);
                return ValidateOptionsResult.Success;
            }
            catch (ArgumentException error)
            {
                return ValidateOptionsResult.Fail(error.Message);
            }
        }
    }

    // Configuration's source generator skips init-only setters. Bind scalars without runtime reflection.
    private sealed class Factory(SqlServerOutboxOptions defaults, OutboxOptionsConfiguration<SqlServerOutboxOptions>? configuration,
        IEnumerable<IConfigureOptions<SqlServerOutboxOptions>> setups,
        IEnumerable<IPostConfigureOptions<SqlServerOutboxOptions>> postConfigures,
        IEnumerable<IValidateOptions<SqlServerOutboxOptions>> validations)
        : OptionsFactory<SqlServerOutboxOptions>(setups, postConfigures, validations)
    {
        protected override SqlServerOutboxOptions CreateInstance(string name)
        {
            return new()
            {
                DispatchStoreName = configuration?[nameof(defaults.DispatchStoreName)] ?? defaults.DispatchStoreName,
                Schema = configuration?[nameof(defaults.Schema)] ?? defaults.Schema,
                TablePrefix = configuration?[nameof(defaults.TablePrefix)] ?? defaults.TablePrefix,
                MaxDispatchAttempts = configuration?.Read<int>(nameof(defaults.MaxDispatchAttempts)) ?? defaults.MaxDispatchAttempts,
                Lanes = configuration?.Read<int>(nameof(defaults.Lanes)) ?? defaults.Lanes
            };
        }
    }
}
