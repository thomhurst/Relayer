using System.Globalization;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;

namespace Relayer.Outbox;

// Keep configuration lookup separate from OptionsFactory's setup and validation of init-only options.
internal sealed class OutboxOptionsConfiguration<TOptions> where TOptions : class
{
    private readonly IConfiguration _section;
    private readonly OutboxOptionsConfiguration<TOptions>? _previous;

    private OutboxOptionsConfiguration(IConfiguration section, OutboxOptionsConfiguration<TOptions>? previous)
    {
        _section = section;
        _previous = previous;
    }

    public static OutboxOptionsConfiguration<TOptions>? Register(IServiceCollection services, IConfiguration? section)
    {
        if (section is null)
        {
            // An explicit options instance replaces configuration from earlier registrations.
            services.RemoveAll<OutboxOptionsConfiguration<TOptions>>();
            return null;
        }

        var previous = services.LastOrDefault(static descriptor => !descriptor.IsKeyedService
            && descriptor.ServiceType == typeof(OutboxOptionsConfiguration<TOptions>))
            ?.ImplementationInstance as OutboxOptionsConfiguration<TOptions>;
        var configuration = new OutboxOptionsConfiguration<TOptions>(section, previous);
        services.Replace(ServiceDescriptor.Singleton(configuration));
        services.AddSingleton<IOptionsChangeTokenSource<TOptions>>(new ConfigurationChangeTokenSource<TOptions>(section));
        return configuration;
    }

    public string? this[string key]
    {
        get
        {
            for (OutboxOptionsConfiguration<TOptions>? layer = this; layer is not null; layer = layer._previous)
            {
                var value = layer._section[key];

                if (value is not null)
                {
                    return value;
                }
            }

            return null;
        }
    }

    public T? Read<T>(string key) where T : struct, IParsable<T>
    {
        var value = this[key];

        if (value is null)
        {
            return null;
        }

        if (T.TryParse(value, CultureInfo.InvariantCulture, out var parsed))
        {
            return parsed;
        }

        throw new OptionsValidationException(Options.DefaultName, typeof(TOptions), [$"{key}: '{value}' is invalid."]);
    }
}
