using System.ComponentModel;
using System.Diagnostics.CodeAnalysis;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace Relayer.Pipeline;

/// <summary>Generated handler registration implemented by consumer builders.</summary>
public interface IConsumerInfrastructure
{
    /// <summary>Adds a generated descriptor to this consumer and the shared registry.</summary>
    void AddHandler(HandlerDescriptor descriptor);
}

/// <summary>Registers generated handler metadata outside the application consumer surface.</summary>
public static class ConsumerInfrastructureExtensions
{
    /// <summary>Adds a generated descriptor during configuration.</summary>
    public static void AddHandler(this IConsumerBuilder builder, HandlerDescriptor descriptor)
    {
        ArgumentNullException.ThrowIfNull(builder);

        if (builder is not IConsumerInfrastructure infrastructure)
        {
            throw new ArgumentException("The builder must implement IConsumerInfrastructure to support generated handlers.", nameof(builder));
        }

        infrastructure.AddHandler(descriptor);
    }

    /// <summary>Adds the default scoped handler service when no unkeyed registration covers it.</summary>
    [EditorBrowsable(EditorBrowsableState.Never)]
    public static void TryAddHandlerService<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)] THandler>(
        this IServiceCollection services)
        where THandler : class, IHandlerMarker
    {
        ArgumentNullException.ThrowIfNull(services);

        var handlerType = typeof(THandler);

        if (handlerType.IsConstructedGenericType)
        {
            var definition = handlerType.GetGenericTypeDefinition();

            foreach (var descriptor in services)
            {
                if (descriptor.ServiceType == definition && !descriptor.IsKeyedService)
                {
                    // Leave closing and resolving the open generic registration to the container.
                    return;
                }
            }
        }

        services.TryAddScoped<THandler>();
    }
}
