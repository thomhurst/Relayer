using Relayer.Transports;
using Relayer.Pipeline;
using System.Diagnostics.CodeAnalysis;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace Relayer;

/// <summary>Registers concurrent consume middleware using the existing consume pipeline.</summary>
public static class ConsumeBuilderExtensions
{
    /// <summary>Adds singleton middleware to every declared consumer, after consumer-specific middleware.</summary>
    /// <remarks>Middleware must be thread-safe and cannot capture scoped services. Registration order is execution order.</remarks>
    public static IRelayerBuilder UseConsumeMiddleware<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)] TMiddleware>(this IRelayerBuilder builder)
        where TMiddleware : class, IConsumeMiddleware
    {
        ArgumentNullException.ThrowIfNull(builder);
        if (builder.Infrastructure().MessageTypes.IsFrozen)
            throw new InvalidOperationException("Middleware cannot be registered after AddRelayer configuration completes.");
        builder.Services.AddSingleton<IConsumeMiddleware, TMiddleware>();
        return builder;
    }

    /// <summary>Adds singleton middleware to this consumer in declaration order.</summary>
    /// <remarks>Middleware must be thread-safe and cannot capture scoped services. Resolve delivery services through MessageContext.Services.</remarks>
    public static IConsumerBuilder Use<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)] TMiddleware>(this IConsumerBuilder builder)
        where TMiddleware : class, IConsumeMiddleware
    {
        ArgumentNullException.ThrowIfNull(builder);
        builder.ConfigurePipeline(static pipeline => pipeline.Use(static services => services.GetRequiredService<TMiddleware>()));
        builder.Services.TryAddSingleton<TMiddleware>();
        return builder;
    }
}
