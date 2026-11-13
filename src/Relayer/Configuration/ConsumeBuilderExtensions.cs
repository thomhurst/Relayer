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
        if (builder.MessageTypes.IsFrozen)
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

    /// <summary>Applies a declaration and global middleware once during transport startup, before Build or BuildBatch.</summary>
    /// <remarks>Consumer configuration runs first so its transaction boundary surrounds global middleware. Batch composition rejects single-message middleware.</remarks>
    public static ConsumePipelineBuilder ConfigureConsumer(this ConsumePipelineBuilder pipeline, IServiceProvider services, IConsumerDefinition consumer)
    {
        ArgumentNullException.ThrowIfNull(pipeline);
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(consumer);
        foreach (var configure in consumer.PipelineConfiguration) configure(pipeline);
        foreach (var middleware in services.GetServices<IConsumeMiddleware>()) pipeline.Use(middleware);
        return pipeline;
    }
}
