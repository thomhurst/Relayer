using System.Diagnostics.CodeAnalysis;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Relayer.Pipeline;
using Relayer.Transports;

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
        {
            throw new InvalidOperationException("Middleware cannot be registered after AddRelayer configuration completes.");
        }

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

    /// <summary>
    /// Dispatches messages named <paramref name="names"/> (version 1) to this consumer's handler for
    /// <typeparamref name="TMessage"/>, deserialized as that type. Receive only; sends keep the type's registered identity.
    /// </summary>
    /// <remarks><see cref="MessageContext.MessageType"/> reports the identity that arrived. Validated when the consumer starts.</remarks>
    public static IConsumerBuilder Accepts<TMessage>(this IConsumerBuilder builder, params string[] names)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(names);
        var identities = new MessageType[names.Length];

        for (var i = 0; i < names.Length; i++)
        {
            identities[i] = new(names[i], 1);
        }

        return builder.Accepts<TMessage>(identities);
    }

    /// <summary>
    /// Dispatches messages with the given wire identities to this consumer's handler for <typeparamref name="TMessage"/>,
    /// deserialized as that type. Receive only; sends keep the type's registered identity.
    /// </summary>
    /// <remarks><see cref="MessageContext.MessageType"/> reports the identity that arrived. Validated when the consumer starts.</remarks>
    public static IConsumerBuilder Accepts<TMessage>(this IConsumerBuilder builder, params MessageType[] identities)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(identities);
        ConsumePipelineBuilder.ValidateAccepted(identities);
        var accepted = (MessageType[])identities.Clone();
        return builder.DeclareAliases<TMessage>(accepted);
    }
}
