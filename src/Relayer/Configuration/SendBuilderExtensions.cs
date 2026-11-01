using System.Diagnostics.CodeAnalysis;
using Microsoft.Extensions.DependencyInjection;

namespace Relayer;

/// <summary>Registers send routes and singleton send middleware.</summary>
public static class SendBuilderExtensions
{
    /// <summary>Adds middleware in execution order. Middleware must be thread-safe and cannot capture scoped services.</summary>
    public static IRelayerBuilder UseSendMiddleware<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)] TMiddleware>(this IRelayerBuilder builder)
        where TMiddleware : class, ISendMiddleware
    {
        ArgumentNullException.ThrowIfNull(builder);
        if (builder.MessageTypes.IsFrozen)
        {
            throw new InvalidOperationException("Middleware cannot be registered after AddRelayer configuration completes.");
        }
        builder.Services.AddSingleton<ISendMiddleware, TMiddleware>();
        return builder;
    }
}
