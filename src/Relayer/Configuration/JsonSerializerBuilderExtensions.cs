using System.ComponentModel;
using System.Text.Json.Serialization;
using Microsoft.Extensions.DependencyInjection;

namespace Relayer;

/// <summary>Configures source-generated JSON serialization.</summary>
public static class JsonSerializerBuilderExtensions
{
    /// <summary>
    /// Adds a source-generated context. For a type covered by several registrations, the last applicable context wins.
    /// Configure serializer options when constructing the context.
    /// </summary>
    public static IRelayerBuilder UseJsonSerializer(this IRelayerBuilder builder, JsonSerializerContext context)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(context);
        builder.Services.AddSingleton(new JsonSerializerRegistration(context));
        return builder;
    }

    /// <summary>Registers a generated context for the message roots declared by its assembly. Called by generated code.</summary>
    [EditorBrowsable(EditorBrowsableState.Never)]
    public static IRelayerBuilder UseJsonSerializer(this IRelayerBuilder builder, JsonSerializerContext context, Type[] messageRoots)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(context);
        ArgumentNullException.ThrowIfNull(messageRoots);
        builder.Services.AddSingleton(new JsonSerializerRegistration(context, new HashSet<Type>(messageRoots)));
        return builder;
    }
}

internal sealed record JsonSerializerRegistration(JsonSerializerContext Context, HashSet<Type>? MessageRoots = null);
