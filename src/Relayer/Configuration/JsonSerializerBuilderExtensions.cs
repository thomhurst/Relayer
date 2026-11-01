using System.Text.Json.Serialization;
using Microsoft.Extensions.DependencyInjection;

namespace Relayer;

/// <summary>Configures source-generated JSON serialization.</summary>
public static class JsonSerializerBuilderExtensions
{
    /// <summary>
    /// Adds a source-generated context. For a type covered by several contexts, the last context wins.
    /// Configure serializer options when constructing the context.
    /// </summary>
    public static IRelayerBuilder UseJsonSerializer(this IRelayerBuilder builder, JsonSerializerContext context)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(context);
        builder.Services.AddSingleton(new JsonSerializerRegistration(context));
        return builder;
    }
}

internal sealed record JsonSerializerRegistration(JsonSerializerContext Context);
