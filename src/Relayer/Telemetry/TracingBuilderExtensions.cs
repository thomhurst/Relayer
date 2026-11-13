using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Relayer.Transports;
namespace Relayer;

/// <summary>Registers the shared tracing source and options without process-wide mutable state.</summary>
public static class TracingBuilderExtensions
{
    /// <summary>Registers tracing primitives. Bind TracingOptions or register IActivityEnricher services before building the provider.</summary>
    /// <remarks>Keep one service provider in production. One source owner is shared per provider; separate providers own separate sources named Relayer.</remarks>
    public static IRelayerBuilder Telemetry(this IRelayerBuilder builder)
    {
        ArgumentNullException.ThrowIfNull(builder);
        builder.Services.AddOptions<TracingOptions>();
        builder.Services.TryAddSingleton<MessageTracing>();
        return builder;
    }
}
