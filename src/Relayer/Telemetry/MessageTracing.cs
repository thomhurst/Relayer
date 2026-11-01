using System.Diagnostics;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;

namespace Relayer;

/// <summary>Owns the application's Relayer activity source. Compose operations once per endpoint at startup.</summary>
public sealed class MessageTracing : IDisposable
{
    private readonly IActivityEnricher[] _enrichers;
    /// <summary>The immutable policy used by operations and batch send composition.</summary>
    public TracingOptions Options { get; }

    /// <summary>Creates an isolated source owner; DI disposes it with its service provider.</summary>
    public MessageTracing(IOptions<TracingOptions> options, IEnumerable<IActivityEnricher> enrichers)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(enrichers);
        Options = options.Value;
        _enrichers = enrichers.ToArray();
        Source = new(new ActivitySourceOptions(RelayerTelemetry.ActivitySourceName)
        {
            TelemetrySchemaUrl = RelayerTelemetry.SchemaUrl,
        });
    }

    /// <summary>The source to enable with AddSource("Relayer").</summary>
    public ActivitySource Source { get; }

    internal void Enrich(Activity activity)
    {
        if (!activity.Recorded) return;
        foreach (var enricher in _enrichers) enricher.Enrich(activity);
    }

    /// <summary>Disposes this owner's activity source.</summary>
    public void Dispose() => Source.Dispose();
}

/// <summary>Registers the shared tracing source and options without process-wide mutable state.</summary>
public static class TracingBuilderExtensions
{
    /// <summary>Registers tracing primitives. Bind TracingOptions or register IActivityEnricher services before building the provider.</summary>
    public static IRelayerBuilder Telemetry(this IRelayerBuilder builder)
    {
        ArgumentNullException.ThrowIfNull(builder);
        builder.Services.AddOptions<TracingOptions>();
        builder.Services.TryAddSingleton<MessageTracing>();
        return builder;
    }
}
