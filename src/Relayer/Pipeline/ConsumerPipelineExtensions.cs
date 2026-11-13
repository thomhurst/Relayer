using Microsoft.Extensions.DependencyInjection;
using Relayer.Transports;

namespace Relayer.Pipeline;

/// <summary>Composes a declared consumer for a transport receive loop.</summary>
public static class ConsumerPipelineExtensions
{
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
