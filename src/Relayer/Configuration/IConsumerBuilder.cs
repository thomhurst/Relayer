using Microsoft.Extensions.DependencyInjection;
using Relayer.Pipeline;

namespace Relayer;

/// <summary>The common consumer surface used by generated handler registration.</summary>
public interface IConsumerBuilder
{
    /// <summary>The application's service collection.</summary>
    IServiceCollection Services
    {
        get;
    }

    /// <summary>Configures the existing consume pipeline once at startup, before global middleware.</summary>
    IConsumerBuilder ConfigurePipeline(Action<ConsumePipelineBuilder> configure);

    internal IConsumerBuilder DeclareAliases<TMessage>(MessageType[] identities)
    {
        return ConfigurePipeline(pipeline => pipeline.Accepts<TMessage>(identities));
    }

    /// <summary>Sets the portable retry, redelivery and dead-letter policy.</summary>
    IConsumerBuilder Failures(FailureOptions options);

    /// <summary>Overrides the global failure classifier for this consumer.</summary>
    IConsumerBuilder Classify(IFailureClassifier classifier);

    /// <summary>Sets the maximum number of concurrent handler invocations.</summary>
    IConsumerBuilder Concurrency(int maximum);

    /// <summary>Sets a maximum batch size and the maximum wait for a partial batch.</summary>
    IConsumerBuilder Batch(int maxSize, TimeSpan maxWait);
}
