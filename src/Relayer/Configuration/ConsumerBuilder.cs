using Relayer.Transports;
using Relayer.Pipeline;
using Microsoft.Extensions.DependencyInjection;

namespace Relayer;

/// <summary>Configures a consumer without replacing its native transport options.</summary>
public sealed class ConsumerBuilder<TConsumerOptions> : IConsumerBuilder, IConsumerInfrastructure where TConsumerOptions : class, new()
{
    private readonly IServiceCollection _services;
    private readonly HandlerRegistry _registry;
    private readonly string _source;
    private readonly List<Type> _handlers = [];
    private readonly List<Action<ConsumePipelineBuilder>> _pipelineConfiguration = [];
    private int _concurrency = 1;
    private int _batchSize = 1;
    private TimeSpan _batchWait = TimeSpan.FromMilliseconds(100);
    private bool _built;
    private FailureOptions? _failure;
    private IFailureClassifier? _classifier;

    internal ConsumerBuilder(IServiceCollection services, HandlerRegistry registry, string source)
    {
        _services = services;
        _registry = registry;
        _source = source;
    }

    /// <summary>The transport's native consumer options.</summary>
    public TConsumerOptions Options { get; } = new();

    IServiceCollection IConsumerBuilder.Services => _services;

    void IConsumerInfrastructure.AddHandler(HandlerDescriptor descriptor)
    {
        EnsureMutable();
        _registry.Add(descriptor);
        if (!_handlers.Contains(descriptor.HandlerType)) _handlers.Add(descriptor.HandlerType);
    }

    IConsumerBuilder IConsumerBuilder.Concurrency(int maximum) => Concurrency(maximum);

    IConsumerBuilder IConsumerBuilder.Batch(int maxSize, TimeSpan maxWait) => Batch(maxSize, maxWait);

    /// <summary>Sets the maximum number of concurrent handler invocations.</summary>
    public ConsumerBuilder<TConsumerOptions> Concurrency(int maximum)
    {
        EnsureMutable();
        _concurrency = maximum;
        return this;
    }

    /// <summary>Sets a maximum batch size and the maximum wait for a partial batch.</summary>
    public ConsumerBuilder<TConsumerOptions> Batch(int maxSize, TimeSpan maxWait)
    {
        EnsureMutable();
        _batchSize = maxSize;
        _batchWait = maxWait;
        return this;
    }

    /// <summary>Configures the existing consume pipeline once at startup, before global middleware.</summary>
    public ConsumerBuilder<TConsumerOptions> ConfigurePipeline(Action<ConsumePipelineBuilder> configure)
    {
        EnsureMutable();
        ArgumentNullException.ThrowIfNull(configure);
        _pipelineConfiguration.Add(configure);
        return this;
    }

    IConsumerBuilder IConsumerBuilder.ConfigurePipeline(Action<ConsumePipelineBuilder> configure) => ConfigurePipeline(configure);

    /// <summary>Sets this consumer's portable retry, redelivery and dead-letter policy.</summary>
    public ConsumerBuilder<TConsumerOptions> Failures(FailureOptions options)
    {
        EnsureMutable();
        ArgumentNullException.ThrowIfNull(options);
        _failure = options;
        return this;
    }

    /// <summary>Overrides the global classifier for this consumer. The classifier must be thread-safe.</summary>
    public ConsumerBuilder<TConsumerOptions> Classify(IFailureClassifier classifier)
    {
        EnsureMutable();
        ArgumentNullException.ThrowIfNull(classifier);
        _classifier = classifier;
        return this;
    }

    IConsumerBuilder IConsumerBuilder.Failures(FailureOptions options) => Failures(options);
    IConsumerBuilder IConsumerBuilder.Classify(IFailureClassifier classifier) => Classify(classifier);

    private void EnsureMutable()
    {
        if (_built) throw new InvalidOperationException("The consumer declaration is frozen.");
    }

    internal IConsumerDefinition Build()
    {
        _built = true;
        return new ConsumerDefinition<TConsumerOptions>(_source, Options, _handlers.ToArray(), _concurrency, _batchSize, _batchWait, _pipelineConfiguration.ToArray(), _failure, _classifier);
    }
}
