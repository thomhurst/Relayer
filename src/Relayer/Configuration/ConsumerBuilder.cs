using Microsoft.Extensions.DependencyInjection;
using Relayer.Pipeline;
using Relayer.Transports;

namespace Relayer;

/// <summary>Configures a consumer without replacing its native transport options.</summary>
public sealed class ConsumerBuilder<TConsumerOptions> : IConsumerBuilder, IConsumerInfrastructure where TConsumerOptions : class, new()
{
    private readonly IServiceCollection _services;
    private readonly HandlerRegistry _registry;
    private readonly string _source;
    private readonly List<Type> _handlers = [];
    private readonly HashSet<(Type Handler, Type Message, bool IsBatch)> _selected = [];
    private readonly List<Action<ConsumePipelineBuilder>> _pipelineConfiguration = [];
    private readonly List<ConsumerMessageAlias> _messageAliases = [];
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
    public TConsumerOptions Options
    {
        get;
    } = new();

    IServiceCollection IConsumerBuilder.Services => _services;

    void IConsumerInfrastructure.AddHandler(HandlerDescriptor descriptor)
    {
        EnsureMutable();
        _registry.Add(descriptor);
        _selected.Add((descriptor.HandlerType, descriptor.MessageType, descriptor.IsBatch));

        if (!_handlers.Contains(descriptor.HandlerType))
        {
            _handlers.Add(descriptor.HandlerType);
        }
    }

    IConsumerBuilder IConsumerBuilder.Concurrency(int maximum)
    {
        return Concurrency(maximum);
    }

    IConsumerBuilder IConsumerBuilder.Batch(int maxSize, TimeSpan maxWait)
    {
        return Batch(maxSize, maxWait);
    }

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

    IConsumerBuilder IConsumerBuilder.ConfigurePipeline(Action<ConsumePipelineBuilder> configure)
    {
        return ConfigurePipeline(configure);
    }

    IConsumerBuilder IConsumerBuilder.DeclareAliases<TMessage>(MessageType[] identities)
    {
        ConfigurePipeline(pipeline => pipeline.Accepts<TMessage>(identities));

        foreach (var identity in identities)
        {
            _messageAliases.Add(new(typeof(TMessage), typeof(Tombstone<TMessage>), identity));
        }

        return this;
    }

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

    IConsumerBuilder IConsumerBuilder.Failures(FailureOptions options)
    {
        return Failures(options);
    }

    IConsumerBuilder IConsumerBuilder.Classify(IFailureClassifier classifier)
    {
        return Classify(classifier);
    }

    private void EnsureMutable()
    {
        if (_built)
        {
            throw new InvalidOperationException("The consumer declaration is frozen.");
        }
    }

    internal IConsumerDefinition Build()
    {
        _built = true;
        return new ConsumerDefinition<TConsumerOptions>(_source, Options, _handlers.ToArray(), _concurrency, _batchSize, _batchWait, _pipelineConfiguration.ToArray(), _failure, _classifier, _selected, _messageAliases.ToArray());
    }
}
