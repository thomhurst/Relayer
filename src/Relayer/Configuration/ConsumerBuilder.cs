using Microsoft.Extensions.DependencyInjection;

namespace Relayer;

/// <summary>Configures a consumer without replacing its native transport options.</summary>
public sealed class ConsumerBuilder<TConsumerOptions> : IConsumerBuilder where TConsumerOptions : class, new()
{
    private readonly IServiceCollection _services;
    private readonly HandlerRegistry _registry;
    private readonly string _source;
    private readonly List<Type> _handlers = [];
    private int _concurrency = 1;
    private int _batchSize = 1;
    private TimeSpan _batchWait = TimeSpan.FromMilliseconds(100);
    private bool _built;

    internal ConsumerBuilder(IServiceCollection services, HandlerRegistry registry, string source)
    {
        _services = services;
        _registry = registry;
        _source = source;
    }

    /// <summary>The transport's native consumer options.</summary>
    public TConsumerOptions Options { get; } = new();

    IServiceCollection IConsumerBuilder.Services => _services;

    void IConsumerBuilder.AddHandler(HandlerDescriptor descriptor)
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

    private void EnsureMutable()
    {
        if (_built) throw new InvalidOperationException("The consumer declaration is frozen.");
    }

    internal IConsumerDefinition Build()
    {
        _built = true;
        return new ConsumerDefinition<TConsumerOptions>(_source, Options, _handlers.ToArray(), _concurrency, _batchSize, _batchWait);
    }
}
