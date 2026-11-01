using System.Diagnostics.CodeAnalysis;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace Relayer;

/// <summary>Configures a consumer without replacing its native transport options.</summary>
public sealed class ConsumerBuilder<TConsumerOptions> where TConsumerOptions : class, new()
{
    private readonly IServiceCollection _services;
    private readonly string _source;
    private readonly List<Type> _handlers = [];
    private int _concurrency = 1;
    private int _batchSize = 1;
    private TimeSpan _batchWait = TimeSpan.FromMilliseconds(100);
    private bool _built;

    internal ConsumerBuilder(IServiceCollection services, string source)
    {
        _services = services;
        _source = source;
    }

    /// <summary>The transport's native consumer options.</summary>
    public TConsumerOptions Options { get; } = new();

    /// <summary>Registers a handler explicitly. Generated dispatch resolves its message contracts at startup.</summary>
    public ConsumerBuilder<TConsumerOptions> Handler<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)] THandler>()
        where THandler : class, IHandlerMarker
    {
        EnsureMutable();
        if (!_handlers.Contains(typeof(THandler)))
        {
            _handlers.Add(typeof(THandler));
            _services.TryAddScoped<THandler>();
        }
        return this;
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
