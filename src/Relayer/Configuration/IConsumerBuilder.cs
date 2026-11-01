using Microsoft.Extensions.DependencyInjection;

namespace Relayer;

/// <summary>The common consumer surface used by generated handler registration.</summary>
public interface IConsumerBuilder
{
    /// <summary>The application's service collection.</summary>
    IServiceCollection Services { get; }

    /// <summary>Adds a generated descriptor to this consumer and the shared handler registry.</summary>
    void AddHandler(HandlerDescriptor descriptor);

    /// <summary>Sets the maximum number of concurrent handler invocations.</summary>
    IConsumerBuilder Concurrency(int maximum);

    /// <summary>Sets a maximum batch size and the maximum wait for a partial batch.</summary>
    IConsumerBuilder Batch(int maxSize, TimeSpan maxWait);
}
