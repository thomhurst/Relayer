namespace Relayer.Pipeline;

/// <summary>Generated handler registration implemented by consumer builders.</summary>
public interface IConsumerInfrastructure
{
    /// <summary>Adds a generated descriptor to this consumer and the shared registry.</summary>
    void AddHandler(HandlerDescriptor descriptor);
}

/// <summary>Registers generated handler metadata outside the application consumer surface.</summary>
public static class ConsumerInfrastructureExtensions
{
    /// <summary>Adds a generated descriptor during configuration.</summary>
    public static void AddHandler(this IConsumerBuilder builder, HandlerDescriptor descriptor)
    {
        ArgumentNullException.ThrowIfNull(builder);
        if (builder is not IConsumerInfrastructure infrastructure)
            throw new ArgumentException("The builder must implement IConsumerInfrastructure to support generated handlers.", nameof(builder));
        infrastructure.AddHandler(descriptor);
    }
}
