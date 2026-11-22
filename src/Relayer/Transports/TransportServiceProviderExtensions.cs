using Microsoft.Extensions.DependencyInjection;
using Relayer.Serialization;

namespace Relayer.Transports;

/// <summary>Resolves services when composing a transport.</summary>
public static class TransportServiceProviderExtensions
{
    /// <summary>Resolves the named serializer, falling back to the application serializer. For adapter startup only.</summary>
    public static IMessageSerializer GetMessageSerializer(this IServiceProvider services, string transportName)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentException.ThrowIfNullOrWhiteSpace(transportName);
        return NamedMessageSerializer.Resolve(services, transportName) ?? services.GetRequiredService<IMessageSerializer>();
    }
}
