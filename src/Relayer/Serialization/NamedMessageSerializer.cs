#if NET10_0
using System.Buffers;
using System.Collections.Concurrent;
using System.Diagnostics.CodeAnalysis;
using Microsoft.Extensions.DependencyInjection.Extensions;
#endif
using Microsoft.Extensions.DependencyInjection;

namespace Relayer.Serialization;

internal static class NamedMessageSerializer
{
    internal static void Register(IServiceCollection services, string name, Func<IServiceProvider, IMessageSerializer> factory)
    {
#if NET10_0
        services.TryAddSingleton<Ownership>();
#endif
        // Use a private service type for lookup and an opaque key for the DI-owned factory result.
        // This also keeps overrides out of GetKeyedServices<IMessageSerializer>(KeyedService.AnyKey).
        var registration = new Registration();
        services.AddKeyedSingleton(name, registration);
        services.AddKeyedSingleton<object>(registration, (provider, _) =>
        {
#if NET10_0
            var ownership = provider.GetRequiredService<Ownership>();
            var factoryServices = new SerializerFactoryServices(provider);
#else
            var factoryServices = provider;
#endif
            var serializer = factory(factoryServices)
                ?? throw new InvalidOperationException($"Transport '{name}': the serializer factory returned null.");
#if NET10_0
            return ownership.Capture(provider, registration, serializer, factoryServices.ApplicationSerializer);
#else
            return serializer;
#endif
        });
    }

    internal static IMessageSerializer? Resolve(IServiceProvider services, string name)
    {
        var registration = services.GetKeyedService<Registration>(name);

        if (registration is null)
        {
            return null;
        }

        var serializer = (IMessageSerializer)services.GetRequiredKeyedService<object>(registration);
#if NET10_0
        return serializer is Alias alias ? alias.Serializer : serializer;
#else
        return serializer;
#endif
    }

    private sealed class Registration;

#if NET10_0
    // .NET 10 captures every disposable factory result. .NET 11 deduplicates captures by reference.
    private sealed class Ownership
    {
        private readonly ConcurrentDictionary<IMessageSerializer, Registration> _owners = new(ReferenceEqualityComparer.Instance);

        internal IMessageSerializer Capture(
            IServiceProvider provider, Registration registration, IMessageSerializer serializer, object? applicationSerializer)
        {
            if (serializer is not (IDisposable or IAsyncDisposable))
            {
                return serializer;
            }

            if (ReferenceEquals(serializer, applicationSerializer))
            {
                return new Alias(serializer);
            }

            var owner = _owners.GetOrAdd(serializer, registration);

            if (ReferenceEquals(owner, registration))
            {
                return serializer;
            }

            // A concurrent first resolution must be captured before services depending on this alias.
            _ = provider.GetRequiredKeyedService<object>(owner);
            return new Alias(serializer);
        }
    }

    // Observe explicit application serializer lookups without resolving an unused fallback.
    private sealed class SerializerFactoryServices(IServiceProvider provider) : IKeyedServiceProvider, ISupportRequiredService
    {
        private object? _applicationSerializer;

        internal object? ApplicationSerializer => _applicationSerializer;

        public object? GetService(Type serviceType)
        {
            return Observe(serviceType, null, provider.GetService(serviceType));
        }

        public object GetRequiredService(Type serviceType)
        {
            return Observe(serviceType, null, provider.GetRequiredService(serviceType));
        }

        public object? GetKeyedService(Type serviceType, object? serviceKey)
        {
            return Observe(serviceType, serviceKey, provider.GetKeyedService(serviceType, serviceKey));
        }

        public object GetRequiredKeyedService(Type serviceType, object? serviceKey)
        {
            return Observe(serviceType, serviceKey, provider.GetRequiredKeyedService(serviceType, serviceKey));
        }

        [return: NotNullIfNotNull(nameof(service))]
        private object? Observe(Type serviceType, object? serviceKey, object? service)
        {
            if (serviceKey is null && serviceType == typeof(IMessageSerializer))
            {
                _applicationSerializer = service;
            }

            return service;
        }
    }

    private sealed class Alias(IMessageSerializer serializer) : IMessageSerializer
    {
        internal IMessageSerializer Serializer => serializer;

        public string ContentType => serializer.ContentType;

        public void Serialize<TMessage>(TMessage message, IBufferWriter<byte> destination)
        {
            serializer.Serialize(message, destination);
        }

        public TMessage Deserialize<TMessage>(ReadOnlySequence<byte> source)
        {
            return serializer.Deserialize<TMessage>(source);
        }
    }
#endif
}
