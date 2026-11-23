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
        services.AddKeyedSingleton<IMessageSerializer>(name, (provider, _) =>
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
            return ownership.Capture(provider, name, serializer, factoryServices.ApplicationSerializer);
#else
            return serializer;
#endif
        });
    }

    internal static IMessageSerializer? Resolve(IServiceProvider services, string name)
    {
        var serializer = services.GetKeyedService<IMessageSerializer>(name);
#if NET10_0
        return serializer is Alias alias ? alias.Serializer : serializer;
#else
        return serializer;
#endif
    }

#if NET10_0
    // .NET 10 captures every disposable factory result. .NET 11 deduplicates captures by reference.
    private sealed class Ownership
    {
        private readonly ConcurrentDictionary<IMessageSerializer, string> _owners = new(ReferenceEqualityComparer.Instance);

        internal IMessageSerializer Capture(
            IServiceProvider provider, string name, IMessageSerializer serializer, object? applicationSerializer)
        {
            if (serializer is not (IDisposable or IAsyncDisposable))
            {
                return serializer;
            }

            if (ReferenceEquals(serializer, applicationSerializer))
            {
                return new Alias(serializer);
            }

            var owner = _owners.GetOrAdd(serializer, name);

            if (owner == name)
            {
                return serializer;
            }

            // A concurrent first resolution must be captured before services depending on this alias.
            _ = provider.GetRequiredKeyedService<IMessageSerializer>(owner);
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
