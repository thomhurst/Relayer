#if NET10_0
using System.Buffers;
using System.Collections.Concurrent;
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
#endif
            var serializer = factory(provider)
                ?? throw new InvalidOperationException($"Transport '{name}': the serializer factory returned null.");
#if NET10_0
            return ownership.Capture(provider, name, serializer);
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

        internal IMessageSerializer Capture(IServiceProvider provider, string name, IMessageSerializer serializer)
        {
            if (serializer is not (IDisposable or IAsyncDisposable))
            {
                return serializer;
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
