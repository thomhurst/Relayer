using System.Collections.Frozen;
using System.Diagnostics.CodeAnalysis;
using System.Text.Json.Serialization;
using System.Text.Json.Serialization.Metadata;
using Relayer.Serialization;

namespace Relayer;

/// <summary>
/// Maps CLR types to their stable wire identity and back. Populated explicitly or by the
/// Relayer source generator; never by reflection.
/// </summary>
public sealed class MessageTypeRegistry
{
    private readonly Dictionary<Type, MessageType> _byType = [];
    private readonly Dictionary<MessageType, Type> _byName = [];
    private readonly List<Func<JsonSerializerContext[], JsonMessageType>> _jsonTypes = [];
    private FrozenDictionary<Type, MessageType>? _frozenByType;
    private FrozenDictionary<MessageType, Type>? _frozenByName;

    /// <summary>Whether the registry has been frozen and can no longer change.</summary>
    public bool IsFrozen => _frozenByType is not null;

    /// <summary>Registers <typeparamref name="TMessage"/> with the given identity.</summary>
    /// <exception cref="InvalidOperationException">The registry is frozen, or the type or identity is already registered differently.</exception>
    public MessageTypeRegistry Add<TMessage>(string name, int version = 1)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        ArgumentOutOfRangeException.ThrowIfLessThan(version, 1);

        if (IsFrozen)
        {
            throw new InvalidOperationException("The message type registry is frozen and cannot be modified after startup.");
        }

        var identity = new MessageType(name, version);

        if (_byType.TryGetValue(typeof(TMessage), out var existing) && existing != identity)
        {
            throw new InvalidOperationException($"{typeof(TMessage)} is already registered as '{existing}'.");
        }

        if (_byName.TryGetValue(identity, out var existingType) && existingType != typeof(TMessage))
        {
            throw new InvalidOperationException($"'{identity}' is already registered to {existingType}.");
        }

        if (_byType.TryGetValue(typeof(Tombstone<TMessage>), out var existingTombstone) && existingTombstone != identity)
        {
            throw new InvalidOperationException($"{typeof(Tombstone<TMessage>)} is already registered as '{existingTombstone}'. Register only the payload type {typeof(TMessage)}; its tombstone alias is registered automatically.");
        }

        if (!_byType.ContainsKey(typeof(TMessage)))
        {
            _jsonTypes.Add(static contexts => ResolveJsonType<TMessage>(contexts));
        }
        _byType[typeof(Tombstone<TMessage>)] = identity;
        _byType[typeof(TMessage)] = identity;
        _byName[identity] = typeof(TMessage);
        return this;
    }

    /// <summary>Freezes the registry for fast lookups. Called once at startup.</summary>
    public void Freeze()
    {
        _frozenByName ??= _byName.ToFrozenDictionary();
        _frozenByType ??= _byType.ToFrozenDictionary();
    }

    internal JsonMessageType[] CreateJsonTypes(JsonSerializerContext[] contexts)
    {
        var types = new JsonMessageType[_jsonTypes.Count];
        for (var i = 0; i < types.Length; i++)
        {
            types[i] = _jsonTypes[i](contexts);
        }
        return types;
    }

    private static JsonMessageType ResolveJsonType<TMessage>(JsonSerializerContext[] contexts)
    {
        for (var i = contexts.Length - 1; i >= 0; i--)
        {
            if (contexts[i].GetTypeInfo(typeof(TMessage)) is JsonTypeInfo<TMessage> typeInfo)
            {
                typeInfo.MakeReadOnly();
                return new JsonMessageType(JsonMessageType<TMessage>.Key, typeInfo);
            }
        }
        throw new InvalidOperationException($"No source-generated JSON metadata exists for '{typeof(TMessage)}'. Call AddMessageTypesFromThisAssembly or UseJsonSerializer with a context covering that message type, or register an IMessageSerializer.");
    }

    /// <summary>Gets the identity of <paramref name="type"/>, if registered.</summary>
    public bool TryGetMessageType(Type type, out MessageType messageType) =>
        _frozenByType is { } frozen
            ? frozen.TryGetValue(type, out messageType)
            : _byType.TryGetValue(type, out messageType);

    /// <summary>Gets the CLR type registered for <paramref name="messageType"/>, if any.</summary>
    public bool TryGetClrType(MessageType messageType, [NotNullWhen(true)] out Type? type) =>
        _frozenByName is { } frozen
            ? frozen.TryGetValue(messageType, out type)
            : _byName.TryGetValue(messageType, out type);
}
