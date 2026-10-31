using System.Collections.Frozen;
using System.Diagnostics.CodeAnalysis;

namespace Relayer;

/// <summary>
/// Maps CLR types to their stable wire identity and back. Populated explicitly or by the
/// Relayer source generator; never by reflection.
/// </summary>
public sealed class MessageTypeRegistry
{
    private readonly Dictionary<Type, MessageType> _byType = [];
    private readonly Dictionary<MessageType, Type> _byName = [];
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
