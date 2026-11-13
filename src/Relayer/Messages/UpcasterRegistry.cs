using Relayer.Serialization;

namespace Relayer.Pipeline;

/// <summary>Explicit message upgrades, validated and composed at startup without reflection.</summary>
public sealed class UpcasterRegistry
{
    private readonly Dictionary<Type, UpcasterDescriptor> _steps = [];
    private bool _frozen;

    /// <summary>Registers one upgrade from a contract to a newer version of the same stable name.</summary>
    /// <remarks>Each source contract has one upgrade. Register every contract in MessageTypes, including intermediate versions.</remarks>
    public UpcasterRegistry Add<TFrom, TTo>(IUpcaster<TFrom, TTo> upcaster)
    {
        ArgumentNullException.ThrowIfNull(upcaster);
        if (_frozen) throw new InvalidOperationException("Upcasters cannot be registered after AddRelayer configuration completes.");
        if (!_steps.TryAdd(typeof(TFrom), new UpcasterDescriptor<TFrom, TTo>(upcaster)))
            throw new InvalidOperationException($"An upcaster from '{typeof(TFrom)}' is already registered. Register one upgrade per source contract.");
        return this;
    }

    internal void Freeze(MessageTypeRegistry messages)
    {
        foreach (var step in _steps.Values)
        {
            if (!messages.TryGetMessageType(step.From, out var from) || !messages.TryGetMessageType(step.To, out var to))
                throw new InvalidOperationException($"Register stable wire identities for both upcaster contracts '{step.From}' and '{step.To}'.");
            if (from.Name != to.Name || from.Version >= to.Version)
                throw new InvalidOperationException($"Upcaster '{from}' to '{to}' must keep the stable message name and increase its version.");
        }
        _frozen = true;
    }

    internal Dictionary<MessageType, UpcastReader> Compose(Type target, MessageTypeRegistry messages, IMessageSerializer serializer)
    {
        var readers = new Dictionary<MessageType, UpcastReader>();
        foreach (var first in _steps.Values)
        {
            var current = first.To;
            var chain = new List<UpcasterDescriptor> { first };
            while (current != target && _steps.TryGetValue(current, out var next))
            {
                chain.Add(next);
                current = next.To;
            }
            if (current != target) continue;
            var reader = first.CreateReader(serializer);
            for (var i = 1; i < chain.Count; i++) reader = chain[i].Append(reader);
            messages.TryGetMessageType(first.From, out var identity);
            readers.Add(identity, reader);
        }
        return readers;
    }
}

internal abstract class UpcasterDescriptor(Type from, Type to)
{
    internal Type From { get; } = from;
    internal Type To { get; } = to;
    internal abstract UpcastReader CreateReader(IMessageSerializer serializer);
    internal abstract UpcastReader Append(UpcastReader previous);
}

internal sealed class UpcasterDescriptor<TFrom, TTo>(IUpcaster<TFrom, TTo> upcaster)
    : UpcasterDescriptor(typeof(TFrom), typeof(TTo))
{
    internal override UpcastReader CreateReader(IMessageSerializer serializer)
        => Append(new SerializedReader<TFrom>(serializer));

    internal override UpcastReader Append(UpcastReader previous)
        => new ConvertingReader((UpcastReader<TFrom>)previous, upcaster);

    private sealed class ConvertingReader(UpcastReader<TFrom> previous, IUpcaster<TFrom, TTo> converter) : UpcastReader<TTo>
    {
        internal override TTo Read(DefaultMessageContext context) => converter.Upcast(previous.Read(context));
    }
}

internal abstract class UpcastReader;

internal abstract class UpcastReader<TMessage> : UpcastReader
{
    internal abstract TMessage Read(DefaultMessageContext context);
}

internal sealed class SerializedReader<TMessage>(IMessageSerializer serializer) : UpcastReader<TMessage>
{
    internal override TMessage Read(DefaultMessageContext context)
    {
        try { return serializer.Deserialize<TMessage>(context.Body); }
        catch (OperationCanceledException) when (context.CancellationToken.IsCancellationRequested) { throw; }
        catch (Exception error) { throw new MessageDeserializationException(context.MessageType, error); }
    }
}
