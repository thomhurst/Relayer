namespace Relayer;

/// <summary>
/// A small, allocation-free typed feature collection. Transports expose native objects through it,
/// following the ASP.NET Core <c>HttpContext.Features</c> pattern (docs/decisions.md D6).
/// </summary>
public sealed class MessageFeatures
{
    private Entry[] _items = new Entry[4];
    private int _count;

    /// <summary>Gets the feature of type <typeparamref name="TFeature"/>, or <see langword="null"/>.</summary>
    public TFeature? Get<TFeature>() where TFeature : class
    {
        var items = _items;
        for (var i = 0; i < _count; i++)
        {
            if (items[i].Type == typeof(TFeature))
            {
                return (TFeature)items[i].Instance;
            }
        }

        return null;
    }

    /// <summary>Gets the feature of type <typeparamref name="TFeature"/>, or throws if absent.</summary>
    /// <exception cref="InvalidOperationException">The feature is not available on this message.</exception>
    public TFeature GetRequired<TFeature>() where TFeature : class =>
        Get<TFeature>() ?? throw new InvalidOperationException(
            $"Feature {typeof(TFeature).Name} is not available. The message may come from a different transport.");

    /// <summary>Sets or replaces the feature of type <typeparamref name="TFeature"/>.</summary>
    public void Set<TFeature>(TFeature instance) where TFeature : class
    {
        ArgumentNullException.ThrowIfNull(instance);

        for (var i = 0; i < _count; i++)
        {
            if (_items[i].Type == typeof(TFeature))
            {
                _items[i] = new Entry(typeof(TFeature), instance);
                return;
            }
        }

        if (_count == _items.Length)
        {
            Array.Resize(ref _items, _items.Length * 2);
        }

        _items[_count++] = new Entry(typeof(TFeature), instance);
    }

    /// <summary>Removes all features so the instance can be pooled and reused.</summary>
    public void Clear()
    {
        Array.Clear(_items, 0, _count);
        _count = 0;
    }

    private readonly record struct Entry(Type Type, object Instance);
}
