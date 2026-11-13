namespace Relayer.GooglePubSub;

// Bound retained native-boundary state (D52); overflow is collected and misses allocate.
internal sealed class GooglePubSubPool<T>(Func<T> create) where T : class
{
    internal const int Capacity = 256;
    private readonly SlotPool<T> _items = new(capacity: Capacity);

    internal T Rent() => _items.TryRent() ?? create();
    internal void Return(T item) => _items.TryReturn(item);
}
