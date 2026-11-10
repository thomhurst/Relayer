namespace Relayer.Nats.Tests;

internal static class NatsSessionBroker
{
    private static Lazy<Task<NatsTopologyTests.Broker>> _shared = new(StartAsync);
    internal static Task<NatsTopologyTests.Broker> Current => Volatile.Read(ref _shared).Value;

    private static async Task<NatsTopologyTests.Broker> StartAsync()
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(2));
        return await NatsTopologyTests.Broker.StartAsync(timeout.Token);
    }

    [After(TestSession)]
    public static async Task StopAsync()
    {
        var shared = Interlocked.Exchange(ref _shared, new(StartAsync));
        if (!shared.IsValueCreated) return;
        NatsTopologyTests.Broker broker;
        try { broker = await shared.Value; }
        catch { return; } // Failed startup already disposes its container.
        await broker.DisposeAsync();
    }
}
