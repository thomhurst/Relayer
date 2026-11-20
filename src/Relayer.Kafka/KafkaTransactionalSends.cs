using Relayer.Transports;

namespace Relayer.Kafka;

// An exactly-once consumer's handler sends and failure copies to its own Kafka transport. Each run swaps in a
// transport over that run's transactional producer, so the records join the transaction Dekaf commits with the
// consumed offsets.
internal sealed class KafkaTransactionalSends(KafkaProducerTransport shared) : ITransport
{
    private KafkaProducerTransport? _run;

    internal KafkaProducerTransport? Run
    {
        get
        {
            return Volatile.Read(ref _run);
        }
        set
        {
            Volatile.Write(ref _run, value);
        }
    }

    public string Name => shared.Name;

    public TransportCapabilities GetCapabilities(string endpoint)
    {
        return shared.GetCapabilities(endpoint);
    }

    public ValueTask<TopologyReport> ApplyTopologyAsync(TopologyMode mode, CancellationToken cancellationToken)
    {
        return shared.ApplyTopologyAsync(mode, cancellationToken);
    }

    public ValueTask SendAsync(OutgoingMessage message, CancellationToken cancellationToken)
    {
        return Current.SendAsync(message, cancellationToken);
    }

    public ValueTask SendBatchAsync(ReadOnlyMemory<OutgoingMessage> messages, CancellationToken cancellationToken)
    {
        return Current.SendBatchAsync(messages, cancellationToken);
    }

    // Handlers only run inside a consumer run, which owns the transactional producer's lifetime.
    public Task StartAsync(CancellationToken cancellationToken)
    {
        return Task.CompletedTask;
    }

    public Task StopAsync(CancellationToken cancellationToken)
    {
        return Task.CompletedTask;
    }

    public ValueTask DisposeAsync()
    {
        return default;
    }

    private KafkaProducerTransport Current => Run
        ?? throw new InvalidOperationException($"Kafka transport '{shared.Name}': an exactly-once consumer sent outside its run. Its sends must join the run's transaction.");
}
