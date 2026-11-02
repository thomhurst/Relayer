using System.Threading.Channels;
using Dekaf;
using Dekaf.Consumer;

namespace Relayer.Kafka;

internal enum KafkaRebalanceKind { Assigned, Revoked, Lost }

internal sealed class KafkaRebalance(KafkaRebalanceKind kind, TopicPartition[] partitions, CancellationToken token)
{
    internal KafkaRebalanceKind Kind { get; } = kind;
    internal TopicPartition[] Partitions { get; } = partitions;
    internal CancellationToken Token { get; } = token;
    // Dekaf scopes revocation commits and assigned pause/seek staging to the callback's flow.
    // Keep that native scope when the single owner pumps this callback's work.
    internal ExecutionContext? Context { get; } = ExecutionContext.Capture();
    internal KafkaConsumer Owner = null!;
    internal int CommitCount;
    internal ValueTask Commit;
    internal TaskCompletionSource Completion { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
}

internal sealed class KafkaOwnership : IRebalanceListener
{
    private Channel<KafkaRebalance> _commands = Channel.CreateUnbounded<KafkaRebalance>(
        new() { SingleReader = true, SingleWriter = false, AllowSynchronousContinuations = false });
    internal Action? Wake { get; set; }
    private int _pending;
    private long _version;
    internal long Version => Volatile.Read(ref _version);
    internal void Restart()
    {
        _commands = Channel.CreateUnbounded<KafkaRebalance>(
            new() { SingleReader = true, SingleWriter = false, AllowSynchronousContinuations = false });
        Volatile.Write(ref _pending, 0);
    }
    internal bool HasCommands => Volatile.Read(ref _pending) != 0;
    internal bool TryRead(out KafkaRebalance? command)
    {
        if (!_commands.Reader.TryRead(out command)) return false;
        Interlocked.Decrement(ref _pending);
        return true;
    }
    internal void Stop(Exception error)
    {
        _commands.Writer.TryComplete(error);
        while (_commands.Reader.TryRead(out var command)) command.Completion.TrySetException(error);
    }

    private ValueTask Enqueue(KafkaRebalanceKind kind, IEnumerable<TopicPartition> partitions, CancellationToken token)
    {
        var command = new KafkaRebalance(kind, partitions.ToArray(), token);
        Interlocked.Increment(ref _version);
        Interlocked.Increment(ref _pending);
        if (!_commands.Writer.TryWrite(command))
        {
            Interlocked.Decrement(ref _pending);
            return default; // Close after the pump drained and fenced ownership.
        }
        Wake?.Invoke();
        return new(command.Completion.Task);
    }

    public ValueTask OnPartitionsAssignedAsync(IEnumerable<TopicPartition> partitions, CancellationToken cancellationToken) =>
        Enqueue(KafkaRebalanceKind.Assigned, partitions, cancellationToken);
    public ValueTask OnPartitionsRevokedAsync(IEnumerable<TopicPartition> partitions, CancellationToken cancellationToken) =>
        Enqueue(KafkaRebalanceKind.Revoked, partitions, cancellationToken);
    public ValueTask OnPartitionsLostAsync(IEnumerable<TopicPartition> partitions, CancellationToken cancellationToken) =>
        Enqueue(KafkaRebalanceKind.Lost, partitions, cancellationToken);
}
