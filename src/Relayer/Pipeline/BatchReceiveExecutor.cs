using System.Runtime.CompilerServices;
using Microsoft.Extensions.DependencyInjection;
using Relayer.Transports;

namespace Relayer;

/// <summary>Receives native deliveries into borrowed caller storage, respecting native count and byte limits.</summary>
/// <returns>The number written, from zero through destination.Length.</returns>
/// <remarks>
/// Return zero when maximumWait expires without a delivery. Honor cancellation and maximumWait, and do not
/// retain destination. Keep delivered bodies and headers alive until processing and native settlement finish.
/// The client-side executor always passes one slot; a native executor can fill multiple slots.
/// </remarks>
public delegate ValueTask<int> BatchReceiveDelegate<TIncoming>(Memory<TIncoming> destination, TimeSpan maximumWait, CancellationToken cancellationToken);

/// <summary>Selects an executable native batch receive or the core's client-side accumulation at startup.</summary>
/// <remarks>
/// Compose before starting receives. Configuration and capabilities alone are not executors. Native batching
/// takes precedence and is capped by both declared limits; native byte limits may return a smaller batch.
/// The fallback accumulates individually received deliveries without changing ordering or settlement.
/// </remarks>
public sealed class BatchReceiveExecutor<TIncoming>
{
    private readonly BatchReceiveDelegate<TIncoming> _receive;
    private readonly TimeSpan _wait;
    private readonly TimeProvider _time;

    /// <summary>Validates the endpoint and chooses an available executor for this consumer declaration.</summary>
    public BatchReceiveExecutor(IServiceProvider services, string transportName, IConsumerDefinition consumer, string consumerName,
        BatchReceiveDelegate<TIncoming>? nativeReceive = null, BatchReceiveDelegate<TIncoming>? singleReceive = null,
        TimeProvider? timeProvider = null)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(consumer);
        ArgumentException.ThrowIfNullOrWhiteSpace(transportName);
        ArgumentException.ThrowIfNullOrWhiteSpace(consumerName);
        ArgumentException.ThrowIfNullOrWhiteSpace(consumer.Source);
        var location = $"Consumer '{consumerName}' from '{consumer.Source}' on transport '{transportName}'";
        if (consumer.BatchSize < 1 || consumer.BatchWait <= TimeSpan.Zero)
            throw new InvalidOperationException($"{location}: BatchSize must be positive and BatchWait must be positive.");
        ITransport? selected = null;
        foreach (var transport in services.GetServices<ITransport>())
        {
            if (transport.Name != transportName) continue;
            if (selected is not null) throw new InvalidOperationException($"{location}: register exactly one named ITransport.");
            selected = transport;
        }
        if (selected is null) throw new InvalidOperationException($"{location}: register a named ITransport adapter.");
        var errors = new List<string>();
        var validator = new EndpointCapabilityValidator([selected], errors, services.GetRequiredService<EndpointCapabilityCache>());
        var capabilities = validator.GetCapabilities(selected, consumer.Source);
        if (errors.Count != 0 || capabilities is null)
            throw new InvalidOperationException($"{location}: {string.Join(Environment.NewLine, errors)}");
        UsesNativeBatching = nativeReceive is not null && (capabilities.MaxBatchSize > 1 || consumer.BatchSize == 1);
        MaximumSize = UsesNativeBatching ? Math.Min(consumer.BatchSize, capabilities.MaxBatchSize) : consumer.BatchSize;
        _receive = (UsesNativeBatching ? nativeReceive : singleReceive)
            ?? throw new InvalidOperationException($"{location}: BatchSize {consumer.BatchSize} has no executor. Register native batch receive within MaxBatchSize or a single-message receive executor for core client-side batching.");
        _wait = consumer.BatchWait;
        _time = timeProvider ?? TimeProvider.System;
    }

    /// <summary>Whether native batching was selected at startup.</summary>
    public bool UsesNativeBatching { get; }
    /// <summary>The effective count limit, capped by the native limit when using native batching.</summary>
    public int MaximumSize { get; }

    /// <summary>Receives one batch. Process only the returned prefix, then settle using the native client.</summary>
    /// <remarks>
    /// Caller owns all received deliveries even if a later receive fails. On exceptions, drain or release them
    /// using the native client's shutdown/redelivery mechanism; never acknowledge them as successful.
    /// One call is one receive loop. Concurrent calls require independent destination storage.
    /// </remarks>
    [RuntimeAsyncMethodGeneration(false)]
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    public async ValueTask<int> ReceiveAsync(Memory<TIncoming> destination, CancellationToken cancellationToken = default)
    {
        if (destination.Length < MaximumSize) throw new ArgumentException("Provide storage for MaximumSize deliveries.", nameof(destination));
        cancellationToken.ThrowIfCancellationRequested();
        if (UsesNativeBatching)
        {
            var nativeCount = ValidateCount(await _receive(destination[..MaximumSize], _wait, cancellationToken).ConfigureAwait(false), MaximumSize);
            cancellationToken.ThrowIfCancellationRequested();
            return nativeCount;
        }
        var count = 0;
        long started = 0;
        while (count < MaximumSize)
        {
            var remaining = count == 0 ? _wait : _wait - _time.GetElapsedTime(started);
            if (remaining <= TimeSpan.Zero) break;
            cancellationToken.ThrowIfCancellationRequested();
            var received = ValidateCount(await _receive(destination.Slice(count, 1), remaining, cancellationToken).ConfigureAwait(false), 1);
            cancellationToken.ThrowIfCancellationRequested();
            if (received == 0) break;
            if (count++ == 0) started = _time.GetTimestamp();
        }
        return count;
    }

    private static int ValidateCount(int count, int maximum) => (uint)count <= (uint)maximum ? count
        : throw new InvalidOperationException("The receive executor returned a count outside its supplied destination.");
}
