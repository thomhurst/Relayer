using System.Collections.Frozen;
using System.Runtime.CompilerServices;
using System.Threading.Channels;
using Microsoft.Extensions.Logging;
using Relayer.Transports;

namespace Relayer.InMemory;

/// <summary>Receives one native delivery and returns the core's settlement decision.</summary>
public delegate ValueTask<Disposition> InMemoryReceiveDelegate(InMemoryDelivery delivery, CancellationToken cancellationToken);

/// <summary>A bounded, process-local native transport. Instances do not share state.</summary>
public sealed class InMemoryTransport : ITransport, ITransportSendTracing
{
    private readonly FrozenDictionary<string, InMemoryEndpoint> _endpoints;
    private readonly FrozenDictionary<string, SendTracing>? _sendTracing;
    private CancellationTokenSource? _stop;
    private CancellationTokenSource? _abort;
    private bool _frozen;
    private bool _disposed;
    private int _running;

    /// <summary>The OpenTelemetry messaging system identifier used by shared tracing.</summary>
    public const string MessagingSystem = "relayer.inmemory";

    /// <summary>Creates explicit native endpoints. No unknown destination is auto-created.</summary>
    public InMemoryTransport(string name, IReadOnlyDictionary<string, InMemoryEndpointOptions> endpoints, TimeProvider? timeProvider = null,
        MessageTracing? tracing = null, ILoggerFactory? loggerFactory = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        ArgumentNullException.ThrowIfNull(endpoints);
        Name = name;
        var built = new Dictionary<string, InMemoryEndpoint>(StringComparer.Ordinal);
        foreach (var pair in endpoints)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(pair.Key);
            built.Add(pair.Key, new(pair.Key, pair.Value, timeProvider ?? TimeProvider.System, tracing));
        }
        _endpoints = built.ToFrozenDictionary(StringComparer.Ordinal);
        if (tracing is not null)
        {
            var operations = new Dictionary<string, SendTracing>(StringComparer.Ordinal);
            var logger = loggerFactory?.CreateLogger<InMemoryTransport>();
            foreach (var endpoint in endpoints.Keys)
                operations.Add(endpoint, new(tracing, new() { System = MessagingSystem, Destination = endpoint },
                    SendCoreAsync, SendBatchCoreAsync, logger));
            _sendTracing = operations.ToFrozenDictionary(StringComparer.Ordinal);
        }
    }

    /// <inheritdoc />
    public string Name { get; }

    internal void Observe(IRelayerObserver? observer)
    {
        foreach (var endpoint in _endpoints.Values) endpoint.Observer = observer;
    }
    /// <summary>Completes when all native loops stop; faults on unexpected processing or settlement failures.</summary>
    public Task Completion { get; private set; } = Task.CompletedTask;
    /// <inheritdoc />
    public TransportCapabilities GetCapabilities(string endpoint) => Endpoint(endpoint).Capabilities;

    /// <inheritdoc />
    public SendTracing? GetSendTracing(string destination)
    {
        if (_sendTracing is null) return null;
        return _sendTracing.TryGetValue(destination, out var tracing) ? tracing
            : throw new InvalidOperationException($"In-memory transport '{Name}' has no endpoint '{destination}'. Declare it before building the transport.");
    }

    /// <inheritdoc />
    public ValueTask<TopologyReport> ApplyTopologyAsync(TopologyMode mode, CancellationToken cancellationToken)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        cancellationToken.ThrowIfCancellationRequested();
        if (!Enum.IsDefined(mode)) throw new ArgumentOutOfRangeException(nameof(mode));
        // Explicit constructor declarations are the native entities; no external broker state is provisioned.
        return new(new TopologyReport(Array.Empty<string>()));
    }

    /// <summary>Registers native receivers before first start. Logs require a unique group per registration.</summary>
    /// <remarks>Ordered endpoints and logs require concurrency equal to Partitions, with one serial receiver per partition.</remarks>
    public void RegisterConsumer(string endpoint, InMemoryReceiveDelegate receive, string? group = null, int concurrency = 1)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (_frozen) throw new InvalidOperationException("Register in-memory consumers before the first StartAsync.");
        ArgumentNullException.ThrowIfNull(receive);
        Endpoint(endpoint).Register(receive, group, concurrency);
    }

    /// <summary>Reads independently owned native dead-letter snapshots.</summary>
    /// <remarks>Snapshots are unbounded and excluded from native admission capacity. Drain the reader to release their copied bodies.</remarks>
    public ChannelReader<InMemoryDeadLetter> DeadLetters(string endpoint) => Endpoint(endpoint).DeadLetters.Reader;

    /// <summary>Returns the last completed log position for one group and partition, or -1 before processing.</summary>
    public long GetCommittedPosition(string endpoint, string group, int partition) => Endpoint(endpoint).GetCommittedPosition(group, partition);

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    private async ValueTask SendCoreAsync(OutgoingMessage message, CancellationToken cancellationToken)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        cancellationToken.ThrowIfCancellationRequested();
        var endpoint = Endpoint(message.Destination);
        await endpoint.ReserveAsync(cancellationToken).ConfigureAwait(false);
        var native = endpoint.Rent();
        try
        {
            native.Copy(message);
            message.ApplyNative(native);
            endpoint.ValidateSend(native);
            GetSendTracing(message.Destination)?.RecordNativeSend();
            await endpoint.EnqueueAsync(native, cancellationToken).ConfigureAwait(false);
        }
        catch
        {
            endpoint.Release(native);
            throw;
        }
    }

    /// <inheritdoc />
    public ValueTask SendAsync(OutgoingMessage message, CancellationToken cancellationToken) =>
        GetSendTracing(message.Destination) is { } tracing ? tracing.SendAsync(message, cancellationToken) : SendCoreAsync(message, cancellationToken);

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    private async ValueTask SendBatchCoreAsync(ReadOnlyMemory<OutgoingMessage> messages, CancellationToken cancellationToken)
    {
        ValidateBatch(messages, cancellationToken);
        // Native batches preserve input order and allow partial completion, as specified by ITransport.
        for (var i = 0; i < messages.Length; i++)
            await SendCoreAsync(messages.Span[i], cancellationToken).ConfigureAwait(false);
    }

    private void ValidateBatch(ReadOnlyMemory<OutgoingMessage> messages, CancellationToken cancellationToken)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        cancellationToken.ThrowIfCancellationRequested();
        for (var i = 0; i < messages.Length; i++)
        {
            var message = messages.Span[i];
            if (messages.Length > Endpoint(message.Destination).Capabilities.MaxBatchSize)
                throw new InvalidOperationException($"Endpoint '{message.Destination}': batch exceeds MaxBatchSize.");
        }
    }

    /// <inheritdoc />
    /// <remarks>The total batch length must fit every destination's MaxBatchSize. Mixed destinations retain native input order and use separate single-message spans.</remarks>
    public ValueTask SendBatchAsync(ReadOnlyMemory<OutgoingMessage> messages, CancellationToken cancellationToken)
    {
        if (messages.IsEmpty || GetSendTracing(messages.Span[0].Destination) is not { } tracing)
            return SendBatchCoreAsync(messages, cancellationToken);
        var destination = messages.Span[0].Destination;
        for (var i = 1; i < messages.Length; i++)
            if (messages.Span[i].Destination != destination) return SendMixedBatchAsync(messages, cancellationToken);
        return tracing.SendBatchAsync(messages, cancellationToken);
    }

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    private async ValueTask SendMixedBatchAsync(ReadOnlyMemory<OutgoingMessage> messages, CancellationToken cancellationToken)
    {
        ValidateBatch(messages, cancellationToken);
        for (var i = 0; i < messages.Length; i++)
            await SendAsync(messages.Span[i], cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public Task StartAsync(CancellationToken cancellationToken)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        cancellationToken.ThrowIfCancellationRequested();
        if (Interlocked.CompareExchange(ref _running, 1, 0) != 0)
            throw new InvalidOperationException("The in-memory transport is already running or stopping.");
        _frozen = true;
        _stop?.Dispose();
        _abort?.Dispose();
        _stop = new();
        _abort = new();
        var tasks = new List<Task>();
        // Native background receive loops must not inherit the caller's ambient producer Activity or scope.
        try
        {
            if (ExecutionContext.IsFlowSuppressed()) StartLoops(tasks);
            else
            {
                using (ExecutionContext.SuppressFlow()) StartLoops(tasks);
            }
        }
        catch
        {
            _stop.Cancel();
            _abort.Cancel();
            Completion = ObserveCompletionAsync(Task.WhenAll(tasks));
            throw;
        }
        foreach (var task in tasks)
            _ = task.ContinueWith(static (_, state) =>
            {
                try { ((CancellationTokenSource)state!).Cancel(); }
                catch (ObjectDisposedException) { /* A completed run may already have been restarted or disposed. */ }
            }, _stop,
                CancellationToken.None, TaskContinuationOptions.OnlyOnFaulted | TaskContinuationOptions.ExecuteSynchronously, TaskScheduler.Default);
        Completion = ObserveCompletionAsync(Task.WhenAll(tasks));
        return Task.CompletedTask;
    }

    private void StartLoops(List<Task> tasks)
    {
        foreach (var endpoint in _endpoints.Values) endpoint.Start(tasks, _stop!.Token, _abort!.Token);
    }

    private async Task ObserveCompletionAsync(Task loops)
    {
        try { await loops.ConfigureAwait(false); }
        finally { Interlocked.Exchange(ref _running, 0); }
    }

    /// <inheritdoc />
    public async Task StopAsync(CancellationToken cancellationToken)
    {
        if (_stop is null) return;
        _stop.Cancel(); // Stop fetching; allow current handlers to finish before settlement.
        using var registration = cancellationToken.UnsafeRegister(static state => ((CancellationTokenSource)state!).Cancel(), _abort);
        try { await Completion.WaitAsync(cancellationToken).ConfigureAwait(false); }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            // WaitAsync may unregister the abort callback while cancellation callbacks are still running.
            _abort!.Cancel();
            throw;
        }
    }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        if (_disposed) return;
        try { await StopAsync(CancellationToken.None).ConfigureAwait(false); }
        catch (Exception) when (Completion.IsFaulted) { /* Completion and the hosted service report processing failures. */ }
        finally
        {
            _disposed = true;
            _stop?.Dispose();
            _abort?.Dispose();
            foreach (var endpoint in _endpoints.Values) endpoint.Dispose();
        }
    }

    private InMemoryEndpoint Endpoint(string name) => _endpoints.TryGetValue(name, out var endpoint) ? endpoint
        : throw new InvalidOperationException($"In-memory transport '{Name}' has no endpoint '{name}'. Declare it before building the transport.");
}
