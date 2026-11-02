namespace Relayer;

/// <summary>A context rented from a consumer's pool. Dispose it after awaiting processing.</summary>
/// <remarks>Do not retain a context or use it after disposal. Native features can be populated before processing.</remarks>
public sealed class DefaultMessageContext : MessageContext, IDisposable
{
    private readonly ConsumePipeline _owner;
    private ConsumeMessage _message;
    private ReceiveMessageState? _receive;
    private CancellationToken _cancellation;
    private IServiceProvider? _services;
    // 0: pooled, 1: rented, 2: processing, 3: processed. A delivery can run only once.
    private int _state;

    internal int PoolSlot { get; }
    internal DefaultMessageContext(ConsumePipeline owner, int poolSlot = -1)
    {
        _owner = owner;
        PoolSlot = poolSlot;
        Settlement = new(this, owner);
    }

    internal PendingSettlement Settlement { get; }
    internal bool CanRequestSettlement => Volatile.Read(ref _state) == 2 && _services is not null;

    /// <inheritdoc />
    public override string MessageId => _receive?.MessageId ?? _message.MessageId;
    /// <inheritdoc />
    public override MessageType MessageType => _receive?.MessageType ?? _message.MessageType;
    /// <inheritdoc />
    public override MessageHeaders Headers => _receive ?? _message.Headers ?? MessageHeaders.Empty;
    /// <inheritdoc />
    public override int Attempt => _attempt > 0 ? _attempt : _receive?.Attempt ?? _message.Attempt;
    /// <inheritdoc />
    public override IServiceProvider Services => _services ?? throw new InvalidOperationException("Services are available only during message processing.");
    /// <inheritdoc />
    public override MessageFeatures Features { get; } = new();
    /// <inheritdoc />
    public override CancellationToken CancellationToken => _cancellation;

    internal System.Buffers.ReadOnlySequence<byte> Body => _receive is { } receive ? receive.Body : _message.Body;
    internal bool IsTombstone => _receive?.IsTombstone ?? false;
    internal WireString? TombstoneKey => _receive?.TombstoneKey;
    private int _attempt;
    internal int Redeliveries { get; private set; }
    internal FailureHistory? FailureHistory { get; set; }
    internal Disposition? FailureDisposition { get; set; }
    internal Disposition ObservedDisposition => _receive?.Disposition ?? FailureDisposition ?? Disposition.Complete;
    internal void ApplyFailureDisposition()
    {
        if (FailureDisposition is { } disposition) _receive?.SetDisposition(disposition);
    }
    internal AttemptSendBuffer SendBuffer { get; } = new();
    internal void InitializeAttempts()
    {
        _attempt = Math.Max(Attempt, checked(FailureMetadata.ReadCount(Headers, FailureHeaders.Attempts) + 1));
        Redeliveries = FailureMetadata.ReadCount(Headers, FailureHeaders.Redeliveries);
    }
    internal void NextAttempt() => _attempt = checked(Attempt + 1);
    internal ConsumeDelegate Dispatch { get; set; } = null!;
    internal HeaderNames EnvelopeNames => _receive?.EnvelopeNames ?? _owner.EnvelopeNames;
    internal string? TracePartitionId => _receive?.TracePartitionId;
    internal void EnrichActivity(System.Diagnostics.Activity activity) => _receive?.EnrichActivity(activity);
    internal Exception? LoggedSettlementError { get; set; }
    internal void LogSettlementFailure(Exception error) => _owner.LogSettlementFailure(this, error);
    internal bool TryConsumeNativeSettlementError(Exception error) => _receive?.TryConsumeLoggedSettlementError(error) == true;

    // Read raw native fields so malformed envelopes remain loggable without parsing required fields.
    internal string? GetLogMessageId()
    {
        if (_receive is null) return _message.MessageId;
        return _receive.TryGetValue(EnvelopeNames.Id, out var id) ? id.ToString() : null;
    }

    internal string? GetLogMessageType()
    {
        if (_receive is null) return _owner.GetWireIdentityText(_message.MessageType);
        return _receive.TryGetValue(EnvelopeNames.Type, out var type) ? type.ToString() : null;
    }

    internal void Initialize(in ConsumeMessage message, CancellationToken cancellationToken)
    {
        _message = message;
        _cancellation = cancellationToken;
        _state = 1;
    }

    internal void Initialize(ReceiveMessageState receive, CancellationToken cancellationToken)
    {
        _receive = receive;
        _cancellation = cancellationToken;
        _state = 1;
    }

    internal void BeginProcessing(ConsumePipeline owner)
    {
        if (!ReferenceEquals(owner, _owner) || Interlocked.CompareExchange(ref _state, 2, 1) != 1)
        {
            throw new InvalidOperationException("The context must be rented from this pipeline and cannot be processed concurrently or after disposal.");
        }
        Features.Set<ISettlementFeature>(Settlement);
    }

    internal void SetServices(IServiceProvider services) => _services = services;
    internal void ClearServices() => _services = null;

    internal void EndProcessing()
    {
        _services = null;
        Volatile.Write(ref _state, 3);
    }

    /// <summary>Clears message references and native features, then returns this context to its consumer.</summary>
    public void Dispose()
    {
        var previous = Volatile.Read(ref _state);
        if (previous == 0) return;
        if (previous == 2 || Interlocked.CompareExchange(ref _state, 0, previous) != previous)
        {
            throw new InvalidOperationException("Await message processing before disposing its context.");
        }
        Settlement.Clear();
        _message = default;
        _attempt = 0;
        Redeliveries = 0;
        FailureHistory = null;
        FailureDisposition = null;
        SendBuffer.Clear();
        _receive = null;
        _cancellation = default;
        _services = null;
        Dispatch = null!;
        LoggedSettlementError = null;
        Features.Clear();
        _owner.Return(this);
    }
}
