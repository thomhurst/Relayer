using System.Collections.Frozen;
using System.Data.Common;
using System.Runtime.CompilerServices;
using Npgsql;
using Relayer.Pipeline;
using Relayer.Transports;

namespace Relayer.Outbox.PostgreSql;

// Dispatch is still owned by the shared workers. This adapter supplies only the local consume boundary.
internal sealed class PostgreSqlLocalTransport : ITransport, ITransportSendTracing
{
    private readonly NpgsqlDataSource _source;
    private readonly PostgreSqlOutboxOptions _options;
    private readonly FrozenSet<string> _endpoints;
    private readonly Dictionary<string, Receiver> _consumers = new(StringComparer.Ordinal);
    private readonly PostgreSqlLocalStatePool<PostgreSqlOutboxStore> _completionStores;
    private readonly FrozenDictionary<string, SendTracing>? _tracing;
    private readonly FrozenDictionary<string, MessageOperationTracing>? _settlementTracing;
    private readonly PostgreSqlLocalStatePool<SendState> _sendStates = new(16, static () => new());
    private readonly string _insert;
    private readonly RelayerInstrumentation? _instrumentation;

    internal PostgreSqlDirectDispatcher? Dispatcher
    {
        get; set;
    }

    internal sealed class SendState : IDisposable
    {
        internal readonly PostgreSqlOutboxBatch Batch = new();
        internal readonly List<BufferedOutboxMessage> Rows = [new()];

        public void Dispose()
        {
            Batch.Dispose();
        }
    }
    private readonly TransportCapabilities _capabilities = new() { OrderedByKey = true, MaxBatchSize = 1, Transactions = true };

    internal PostgreSqlLocalTransport(string name, NpgsqlDataSource source, PostgreSqlOutboxOptions options, IEnumerable<string> endpoints, MessageTracing? tracing = null)
    {
        Name = name;
        _source = source;
        _options = options;
        _instrumentation = tracing?.Instrumentation;
        _endpoints = endpoints.ToFrozenSet(StringComparer.Ordinal);
        _completionStores = new(64, () => new(source, options));
        _insert = PostgreSqlOutboxBatch.CreateInsertSql(PostgreSqlOutboxSchema.QuoteIdentifier(options.Schema ?? "public") + "." + PostgreSqlOutboxSchema.QuoteIdentifier(options.TablePrefix + "outbox"));

        if (tracing is not null)
        {
            _tracing = _endpoints.ToFrozenDictionary(endpoint => endpoint,
                endpoint => new SendTracing(tracing, new() { System = "postgresql", Destination = endpoint }, SendCoreAsync, SendBatchAsync), StringComparer.Ordinal);
            _settlementTracing = _endpoints.ToFrozenDictionary(endpoint => endpoint,
                endpoint => new MessageOperationTracing(tracing, new() { System = "postgresql", Destination = endpoint }, MessagingOperation.Settle, "complete"), StringComparer.Ordinal);
        }
    }

    public string Name
    {
        get;
    }

    public TransportCapabilities GetCapabilities(string endpoint)
    {
        if (!_endpoints.Contains(endpoint))
        {
            throw new InvalidOperationException($"PostgreSQL local transport '{Name}' has no endpoint '{endpoint}'. Declare a route or consumer.");
        }

        return _capabilities;
    }

    internal void Register(string endpoint, Func<PostgreSqlLocalDelivery, CancellationToken, ValueTask<Disposition>> receive, int concurrency = 1,
        int? maxAttempts = null, TimeSpan[]? redeliveryDelays = null, bool atLeastOnce = false)
    {
        if (!_consumers.TryAdd(endpoint, new(receive, concurrency, maxAttempts ?? _options.MaxDispatchAttempts, redeliveryDelays?.ToArray() ?? [], atLeastOnce)))
        {
            throw new InvalidOperationException($"Declare only one PostgreSQL local consumer for '{endpoint}'.");
        }
    }

    internal sealed class Receiver(Func<PostgreSqlLocalDelivery, CancellationToken, ValueTask<Disposition>> receive, int maximum,
        int maxAttempts, TimeSpan[] redeliveryDelays, bool atLeastOnce = false)
    {
        internal readonly Func<PostgreSqlLocalDelivery, CancellationToken, ValueTask<Disposition>> Receive = receive;
        internal readonly int MaxAttempts = maxAttempts;
        // The dispatcher commits the claim before the attempt and completes or fails the row in a separate fenced statement.
        internal readonly bool AtLeastOnce = atLeastOnce;
        internal readonly bool ConcurrentDirectDispatch = atLeastOnce && maximum > 1;

        // The failed attempt is one-based; the last configured delay repeats.
        internal TimeSpan RedeliveryDelay(int attempt)
        {
            return redeliveryDelays.Length == 0 ? TimeSpan.Zero : redeliveryDelays[Math.Min(attempt, redeliveryDelays.Length) - 1];
        }
        private int _active;

        internal bool TryEnter()
        {
            var active = Volatile.Read(ref _active);

            while (active < maximum)
            {
                var observed = Interlocked.CompareExchange(ref _active, active + 1, active);

                if (observed == active)
                {
                    return true;
                }

                active = observed;
            }

            return false;
        }

        internal void Exit()
        {
            Interlocked.Decrement(ref _active);
        }
    }

    internal PostgreSqlOutboxStore RentCompletionStore()
    {
        return _completionStores.Rent();
    }

    internal void ReturnCompletionStore(PostgreSqlOutboxStore store)
    {
        _completionStores.Return(store);
    }

    internal Receiver? GetReceiver(string endpoint)
    {
        return _consumers.GetValueOrDefault(endpoint);
    }

    internal Receiver? TryEnter(string endpoint)
    {
        return _consumers.TryGetValue(endpoint, out var receiver) && receiver.TryEnter() ? receiver : null;
    }

    internal void ValidateConsumers()
    {
        foreach (var endpoint in _endpoints)
        {
            if (!_consumers.ContainsKey(endpoint))
            {
                throw new InvalidOperationException($"PostgreSQL local transport '{Name}': endpoint '{endpoint}' has no consumer. Declare the same consumers on every dispatching instance.");
            }
        }
    }

    internal void ValidateStore(NpgsqlDataSource source, string? schema, string prefix, bool retainSentMessages, int maxDispatchAttempts)
    {
        if (!ReferenceEquals(source, _source) || (schema ?? "public") != (_options.Schema ?? "public") || prefix != _options.TablePrefix || retainSentMessages != _options.RetainSentMessages || maxDispatchAttempts != _options.MaxDispatchAttempts)
        {
            throw new InvalidOperationException($"PostgreSQL local transport '{Name}' and its dispatcher must use the same NpgsqlDataSource, schema, table prefix, sent retention setting and poison attempt limit.");
        }
    }

    internal static PostgreSqlLocalTransport? Find(ITransport transport)
    {
        while (transport is ITransportWrapper wrapper)
        {
            transport = wrapper.InnerTransport;
        }

        return transport as PostgreSqlLocalTransport;
    }

    [RuntimeAsyncMethodGeneration(false)]
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    internal async ValueTask ConsumeAsync(OutgoingMessage message, OutboxClaim claim, int attempt,
        NpgsqlTransaction? transaction, CancellationToken cancellationToken)
    {
        if (!_consumers.TryGetValue(message.Destination, out var consumer))
        {
            throw new InvalidOperationException($"PostgreSQL local transport '{Name}': register a consumer for '{message.Destination}' on every dispatching instance.");
        }

        if (consumer.AtLeastOnce)
        {
            // An uncommitted claim would block the fenced completion on its own row lock.
            if (transaction is not null)
            {
                throw new InvalidOperationException("An at-least-once PostgreSQL local attempt must run after its claim commits, outside the dispatch transaction.");
            }

            await ProcessAttemptAsync(consumer.Receive, new(message, claim, null!, attempt), cancellationToken).ConfigureAwait(false);
            return;
        }

        if (transaction is not null)
        {
            // A handler failure must not commit business changes when the lane records its retry/poison transition.
            await transaction.SaveAsync("relayer_local_attempt", cancellationToken).ConfigureAwait(false);

            try
            {
                await ProcessAttemptAsync(consumer.Receive, new(message, claim, transaction, attempt), cancellationToken).ConfigureAwait(false);
                await transaction.ReleaseAsync("relayer_local_attempt", cancellationToken).ConfigureAwait(false);
            }
            catch
            {
                // Rollback is required even after handler cancellation; the lane owner will dispose on shutdown.
                await transaction.RollbackAsync("relayer_local_attempt", CancellationToken.None).ConfigureAwait(false);
                throw;
            }

            return;
        }

        await using var connection = await _source.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var owned = await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);
        await owned.SaveAsync("relayer_local_attempt", cancellationToken).ConfigureAwait(false);
        await ProcessAttemptAsync(consumer.Receive, new(message, claim, owned, attempt), cancellationToken).ConfigureAwait(false);
        await owned.CommitAsync(cancellationToken).ConfigureAwait(false);
    }

    [RuntimeAsyncMethodGeneration(false)]
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    private async ValueTask ProcessAttemptAsync(Func<PostgreSqlLocalDelivery, CancellationToken, ValueTask<Disposition>> consumer,
        PostgreSqlLocalDelivery delivery, CancellationToken cancellationToken)
    {
        var store = _completionStores.Rent();

        try
        {
            var disposition = await consumer(delivery with
            {
                CompletionStore = store,
                Owner = this
            }, cancellationToken).ConfigureAwait(false);

            if (disposition.Kind != DispositionKind.SettledByHandler)
            {
                throw new InvalidOperationException("The PostgreSQL local receive pipeline must complete its fenced row inside the delivery transaction.");
            }

            if (delivery.OpenTransaction is { Connection: null })
            {
                throw new InvalidOperationException("Do not commit or dispose the local delivery transaction. The dispatcher owns its completion.");
            }
        }
        finally
        {
            _completionStores.Return(store);
        }
    }

    [RuntimeAsyncMethodGeneration(false)]
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    internal static async ValueTask SettleAsync(MessageContext context)
    {
        var native = context.Features.GetRequired<NativeMessage<PostgreSqlLocalDelivery>>();
        await native.Message.Owner.SettleAsync(native.Message, native.Disposition, context.CancellationToken).ConfigureAwait(false);
        native.Disposition = Disposition.SettledByHandler;
    }

    [RuntimeAsyncMethodGeneration(false)]
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    internal async ValueTask SettleAsync(PostgreSqlLocalDelivery delivery, Disposition disposition, CancellationToken cancellationToken)
    {
        if (disposition.Kind != DispositionKind.Complete)
        {
            throw new NotSupportedException("Local work uses the consumer's outbox retry and poison policy; native settlement overrides are unsupported.");
        }

        using var activity = _settlementTracing?.GetValueOrDefault(delivery.Message.Destination)?.Start(default);

        try
        {
            if (delivery.OpenTransaction is not { } transaction)
            {
                // The handler already ran, so a failed completion must not be recorded as a failed attempt.
                long completed;

                try
                {
                    completed = await delivery.CompletionStore.CompleteDirectAsync(delivery.Claim, cancellationToken).ConfigureAwait(false);
                }
                catch (DbException error)
                {
                    throw new PostgreSqlLocalCompletionFailure(error.Message, error);
                }

                if (completed != 1)
                {
                    throw new PostgreSqlLocalCompletionFailure("The PostgreSQL local claim was lost before completion. The handler already ran, so recovery delivers the row again; raise the claim or lease duration above the handler's run time.", null);
                }
            }
            else if (!await delivery.CompletionStore.CompleteAsync(delivery.Claim, transaction, cancellationToken).ConfigureAwait(false))
            {
                throw new InvalidOperationException("The PostgreSQL local claim was lost. Business work is rolled back and recovery retries the row.");
            }
        }
        catch
        {
            MessageOperationTracing.Fail(activity, "_OTHER");
            throw;
        }
    }

    [RuntimeAsyncMethodGeneration(false)]
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    internal static async ValueTask HandleFailureAsync(MessageContext context, Exception error, FailureAction action)
    {
        if (action != FailureAction.Discard)
        {
            throw new PostgreSqlLocalFailure(error, action);
        }

        var native = context.Features.GetRequired<NativeMessage<PostgreSqlLocalDelivery>>();

        if (native.Message.OpenTransaction is { } transaction)
        {
            await transaction.RollbackAsync("relayer_local_attempt", CancellationToken.None).ConfigureAwait(false);
        }

        native.Disposition = Disposition.Complete;
    }

    public SendTracing? GetSendTracing(string destination)
    {
        return _tracing?.GetValueOrDefault(destination);
    }

    public ValueTask SendAsync(OutgoingMessage message, CancellationToken cancellationToken)
    {
        return GetSendTracing(message.Destination) is { } tracing ? tracing.SendAsync(message, cancellationToken) : SendCoreAsync(message, cancellationToken);
    }

    [RuntimeAsyncMethodGeneration(false)]
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    private async ValueTask SendCoreAsync(OutgoingMessage message, CancellationToken cancellationToken)
    {
        _ = GetCapabilities(message.Destination);
        message.ValidateForOutbox();

        if (message.IsOutboxDispatch)
        {
            throw new InvalidOperationException("Use the claim-aware PostgreSQL dispatcher for durable local rows.");
        }

        WireString id = default, type = default;

        foreach (var header in message.Headers.Span)
        {
            if (header.Key == "ce_id")
            {
                id = header.Value;
            }

            if (header.Key == "ce_type")
            {
                type = header.Value;
            }
        }

        if (!TryReadId(id, out var messageId) || messageId.Version != 7 || type.IsEmpty)
        {
            throw new InvalidOperationException("PostgreSQL local sends require the original UUIDv7 ce_id and stable ce_type CloudEvents headers. Use ISender with the default envelope.");
        }

        var state = _sendStates.Rent();

        try
        {
            var record = new OutboxRecord(messageId, Name, type.ToString(), message, DateTimeOffset.UtcNow);
            BufferedOutboxMessage.Validate(record);
            state.Rows[0].Copy(record, _options.Lanes);
            await using var connection = await _source.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
            await using var transaction = await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);
            state.Batch.Prepare(transaction, _insert);
            state.Batch.Add(state.Rows[0]);
            GetSendTracing(message.Destination)?.RecordNativeSend();
            await state.Batch.ExecuteAsync(cancellationToken).ConfigureAwait(false);
            await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
            _instrumentation?.AddOutboxPending(1);
            Dispatcher?.Committed(state.Rows, 1);
        }
        finally
        {
            state.Rows[0].Clear();
            _sendStates.Return(state);
        }
    }

    private static bool TryReadId(WireString id, out Guid value)
    {
        Span<char> buffer = stackalloc char[36];

        if (id.TryFormat(buffer, out var written))
        {
            return Guid.TryParse(buffer[..written], out value);
        }

        value = default;
        return false;
    }

    [RuntimeAsyncMethodGeneration(false)]
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    public async ValueTask SendBatchAsync(ReadOnlyMemory<OutgoingMessage> messages, CancellationToken cancellationToken)
    {
        if (messages.Length > 1)
        {
            throw new NotSupportedException("PostgreSQL local transport declares MaxBatchSize = 1. Enlist multiple sends in one outbox transaction instead.");
        }

        if (messages.Length == 1)
        {
            await SendCoreAsync(messages.Span[0], cancellationToken).ConfigureAwait(false);
        }
    }

    public ValueTask<TopologyReport> ApplyTopologyAsync(TopologyMode mode, CancellationToken cancellationToken)
    {
        return new(new TopologyReport(Array.Empty<string>()));
    }

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
        _sendStates.Dispose();
        _completionStores.Dispose();
        return default;
    }
}

internal sealed class PostgreSqlLocalFailure(Exception error, FailureAction action) : ConsumeFailureHandoff(error)
{
    internal FailureAction Action
    {
        get;
    } = action;
}

// An at-least-once completion failed after the handler succeeded. A database error is the inner exception.
internal sealed class PostgreSqlLocalCompletionFailure(string message, DbException? error) : Exception(message, error);
