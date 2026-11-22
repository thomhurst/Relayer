using System.Collections.Frozen;
using Microsoft.Extensions.DependencyInjection;
using Relayer.Serialization;
using Relayer.Transports;

namespace Relayer.Pipeline;

/// <summary>Composes one consumer at startup. Middleware instances must support concurrent calls.</summary>
public sealed class ConsumePipelineBuilder
{
    private readonly IServiceScopeFactory _scopes;
    private readonly MessageTypeRegistry _messages;
    private readonly IMessageSerializer _serializer;
    private readonly ConsumeFailureDelegate _failure;
    private readonly IFailureClassifier _classifier;
    private readonly FailurePipeline? _failures;
    private readonly Dictionary<MessageType, ConsumeDelegate> _dispatch = [];
    private readonly Dictionary<MessageType, ConsumeDelegate> _tombstones = [];
    private readonly List<HandlerDescriptor> _handlers = [];
    private readonly List<IConsumeMiddleware> _middleware = [];
    private readonly List<AcceptedIdentity> _accepted = [];
    private bool _built;
    private EndpointTelemetry? _telemetry;
    private ConsumeDelegate? _settlement;
    private Action<Disposition>? _validateSettlementRequest;
    private ConsumeTracing? _tracing;

    /// <summary>Builds a homogeneous batch consumer using its existing size, wait and concurrency declaration.</summary>
    /// <remarks>Batch handlers coordinate their own per-message work. Tracing and native settlement callbacks are supported; single-message middleware and metrics are not.</remarks>
    public ConsumePipeline BuildBatch(HandlerDescriptor descriptor, IConsumerDefinition consumer,
        System.Diagnostics.ActivitySource? activitySource = null)
    {
        EnsureMutable();
        ArgumentNullException.ThrowIfNull(descriptor);
        ArgumentNullException.ThrowIfNull(consumer);

        if (_telemetry is not null)
        {
            throw new InvalidOperationException("Batch consumers do not support single-message metrics. Record batch metrics around ProcessBatchAsync.");
        }

        if (_tracing is not null && activitySource is not null)
        {
            throw new InvalidOperationException("Use Tracing for the shared Relayer source, or supply an ActivitySource, but not both.");
        }

        if (_dispatch.Count != 0 || _tombstones.Count != 0 || _middleware.Count != 0)
        {
            throw new InvalidOperationException("Compose batch handlers separately from single-message handlers and middleware.");
        }

        if (!_messages.IsFrozen || !_messages.TryGetMessageType(descriptor.MessageType, out var identity))
        {
            throw new InvalidOperationException("Register and freeze the batch message's stable wire identity before composition.");
        }

        ArgumentOutOfRangeException.ThrowIfLessThan(consumer.BatchSize, 1);
        ArgumentOutOfRangeException.ThrowIfLessThan(consumer.Concurrency, 1);

        if (consumer.BatchWait <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(consumer));
        }

        using var compositionScope = _scopes.CreateScope();

        if (compositionScope.ServiceProvider.GetService<TenantContextPool>() is not null)
        {
            throw new InvalidOperationException("Tenant propagation requires single-message consumers. Batch consumers share one DI scope; use individual delivery scopes.");
        }

        var readers = compositionScope.ServiceProvider.GetService<UpcasterRegistry>()?.Compose(descriptor.MessageType, _messages, _serializer) ?? [];
        var batch = descriptor.CreateBatchDispatcher(_serializer, consumer.BatchSize, consumer.Concurrency, readers);
        compositionScope.ServiceProvider.GetService<SenderPool>()?.Enable();
        _built = true;
        var additional = new HashSet<MessageType>(readers.Keys);

        foreach (var accepted in _accepted)
        {
            if (accepted.MessageType != descriptor.MessageType)
            {
                throw new InvalidOperationException($"Consumer accepts '{accepted.Identity}' for '{accepted.MessageType}', but its batch handler handles '{descriptor.MessageType}'.");
            }

            if (accepted.Identity == identity || !additional.Add(accepted.Identity))
            {
                throw new InvalidOperationException($"Consumer already handles '{accepted.Identity}'. Accept each incoming identity once.");
            }
        }

        ConsumeDelegate batchOnly = static _ => throw new InvalidOperationException("Use ProcessBatchAsync for a batch handler.");
        var identities = new Dictionary<MessageType, ConsumeDelegate> { [identity] = batchOnly };

        foreach (var source in additional)
        {
            identities.Add(source, batchOnly);
        }

        return new ConsumePipeline(_scopes, identities.ToFrozenDictionary(), [], _failure, _classifier,
            checked(consumer.BatchSize * consumer.Concurrency), _failures, null, _settlement, _tracing,
            validateSettlementRequest: _validateSettlementRequest)
        {
            BatchExecution = new BatchExecution(_scopes, batch, identity, consumer, _failure, _classifier, _failures, activitySource, _tracing,
                additional.Count == 0 ? null : additional.ToFrozenSet())
        };
    }

    /// <summary>Creates a consumer composer with an explicit failure pipeline.</summary>
    /// <remarks>The failure delegate must finish durable failure handling before returning successfully.</remarks>
    public ConsumePipelineBuilder(IServiceScopeFactory scopes, MessageTypeRegistry messages,
        IMessageSerializer serializer, ConsumeFailureDelegate failure, IFailureClassifier? classifier = null)
    {
        ArgumentNullException.ThrowIfNull(scopes);
        ArgumentNullException.ThrowIfNull(messages);
        ArgumentNullException.ThrowIfNull(serializer);
        ArgumentNullException.ThrowIfNull(failure);
        _scopes = scopes;
        _messages = messages;
        _serializer = serializer;
        _failure = failure;
        _classifier = classifier ?? new DefaultFailureClassifier();
    }

    /// <summary>Creates a consumer with executable, startup-validated retry and durable failure handling.</summary>
    public ConsumePipelineBuilder(IServiceScopeFactory scopes, MessageTypeRegistry messages,
        IMessageSerializer serializer, FailurePipeline failures, IFailureClassifier? classifier = null)
        : this(scopes, messages, serializer, static (_, error, _) => ValueTask.FromException(error), classifier)
    {
        ArgumentNullException.ThrowIfNull(failures);
        _failures = failures;
    }

    /// <summary>Selects a generated single-message dispatcher and its registered wire identity at startup.</summary>
    public ConsumePipelineBuilder Handler(HandlerDescriptor descriptor)
    {
        EnsureMutable();
        ArgumentNullException.ThrowIfNull(descriptor);

        if (!_messages.IsFrozen)
        {
            throw new InvalidOperationException("Freeze message registration before composing a consumer.");
        }

        if (!_messages.TryGetMessageType(descriptor.MessageType, out var identity))
        {
            throw new InvalidOperationException($"Register a stable wire identity for '{descriptor.MessageType}' before composing its consumer.");
        }

        var dispatch = descriptor.CreateConsumeDelegate(_serializer);
        var table = descriptor.IsTombstone ? _tombstones : _dispatch;

        if (!table.TryAdd(identity, dispatch))
        {
            throw new InvalidOperationException($"Consumer already handles '{identity}'. Use one handler to coordinate work for this message type.");
        }

        _handlers.Add(descriptor);
        return this;
    }

    /// <summary>
    /// Dispatches deliveries with extra wire identities to this consumer's handler for <typeparamref name="TMessage"/>,
    /// deserialized as that type. Receive only: sends keep the type's one registered identity.
    /// </summary>
    /// <remarks>
    /// <see cref="MessageContext.MessageType"/> reports the identity that arrived. An identity may belong to another
    /// registered type that this consumer does not handle. Applies to a <see cref="Tombstone{T}"/> handler for the same
    /// type too. Validated when the consumer is built.
    /// </remarks>
    public ConsumePipelineBuilder Accepts<TMessage>(params ReadOnlySpan<MessageType> identities)
    {
        EnsureMutable();
        ValidateAccepted(identities);

        foreach (var identity in identities)
        {
            _accepted.Add(new(typeof(TMessage), typeof(Tombstone<TMessage>), identity));
        }

        return this;
    }

    internal static void ValidateAccepted(ReadOnlySpan<MessageType> identities)
    {
        if (identities.IsEmpty)
        {
            throw new ArgumentException("Accept at least one wire identity.", nameof(identities));
        }

        foreach (var identity in identities)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(identity.Name, nameof(identities));
            ArgumentOutOfRangeException.ThrowIfLessThan(identity.Version, 1, nameof(identities));
        }
    }

    /// <summary>Adds middleware. The first registered middleware is outermost.</summary>
    public ConsumePipelineBuilder Use(IConsumeMiddleware middleware)
    {
        EnsureMutable();
        ArgumentNullException.ThrowIfNull(middleware);

        if (middleware is IConsumeTransactionMiddleware && _middleware.Count != 0)
        {
            throw new InvalidOperationException("Register the consumer transaction boundary once, before other middleware.");
        }

        _middleware.Add(middleware);
        return this;
    }

    internal bool HasInbox => _middleware.Exists(static middleware => middleware is IConsumeInbox { Inbox: true });

    // Native feature setup must run before application middleware, including a transaction boundary.
    // Kept internal so application registrations cannot bypass the transaction ordering check.
    internal ConsumePipelineBuilder UseTransportMiddleware(IConsumeMiddleware middleware)
    {
        EnsureMutable();
        ArgumentNullException.ThrowIfNull(middleware);
        _middleware.Insert(0, middleware);
        return this;
    }

    /// <summary>Creates concurrent middleware in a temporary synchronous startup scope. Do not retain scoped services.</summary>
    /// <remarks>Services resolved by the factory must support synchronous disposal.</remarks>
    public ConsumePipelineBuilder Use(Func<IServiceProvider, IConsumeMiddleware> factory)
    {
        EnsureMutable();
        ArgumentNullException.ThrowIfNull(factory);
        using var scope = _scopes.CreateScope();
        return Use(factory(scope.ServiceProvider));
    }

    /// <summary>Attaches this endpoint's process metrics and generated failure logging.</summary>
    public ConsumePipelineBuilder Telemetry(EndpointTelemetry telemetry)
    {
        EnsureMutable();
        ArgumentNullException.ThrowIfNull(telemetry);

        if (!telemetry.IsProcess)
        {
            throw new ArgumentException("A consumer requires an endpoint recorder with OperationName 'process'.", nameof(telemetry));
        }

        _telemetry = telemetry;
        return this;
    }

    /// <summary>Surrounds each delivery, including native settlement, with a process span.</summary>
    /// <remarks>Compose once per endpoint. No receive span is emitted. Native settlement callbacks wrap each actual call with WrapSettlement.</remarks>
    public ConsumePipelineBuilder Tracing(MessageTracing tracing, MessagingEndpoint endpoint)
    {
        EnsureMutable();
        ArgumentNullException.ThrowIfNull(tracing);
        ArgumentNullException.ThrowIfNull(endpoint);
        _tracing = new(tracing, endpoint);
        return this;
    }

    /// <summary>Runs native settlement after durable handling and async scope disposal, inside processing duration.</summary>
    /// <remarks>
    /// Transports read their native feature and disposition from the context. Honor SettledByHandler without settling twice.
    /// Exceptions propagate without re-entering failure handling. Context.Services is unavailable during settlement.
    /// </remarks>
    public ConsumePipelineBuilder Settle(ConsumeDelegate settlement)
    {
        return Settle(settlement, validateRequest: null);
    }

    internal ConsumePipelineBuilder Settle(ConsumeDelegate settlement, Action<Disposition>? validateRequest)
    {
        EnsureMutable();
        ArgumentNullException.ThrowIfNull(settlement);
        _settlement = settlement;
        _validateSettlementRequest = validateRequest;
        return this;
    }

    /// <summary>Builds this consumer once. Size the context pool for its maximum concurrent deliveries.</summary>
    public ConsumePipeline Build(int contextPoolCapacity = 64)
    {
        EnsureMutable();
        ArgumentOutOfRangeException.ThrowIfLessThan(contextPoolCapacity, 1);

        if (_dispatch.Count == 0 && _tombstones.Count == 0)
        {
            throw new InvalidOperationException("Register at least one generated handler before building a consumer.");
        }

        using (var scope = _scopes.CreateScope())
        {
            if (scope.ServiceProvider.GetService<UpcasterRegistry>() is { } upcasters)
            {
                foreach (var descriptor in _handlers)
                {
                    if (descriptor.IsTombstone)
                    {
                        continue;
                    }

                    foreach (var pair in upcasters.Compose(descriptor.MessageType, _messages, _serializer))
                    {
                        if (!_dispatch.TryAdd(pair.Key, descriptor.CreateUpcastDelegate(pair.Value)))
                        {
                            throw new InvalidOperationException($"Consumer has ambiguous handlers for '{pair.Key}'. Select one handler reachable from each incoming version.");
                        }
                    }
                }
            }
        }

        AddAcceptedIdentities();

        foreach (var identity in _dispatch.Keys.Union(_tombstones.Keys).ToArray())
        {
            _dispatch.TryGetValue(identity, out var value);
            _tombstones.TryGetValue(identity, out var tombstone);
            _dispatch[identity] = new NativeValueDispatch(value, tombstone).InvokeAsync;
        }

        _built = true;
        return new ConsumePipeline(_scopes, _dispatch.ToFrozenDictionary(), _middleware, _failure, _classifier,
            contextPoolCapacity, _failures, _telemetry, _settlement, _tracing, SingletonHandlerServices(), _validateSettlementRequest);
    }

    // The final registration of a service wins, matching how the container resolves it. Applications that
    // compose outside AddRelayer have no recorded registrations, so they keep one scope per attempt.
    private IServiceProvider? SingletonHandlerServices()
    {
        // This scope lives as long as the consumer. It only resolves singletons, which the root owns.
        var scope = _scopes.CreateScope();

        if (scope.ServiceProvider.GetService<IRelayerBuilder>() is { } builder)
        {
            var singletons = new HashSet<Type>();

            foreach (var service in builder.Services)
            {
                if (service.IsKeyedService)
                {
                    continue;
                }

                if (service.Lifetime == ServiceLifetime.Singleton)
                {
                    singletons.Add(service.ServiceType);
                }
                else
                {
                    singletons.Remove(service.ServiceType);
                }
            }

            if (_handlers.TrueForAll(handler => singletons.Contains(handler.HandlerType)))
            {
                return scope.ServiceProvider;
            }
        }

        scope.Dispose();
        return null;
    }

    private void AddAcceptedIdentities()
    {
        foreach (var accepted in _accepted)
        {
            var found = false;

            foreach (var descriptor in _handlers)
            {
                var table = descriptor.IsTombstone ? _tombstones : _dispatch;

                if (descriptor.MessageType != (descriptor.IsTombstone ? accepted.TombstoneType : accepted.MessageType))
                {
                    continue;
                }

                _messages.TryGetMessageType(descriptor.MessageType, out var identity);

                if (!table.TryAdd(accepted.Identity, table[identity]))
                {
                    throw new InvalidOperationException($"Consumer already handles '{accepted.Identity}'. Accept each incoming identity for one handler only.");
                }

                found = true;
            }

            if (!found)
            {
                throw new InvalidOperationException($"Consumer accepts '{accepted.Identity}' for '{accepted.MessageType}', but declares no handler for that type. Add its handler to this consumer.");
            }
        }
    }

    private readonly record struct AcceptedIdentity(Type MessageType, Type TombstoneType, MessageType Identity);

    private sealed class NativeValueDispatch(ConsumeDelegate? value, ConsumeDelegate? tombstone)
    {
        internal ValueTask InvokeAsync(MessageContext context)
        {
            var dispatch = ((DefaultMessageContext)context).IsTombstone ? tombstone : value;

            if (dispatch is null)
            {
                throw new MessageDeserializationException(context.MessageType,
                new InvalidOperationException("No handler is registered for this native value kind. Register IHandler<Tombstone<T>> for null values and IHandler<T> for non-null values."));
            }

            return dispatch(context);
        }
    }

    private void EnsureMutable()
    {
        if (_built)
        {
            throw new InvalidOperationException("This consumer has already been built. Compose each consumer once at startup.");
        }
    }
}
