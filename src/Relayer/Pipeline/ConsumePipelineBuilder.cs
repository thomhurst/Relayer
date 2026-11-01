using System.Collections.Frozen;
using Microsoft.Extensions.DependencyInjection;
using Relayer.Serialization;

namespace Relayer;

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
    private readonly List<IConsumeMiddleware> _middleware = [];
    private bool _built;
    private EndpointTelemetry? _telemetry;
    private ConsumeDelegate? _settlement;

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
        if (!_dispatch.TryAdd(identity, dispatch))
        {
            throw new InvalidOperationException($"Consumer already handles '{identity}'. Use one handler to coordinate work for this message type.");
        }
        return this;
    }

    /// <summary>Adds middleware. The first registered middleware is outermost.</summary>
    public ConsumePipelineBuilder Use(IConsumeMiddleware middleware)
    {
        EnsureMutable();
        ArgumentNullException.ThrowIfNull(middleware);
        _middleware.Add(middleware);
        return this;
    }

    /// <summary>Attaches this endpoint's process metrics and generated failure logging.</summary>
    public ConsumePipelineBuilder Telemetry(EndpointTelemetry telemetry)
    {
        EnsureMutable();
        ArgumentNullException.ThrowIfNull(telemetry);
        if (!telemetry.IsProcess)
            throw new ArgumentException("A consumer requires an endpoint recorder with OperationName 'process'.", nameof(telemetry));
        _telemetry = telemetry;
        return this;
    }

    /// <summary>Runs native settlement after durable handling and async scope disposal, inside processing duration.</summary>
    /// <remarks>
    /// Transports read their native feature and disposition from the context. Honor SettledByHandler without settling twice.
    /// Exceptions propagate without re-entering failure handling. Context.Services is unavailable during settlement.
    /// </remarks>
    public ConsumePipelineBuilder Settle(ConsumeDelegate settlement)
    {
        EnsureMutable();
        ArgumentNullException.ThrowIfNull(settlement);
        _settlement = settlement;
        return this;
    }

    /// <summary>Builds this consumer once. Size the context pool for its maximum concurrent deliveries.</summary>
    public ConsumePipeline Build(int contextPoolCapacity = 64)
    {
        EnsureMutable();
        ArgumentOutOfRangeException.ThrowIfLessThan(contextPoolCapacity, 1);
        if (_dispatch.Count == 0)
        {
            throw new InvalidOperationException("Register at least one generated handler before building a consumer.");
        }
        _built = true;
        return new ConsumePipeline(_scopes, _dispatch.ToFrozenDictionary(), _middleware, _failure, _classifier,
            contextPoolCapacity, _failures, _telemetry, _settlement);
    }

    private void EnsureMutable()
    {
        if (_built)
        {
            throw new InvalidOperationException("This consumer has already been built. Compose each consumer once at startup.");
        }
    }
}
