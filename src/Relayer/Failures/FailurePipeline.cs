using System.Runtime.CompilerServices;
using Microsoft.Extensions.DependencyInjection;
using Relayer.Transports;

namespace Relayer;

/// <summary>Composes executable failure handling and validates D24 capabilities before a consumer starts.</summary>
/// <remarks>
/// Construct once during consumer composition, before starting native receives. Uses the same endpoint capability
/// cache as the hosted startup validator. Executors and the copy factory must support concurrent calls.
/// </remarks>
public sealed class FailurePipeline
{
    private readonly ITransport _transport;
    private readonly string _consumer;
    private readonly string _source;
    private readonly TimeSpan[] _retries;
    private readonly TimeSpan[] _redeliveries;
    private readonly FailureOptions _options;
    private readonly FailureMessageCopyDelegate? _copy;
    private readonly IFailureScheduler? _scheduler;
    private readonly TimeProvider _time;
    private readonly RedeliveryPath[] _paths;
    private readonly bool _nativeDeadLetter;
    private readonly TransportCapabilities _capabilities;

    /// <summary>Validates native paths and available fallback executors for this consumer's complete policy.</summary>
    public FailurePipeline(IServiceProvider services, string transportName, string source, string consumer,
        FailureOptions? options = null, FailureMessageCopyDelegate? copy = null,
        IFailureScheduler? scheduler = null, TimeProvider? timeProvider = null)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentException.ThrowIfNullOrWhiteSpace(transportName);
        ArgumentException.ThrowIfNullOrWhiteSpace(source);
        ArgumentException.ThrowIfNullOrWhiteSpace(consumer);
        _source = source;
        _consumer = consumer;
        _options = options ?? new();
        _copy = copy;
        _scheduler = scheduler;
        _time = timeProvider ?? TimeProvider.System;
        _retries = _options.RetryDelays.ToArray();
        _redeliveries = _options.RedeliveryDelays.ToArray();
        if (!double.IsFinite(_options.Jitter) || _options.Jitter < 0 || _options.Jitter > 1)
            throw new ArgumentOutOfRangeException(nameof(options), "Jitter must be between zero and one.");
        if (_options.CopyTimeout <= TimeSpan.Zero || _options.CopyTimeout.TotalMilliseconds > uint.MaxValue - 1)
            throw new ArgumentOutOfRangeException(nameof(options), "CopyTimeout must be positive and fit a CancellationTokenSource timer.");
        foreach (var delay in _retries)
            if (delay < TimeSpan.Zero || delay.TotalMilliseconds > uint.MaxValue - 1)
                throw new ArgumentOutOfRangeException(nameof(options), "RetryDelays must be non-negative and fit a timer.");
        foreach (var delay in _redeliveries)
            if (delay < TimeSpan.Zero || delay > DateTimeOffset.MaxValue - _time.GetUtcNow())
                throw new ArgumentOutOfRangeException(nameof(options), "RedeliveryDelays must be non-negative and fit a delivery timestamp.");

        ITransport? selected = null;
        foreach (var transport in services.GetServices<ITransport>())
        {
            if (transport.Name != transportName) continue;
            if (selected is not null) throw Invalid("ITransport", "Register exactly one adapter for this transport name.");
            selected = transport;
        }
        _transport = selected ?? throw Invalid("ITransport", "Register a named ITransport adapter.");
        var failures = new List<string>();
        var cache = services.GetRequiredService<EndpointCapabilityCache>();
        var validator = new EndpointCapabilityValidator([_transport], failures, cache);
        var capabilities = validator.GetCapabilities(_transport, source);
        if (failures.Count != 0) throw new InvalidOperationException(string.Join(Environment.NewLine, failures));
        if (capabilities is null) throw Invalid("TransportCapabilities", "Return a valid endpoint capability declaration.");
        _capabilities = capabilities;
        _nativeDeadLetter = capabilities.DeadLetter;
        if (!_nativeDeadLetter)
        {
            if (string.IsNullOrWhiteSpace(_options.DeadLetterEndpoint) || copy is null)
                throw Invalid("DeadLetter", "Enable native DeadLetter with error metadata, or configure DeadLetterEndpoint and a message copy executor.");
            validator.GetCapabilities(_transport, _options.DeadLetterEndpoint);
        }
        _paths = new RedeliveryPath[_redeliveries.Length];
        for (var i = 0; i < _paths.Length; i++) _paths[i] = ResolveRedelivery(_redeliveries[i]);
        if (failures.Count != 0) throw new InvalidOperationException(string.Join(Environment.NewLine, failures));
        // Compose direct-send routes before receives can resolve their scoped sender.
        services.GetRequiredService<SenderPool>().Enable();

        InvalidOperationException Invalid(string capability, string remedy) => new(
            $"Consumer '{consumer}' from '{source}' on transport '{transportName}': {capability} is unavailable. {remedy}");
    }

    internal void ValidateSettlement(Disposition disposition)
    {
        if (disposition.Kind == DispositionKind.Redeliver) ResolveRedelivery(disposition.Delay);
    }

    private RedeliveryPath ResolveRedelivery(TimeSpan delay)
    {
        if (delay > DateTimeOffset.MaxValue - _time.GetUtcNow())
            throw new ArgumentOutOfRangeException(nameof(delay), "The retry delay must fit a delivery timestamp.");
        if (_capabilities.MaxRedeliveryDelay is { } native && delay <= native) return RedeliveryPath.Native;
        if (_copy is not null && _capabilities.MaxDeliveryDelay is { } scheduled && delay <= scheduled) return RedeliveryPath.Copy;
        if (_copy is not null && _scheduler is not null) return RedeliveryPath.Outbox;
        throw new InvalidOperationException($"Consumer '{_consumer}' from '{_source}' on transport '{_transport.Name}': redelivery delay {delay} has no executor. Shorten the delay, enable native redelivery/scheduling, or configure a message copy executor and durable IFailureScheduler.");
    }

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    internal async ValueTask HandleSettlementAsync(DefaultMessageContext context, Disposition request)
    {
        context.CancellationToken.ThrowIfCancellationRequested();
        if (request.Kind == DispositionKind.Redeliver)
        {
            var path = ResolveRedelivery(request.Delay);
            // A manual retry is not an exception. Preserve prior error history without inventing a failure.
            var headers = FailureMetadata.SettlementHeaders(context, checked(context.Redeliveries + 1));
            if (path == RedeliveryPath.Native)
                context.FailureDisposition = Disposition.Redeliver(request.Delay, headers);
            else
            {
                await CopyAsync(context, _source, _time.GetUtcNow() + request.Delay, headers,
                    path == RedeliveryPath.Outbox).ConfigureAwait(false);
                context.FailureDisposition = Disposition.Complete;
            }
        }
        else
        {
            var headers = FailureMetadata.SettlementHeaders(context, context.Redeliveries, request.DeadLetter.Reason);
            if (_nativeDeadLetter)
                context.FailureDisposition = Disposition.DeadLettered(new(request.DeadLetter.Reason, headers: headers));
            else
            {
                await CopyAsync(context, _options.DeadLetterEndpoint!, null, headers, false).ConfigureAwait(false);
                context.FailureDisposition = Disposition.Complete;
            }
        }
    }

    internal void Record(DefaultMessageContext context, Exception error) => FailureMetadata.Record(context, error, _consumer, _time);
    internal bool CanRetry(int retry) => retry < _retries.Length;
    internal ValueTask WaitRetryAsync(int retry, CancellationToken cancellationToken)
    {
        var delay = Jitter(_retries[retry]);
        return delay == TimeSpan.Zero ? ValueTask.CompletedTask : new(Task.Delay(delay, _time, cancellationToken));
    }
    private TimeSpan Jitter(TimeSpan delay) => _options.Jitter == 0 || delay == TimeSpan.Zero ? delay
        : TimeSpan.FromTicks((long)(delay.Ticks * (1 - Random.Shared.NextDouble() * _options.Jitter)));

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    internal async ValueTask HandleAsync(DefaultMessageContext context, Exception error, FailureAction action)
    {
        context.CancellationToken.ThrowIfCancellationRequested();
        if (!Enum.IsDefined(action)) throw new InvalidOperationException("IFailureClassifier returned an undefined FailureAction.");
        if (action == FailureAction.Discard)
        {
            context.FailureDisposition = Disposition.Complete;
            return;
        }
        var redelivery = context.Redeliveries;
        if (action == FailureAction.Retry && redelivery < _paths.Length)
        {
            var headers = FailureMetadata.Headers(context, redelivery + 1);
            var delay = Jitter(_redeliveries[redelivery]);
            if (_paths[redelivery] == RedeliveryPath.Native)
                context.FailureDisposition = Disposition.Redeliver(delay, headers);
            else
            {
                await CopyAsync(context, _source, _time.GetUtcNow() + delay, headers,
                    _paths[redelivery] == RedeliveryPath.Outbox).ConfigureAwait(false);
                context.FailureDisposition = Disposition.Complete;
            }
            return;
        }
        var deadHeaders = FailureMetadata.Headers(context, redelivery);
        if (_nativeDeadLetter)
            context.FailureDisposition = Disposition.DeadLettered(new("Processing failed", error.Message, deadHeaders));
        else
        {
            await CopyAsync(context, _options.DeadLetterEndpoint!, null, deadHeaders, false).ConfigureAwait(false);
            context.FailureDisposition = Disposition.Complete;
        }
    }

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    private async ValueTask CopyAsync(DefaultMessageContext context, string destination, DateTimeOffset? deliverAt,
        KeyValuePair<string, WireString>[] metadata, bool schedule)
    {
        var original = _copy!(context);
        var headers = new List<KeyValuePair<string, WireString>>(original.Headers.Length + metadata.Length);
        foreach (var header in original.Headers.Span)
        {
            var replaced = false;
            foreach (var replacement in metadata)
            {
                if (header.Key != replacement.Key) continue;
                replaced = true;
                break;
            }
            if (!replaced) headers.Add(header);
        }
        headers.AddRange(metadata);
        var copy = original.Forward(destination, headers.ToArray(), deliverAt);
        if (schedule) copy.ValidateForOutbox();
        // Fallback work is on the error path. Its timeout covers the whole durable operation.
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(context.CancellationToken);
        timeout.CancelAfter(_options.CopyTimeout);
        if (schedule)
            await _scheduler!.ScheduleAsync(_transport.Name, copy, timeout.Token).ConfigureAwait(false);
        else
            await _transport.SendAsync(copy, timeout.Token).ConfigureAwait(false);
        timeout.Token.ThrowIfCancellationRequested();
    }

    private enum RedeliveryPath { Native, Copy, Outbox }
}
