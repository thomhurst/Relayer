using System.Runtime.CompilerServices;
using Microsoft.Extensions.DependencyInjection;
using Relayer.Transports;

namespace Relayer.Pipeline;

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
    private readonly double _jitter;
    private readonly string? _deadLetterEndpoint;
    private readonly TimeSpan _copyTimeout;
    private readonly Func<MessageContext, string, OutgoingMessage>? _copy;
    private readonly IFailureScheduler? _scheduler;
    private readonly TimeProvider _time;
    private readonly RedeliveryPath[] _paths;
    private readonly bool _nativeDeadLetter;
    private readonly bool _nativeRedeliveryOnly;
    private readonly TransportCapabilities _capabilities;
    private readonly long? _deadLetterMaxBytes;

    internal (string TransportName, string Source) ReceiveEndpoint => (_transport.Name, _source);

    // An adapter can require stronger delivery guarantees for copies than for ordinary sends.
    internal ITransport? CopyTransport
    {
        get; init;
    }

    internal string? RedeliveryUnavailableHint
    {
        get; init;
    }

    /// <summary>Validates native paths and available fallback executors for this consumer's complete policy.</summary>
    public FailurePipeline(IServiceProvider services, string transportName, string source, string consumer,
        FailureOptions? options = null, FailureMessageCopyDelegate? copy = null,
        IFailureScheduler? scheduler = null, TimeProvider? timeProvider = null)
        : this(services, transportName, source, consumer, false, options,
            copy is null ? null : (context, _) => copy(context), scheduler, timeProvider)
    {
    }

    internal FailurePipeline(IServiceProvider services, string transportName, string source, string consumer,
        bool nativeRedeliveryOnly, FailureOptions? options, Func<MessageContext, string, OutgoingMessage>? copy,
        IFailureScheduler? scheduler, TimeProvider? timeProvider)
    {
        _nativeRedeliveryOnly = nativeRedeliveryOnly;
        ArgumentNullException.ThrowIfNull(services);
        ArgumentException.ThrowIfNullOrWhiteSpace(transportName);
        ArgumentException.ThrowIfNullOrWhiteSpace(source);
        ArgumentException.ThrowIfNullOrWhiteSpace(consumer);
        _source = source;
        _consumer = consumer;
        options ??= new();
        _jitter = options.Jitter;
        _deadLetterEndpoint = options.DeadLetterEndpoint;
        _copyTimeout = options.CopyTimeout ?? TimeSpan.FromSeconds(30);
        _copy = copy;
        _scheduler = scheduler;
        _time = timeProvider ?? TimeProvider.System;
        _retries = options.RetryDelays?.ToArray() ??
            [TimeSpan.FromMilliseconds(100), TimeSpan.FromMilliseconds(500), TimeSpan.FromSeconds(1)];
        _redeliveries = options.RedeliveryDelays.ToArray();

        if (!double.IsFinite(_jitter) || _jitter < 0 || _jitter > 1)
        {
            throw InvalidOption("Jitter must be between zero and one.");
        }

        if (_copyTimeout <= TimeSpan.Zero || _copyTimeout.TotalMilliseconds > uint.MaxValue - 1)
        {
            throw InvalidOption("CopyTimeout must be positive and fit a CancellationTokenSource timer.");
        }

        foreach (var delay in _retries)
        {
            if (delay < TimeSpan.Zero || delay.TotalMilliseconds > uint.MaxValue - 1)
            {
                throw InvalidOption("RetryDelays must be non-negative and fit a timer.");
            }
        }

        foreach (var delay in _redeliveries)
        {
            if (delay < TimeSpan.Zero || delay > DateTimeOffset.MaxValue - _time.GetUtcNow())
            {
                throw InvalidOption("RedeliveryDelays must be non-negative and fit a delivery timestamp.");
            }
        }

        ITransport? selected = null;

        foreach (var transport in services.GetServices<ITransport>())
        {
            if (transport.Name != transportName)
            {
                continue;
            }

            if (selected is not null)
            {
                throw Invalid("ITransport", "Register exactly one adapter for this transport name.");
            }

            selected = transport;
        }

        _transport = selected ?? throw Invalid("ITransport", "Register a named ITransport adapter.");
        var failures = new List<string>();
        var cache = services.GetRequiredService<EndpointCapabilityCache>();
        var validator = new EndpointCapabilityValidator([_transport], failures, cache);
        var capabilities = validator.GetCapabilities(_transport, source);

        if (failures.Count != 0)
        {
            throw new InvalidOperationException(string.Join(Environment.NewLine, failures));
        }

        if (capabilities is null)
        {
            throw Invalid("TransportCapabilities", "Return a valid endpoint capability declaration.");
        }

        _capabilities = capabilities;
        _nativeDeadLetter = capabilities.DeadLetter;

        if (!_nativeDeadLetter)
        {
            if (string.IsNullOrWhiteSpace(_deadLetterEndpoint) || copy is null)
            {
                throw Invalid("DeadLetter", "Enable native DeadLetter with error metadata, or configure DeadLetterEndpoint and a message copy executor.");
            }

            _deadLetterMaxBytes = validator.GetCapabilities(_transport, _deadLetterEndpoint)?.MaxMessageBytes;
        }

        _paths = new RedeliveryPath[_redeliveries.Length];

        for (var i = 0; i < _paths.Length; i++)
        {
            _paths[i] = ResolveRedelivery(_redeliveries[i]);
        }

        if (failures.Count != 0)
        {
            throw new InvalidOperationException(string.Join(Environment.NewLine, failures));
        }
        // Compose direct-send routes before receives can resolve their scoped sender.
        services.GetRequiredService<SenderPool>().Enable();

        ArgumentOutOfRangeException InvalidOption(string reason)
        {
            return new(nameof(options), $"Consumer '{consumer}' from '{source}' on transport '{transportName}': {reason}");
        }

        InvalidOperationException Invalid(string capability, string remedy)
        {
            return new(
                        $"Consumer '{consumer}' from '{source}' on transport '{transportName}': {capability} is unavailable. {remedy}");
        }
    }

    internal int GetLongestNativeRedeliveryRun()
    {
        var longest = 0;
        var consecutive = 0;

        foreach (var path in _paths)
        {
            if (path == RedeliveryPath.Native)
            {
                consecutive++;
                longest = Math.Max(longest, consecutive);
            }
            else
            {
                // A copy starts a new physical message with its own native receive count.
                consecutive = 0;
            }
        }

        return longest;
    }

    internal void ValidateSettlement(Disposition disposition)
    {
        if (disposition.Kind == DispositionKind.Redeliver)
        {
            ResolveRedelivery(disposition.Delay);
        }
    }

    private RedeliveryPath ResolveRedelivery(TimeSpan delay)
    {
        if (delay > DateTimeOffset.MaxValue - _time.GetUtcNow())
        {
            throw new ArgumentOutOfRangeException(nameof(delay), "The retry delay must fit a delivery timestamp.");
        }

        if (_capabilities.MaxRedeliveryDelay is { } native && delay <= native)
        {
            return RedeliveryPath.Native;
        }

        if (_nativeRedeliveryOnly)
        {
            throw new InvalidOperationException($"Consumer '{_consumer}' from '{_source}' on transport '{_transport.Name}': redelivery delay {delay} exceeds native MaxRedeliveryDelay. In-memory retries must retain their group and admission credit; increase MaxRedeliveryDelay or shorten the delay. Copy and outbox redelivery fallbacks are unavailable.");
        }

        if (_copy is not null && _capabilities.MaxDeliveryDelay is { } scheduled && delay <= scheduled)
        {
            return RedeliveryPath.Copy;
        }

        if (_copy is not null && _scheduler is not null)
        {
            return RedeliveryPath.Outbox;
        }

        var hint = RedeliveryUnavailableHint ??
            "Shorten the delay, enable native redelivery/scheduling, or configure a message copy executor and durable IFailureScheduler.";
        throw new InvalidOperationException($"Consumer '{_consumer}' from '{_source}' on transport '{_transport.Name}': redelivery delay {delay} has no executor. {hint}");
    }

    [RuntimeAsyncMethodGeneration(false)]
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    internal async ValueTask HandleSettlementAsync(DefaultMessageContext context, Disposition request)
    {
        context.CancellationToken.ThrowIfCancellationRequested();

        if (request.Kind == DispositionKind.Redeliver)
        {
            var path = ResolveRedelivery(request.Delay);
            // A manual retry is not an exception. Preserve prior error history without inventing a failure.
            var headers = FailureMetadata.SettlementHeaders(context, checked(context.Redeliveries + 1), maxBytes: path == RedeliveryPath.Native ? MetadataBudget(context) : null);

            if (path == RedeliveryPath.Native)
            {
                context.FailureDisposition = Disposition.Redeliver(request.Delay, headers);
            }
            else
            {
                await CopyAsync(context, _source, _time.GetUtcNow() + request.Delay, headers,
                    path == RedeliveryPath.Outbox).ConfigureAwait(false);
                context.FailureDisposition = Disposition.Complete;
            }
        }
        else
        {
            var headers = FailureMetadata.SettlementHeaders(context, context.Redeliveries, request.DeadLetter.Reason, _nativeDeadLetter ? MetadataBudget(context, System.Text.Encoding.UTF8.GetByteCount(request.DeadLetter.Reason)) : null);

            if (_nativeDeadLetter)
            {
                context.FailureDisposition = Disposition.DeadLettered(new(request.DeadLetter.Reason, headers: headers));
            }
            else
            {
                await CopyAsync(context, _deadLetterEndpoint!, null, headers, false).ConfigureAwait(false);
                context.FailureDisposition = Disposition.Complete;
            }
        }
    }

    internal void Record(DefaultMessageContext context, Exception error)
    {
        FailureMetadata.Record(context, error, _consumer, _time);
    }

    internal bool CanRetry(int retry)
    {
        return retry < _retries.Length;
    }

    internal ValueTask WaitRetryAsync(int retry, CancellationToken cancellationToken)
    {
        var configuredDelay = _retries[retry];

        if (configuredDelay == TimeSpan.Zero)
        {
            return ValueTask.CompletedTask;
        }

        var delay = Jitter(configuredDelay);

        if (_time == TimeProvider.System)
        {
            var minimumDelay = TimeSpan.FromMilliseconds(1);
            return new(Task.Delay(delay < minimumDelay ? minimumDelay : delay, cancellationToken));
        }

        if (delay.Ticks % TimeSpan.TicksPerMillisecond != 0)
        {
            return WaitForCustomTimerAsync(delay, cancellationToken);
        }

        return delay == TimeSpan.Zero ? ValueTask.CompletedTask : new(Task.Delay(delay, _time, cancellationToken));
    }

    [RuntimeAsyncMethodGeneration(false)]
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    private async ValueTask WaitForCustomTimerAsync(TimeSpan delay, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        // Task.Delay truncates fractional milliseconds before calling a custom TimeProvider.
        var elapsed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        ITimer timer;

        using (ExecutionContext.IsFlowSuppressed() ? default : ExecutionContext.SuppressFlow())
        {
            timer = _time.CreateTimer(static state => ((TaskCompletionSource)state!).TrySetResult(),
                elapsed, delay, Timeout.InfiniteTimeSpan);
        }

        using (timer)
        {
            await elapsed.Task.WaitAsync(cancellationToken).ConfigureAwait(false);
        }
    }

    private TimeSpan Jitter(TimeSpan delay)
    {
        return _jitter == 0 || delay == TimeSpan.Zero ? delay
                : TimeSpan.FromTicks((long)(delay.Ticks * (1 - Random.Shared.NextDouble() * _jitter)));
    }

    [RuntimeAsyncMethodGeneration(false)]
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    internal async ValueTask HandleAsync(DefaultMessageContext context, Exception error, FailureAction action)
    {
        context.CancellationToken.ThrowIfCancellationRequested();

        if (!Enum.IsDefined(action))
        {
            throw new InvalidOperationException("IFailureClassifier returned an undefined FailureAction.");
        }

        if (action == FailureAction.Discard)
        {
            context.FailureDisposition = Disposition.Complete;
            return;
        }

        var redelivery = context.Redeliveries;

        if (action == FailureAction.Retry && redelivery < _paths.Length)
        {
            var headers = FailureMetadata.Headers(context, redelivery + 1, _paths[redelivery] == RedeliveryPath.Native ? MetadataBudget(context) : null);
            var delay = Jitter(_redeliveries[redelivery]);

            if (_paths[redelivery] == RedeliveryPath.Native)
            {
                context.FailureDisposition = Disposition.Redeliver(delay, headers);
            }
            else
            {
                await CopyAsync(context, _source, _time.GetUtcNow() + delay, headers,
                    _paths[redelivery] == RedeliveryPath.Outbox).ConfigureAwait(false);
                context.FailureDisposition = Disposition.Complete;
            }

            return;
        }

        var description = FailureMetadata.Description(error);
        var deadHeaders = FailureMetadata.Headers(context, redelivery, _nativeDeadLetter ? MetadataBudget(context, System.Text.Encoding.UTF8.GetByteCount(description) + System.Text.Encoding.UTF8.GetByteCount("Processing failed")) : null);

        if (_nativeDeadLetter)
        {
            context.FailureDisposition = Disposition.DeadLettered(new("Processing failed", description, deadHeaders));
        }
        else
        {
            await CopyAsync(context, _deadLetterEndpoint!, null, deadHeaders, false).ConfigureAwait(false);
            context.FailureDisposition = Disposition.Complete;
        }
    }

    [RuntimeAsyncMethodGeneration(false)]
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    private async ValueTask CopyAsync(DefaultMessageContext context, string destination, DateTimeOffset? deliverAt,
        KeyValuePair<string, WireString>[] metadata, bool schedule)
    {
        var original = _copy!(context, destination);
        var headers = new List<KeyValuePair<string, WireString>>(original.Headers.Length + metadata.Length);

        foreach (var header in original.Headers.Span)
        {
            // A deliberately omitted history must not resurrect an oversized incoming value.
            if (header.Key == FailureHeaders.History)
            {
                continue;
            }

            var replaced = false;

            foreach (var replacement in metadata)
            {
                if (header.Key != replacement.Key)
                {
                    continue;
                }

                replaced = true;
                break;
            }

            if (!replaced)
            {
                headers.Add(header);
            }
        }

        headers.AddRange(metadata);
        FailureMetadata.FitCopyHistory(headers, original,
            destination == _source ? _capabilities.MaxMessageBytes : _deadLetterMaxBytes, context.FailureHistory);
        var copy = original.Forward(destination, headers.ToArray(), deliverAt);

        if (schedule)
        {
            copy.ValidateForOutbox();
        }
        // Fallback work is on the error path. Its timeout covers the whole durable operation.
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(context.CancellationToken);
        timeout.CancelAfter(_copyTimeout);

        if (schedule)
        {
            await _scheduler!.ScheduleAsync(_transport.Name, copy, timeout.Token).ConfigureAwait(false);
        }
        else
        {
            var sender = CopyTransport ?? context.RedirectSend(_transport);
            await (sender is ITransportFailureCopyObserver observer
                    ? observer.SendCopyAsync(copy, context.EnvelopeNames, timeout.Token)
                    : sender.SendAsync(copy, timeout.Token)).ConfigureAwait(false);
        }

        timeout.Token.ThrowIfCancellationRequested();
    }

    private long? MetadataBudget(DefaultMessageContext context, int nativeFieldBytes = 0)
    {
        return _capabilities.MaxMessageBytes is { } maximum ? Math.Max(0, maximum - context.Body.Length - nativeFieldBytes) : null;
    }

    private enum RedeliveryPath
    {
        Native,
        Copy,
        Outbox
    }
}
