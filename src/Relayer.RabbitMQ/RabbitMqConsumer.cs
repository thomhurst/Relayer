using System.Runtime.CompilerServices;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQ.Client.Exceptions;

namespace Relayer.RabbitMQ;

// RabbitMQ.Client owns dispatch concurrency, recovery, body lifetime and cancellation notifications.
internal sealed partial class RabbitMqConsumer
{
    private readonly Func<RabbitMqDelivery, CancellationToken, ValueTask<Disposition>> _receive;
    private readonly RabbitMqConsumerOptions _options;
    private readonly RabbitMqEndpointCache _endpoints;
    private readonly TimeProvider _clock;
    private readonly bool _automaticRecovery;
    private readonly long[] _active;
    private CancellationTokenSource _abort = new();
    private IChannel? _channel;
    private IConnection? _connection;
    private ShutdownEventArgs? _connectionShutdown;
    private AsyncEventingBasicConsumer? _native;
    private string? _tag;
    private int _running;
    private int _stopping;
    private Exception? _failure;
    private Exception? _processingFailure;
    private int _recovering;
    private static readonly TimeSpan FailureBackoff = TimeSpan.FromSeconds(1);
    private readonly ILogger? _logger;
    private int _callbacks;
    private TaskCompletionSource _cancelled = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private TaskCompletionSource _drained = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private TaskCompletionSource _completion = new(TaskCreationOptions.RunContinuationsAsynchronously);
    internal Task Completion => _completion.Task;
    internal int ActiveCallbackCount => Volatile.Read(ref _callbacks);
    internal IConsumerDefinition Declaration { get; }
    internal bool TrackHealth { get; set; }
    internal RabbitMqConsumer(IConsumerDefinition declaration, RabbitMqConsumerOptions options,
        Func<RabbitMqDelivery, CancellationToken, ValueTask<Disposition>> receive, MessageTracing tracing, TimeProvider? clock = null, ConnectionFactory? factory = null, ILogger? logger = null)
    {
        Declaration = declaration;
        _logger = logger;
        _options = options;
        _receive = receive;
        _endpoints = new(tracing, declaration.Source, factory?.HostName, factory?.Port);
        _clock = clock ?? TimeProvider.System;
        _automaticRecovery = factory?.AutomaticRecoveryEnabled ?? true;
        _active = new long[declaration.Concurrency];
    }

    internal static void Validate(ConsumerDefinition<RabbitMqConsumerOptions> consumer)
    {
        ArgumentNullException.ThrowIfNull(consumer.Options.ChannelOptions);
        ArgumentNullException.ThrowIfNull(consumer.Failure);
        if (consumer.BatchSize != 1)
            throw new NotSupportedException($"RabbitMQ consumer '{consumer.Source}': native callbacks support single-message handlers only.");
        if (consumer.Options.ChannelOptions.ConsumerDispatchConcurrency != consumer.Concurrency)
            throw new InvalidOperationException($"RabbitMQ consumer '{consumer.Source}': ChannelOptions.ConsumerDispatchConcurrency must equal Concurrency({consumer.Concurrency}).");
        if (consumer.Failure.RedeliveryDelays.Length != 0)
            throw new NotSupportedException($"RabbitMQ consumer '{consumer.Source}': nack cannot persist retry metadata. Use RetryDelays for in-process retries and confirmed dead-letter copies.");
    }

    internal async Task StartAsync(IConnection connection, CancellationToken token)
    {
        if (_channel is not null) return;
        if (_abort.IsCancellationRequested) { _abort.Dispose(); _abort = new(); }
        _failure = null;
        _processingFailure = null;
        Volatile.Write(ref _stopping, 0);
        _cancelled = new(TaskCreationOptions.RunContinuationsAsynchronously);
        _drained = new(TaskCreationOptions.RunContinuationsAsynchronously);
        _completion = new(TaskCreationOptions.RunContinuationsAsynchronously);
        var channel = await connection.CreateChannelAsync(_options.ChannelOptions, token).ConfigureAwait(false);
        _connection = connection;
        _connectionShutdown = null;
        connection.ConnectionShutdownAsync += ConnectionShutdownAsync;
        try
        {
            await channel.BasicQosAsync(0, _options.PrefetchCount, false, token).ConfigureAwait(false);
            var native = new NativeConsumer(this, channel);
            native.ReceivedAsync += ProcessAsync;
            native.UnregisteredAsync += CancelledAsync;
            channel.CallbackExceptionAsync += CallbackFailedAsync;
            _channel = channel;
            _native = native;
            _tag = await channel.BasicConsumeAsync(Declaration.Source, false, "relayer-" + Guid.NewGuid().ToString("N"), false, _options.Exclusive,
                _options.Arguments, native, token).ConfigureAwait(false);
            Volatile.Write(ref _running, 1);
        }
        catch
        {
            connection.ConnectionShutdownAsync -= ConnectionShutdownAsync;
            _connection = null;
            _channel = null;
            _native = null;
            await channel.DisposeAsync().ConfigureAwait(false);
            throw;
        }
    }

    private Task ConnectionShutdownAsync(object sender, ShutdownEventArgs reason)
    {
        Volatile.Write(ref _connectionShutdown, reason);
        // Without native recovery there may never be another callback to report this failure.
        // StopAsync marks intentional shutdown before closing any native resources.
        if (!_automaticRecovery && Volatile.Read(ref _stopping) == 0)
        {
            var error = new AlreadyClosedException(reason);
            Volatile.Write(ref _failure, error);
            _completion.TrySetException(error);
        }
        return Task.CompletedTask;
    }

    private Task CallbackFailedAsync(object sender, CallbackExceptionEventArgs args)
    {
        // The native recoverable connection owns redelivery after a connection outage. Keep
        // its shutdown reason even after recovery so a delayed callback cannot fault the host.
        // Errors that escape processing recovery (including a closed consumer channel) remain terminal.
        if (_automaticRecovery && args.Exception is AlreadyClosedException error &&
            error.ShutdownReason is { Initiator: not ShutdownInitiator.Application, ReplyCode: not 403 } reason &&
            (ReferenceEquals(reason, _connection?.CloseReason) || ReferenceEquals(reason, Volatile.Read(ref _connectionShutdown))))
            return Task.CompletedTask;
        Volatile.Write(ref _failure, args.Exception);
        _completion.TrySetException(args.Exception);
        return Task.CompletedTask;
    }

    private Task CancelledAsync(object sender, ConsumerEventArgs args)
    {
        if (Volatile.Read(ref _stopping) != 0)
        {
            _cancelled.TrySetResult();
            if (Volatile.Read(ref _callbacks) == 0) _drained.TrySetResult();
        }
        return Task.CompletedTask;
    }

    // UnregisteredAsync also runs on native channel shutdown during recovery. Only an actual
    // broker basic.cancel is terminal; a delayed shutdown notification must not fault a recovered consumer.
    private sealed class NativeConsumer(RabbitMqConsumer owner, IChannel channel) : AsyncEventingBasicConsumer(channel)
    {
        public override Task HandleBasicCancelAsync(string consumerTag, CancellationToken cancellationToken = default)
        {
            var error = new InvalidOperationException($"RabbitMQ broker cancelled consumer '{owner.Declaration.Source}'. Recreate the host after restoring its queue.");
            Volatile.Write(ref owner._failure, error);
            owner._completion.TrySetException(error);
            return base.HandleBasicCancelAsync(consumerTag, cancellationToken);
        }
    }

    private Task ProcessAsync(object sender, BasicDeliverEventArgs message)
    {
        Interlocked.Increment(ref _callbacks);
        return ProcessCoreAsync(message).AsTask();
    }

    [RuntimeAsyncMethodGeneration(false)]
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    private async ValueTask ProcessCoreAsync(BasicDeliverEventArgs message)
    {
        var slot = -1;
        if (TrackHealth)
            for (var i = 0; i < _active.Length; i++)
                if (Interlocked.CompareExchange(ref _active[i], _clock.GetUtcNow().UtcTicks, 0) == 0) { slot = i; break; }
        try
        {
            var delivery = new RabbitMqDelivery(_channel!, message)
            { Endpoint = _endpoints.Get(message.Exchange, message.RoutingKey) };
            var previousFailure = Volatile.Read(ref _processingFailure);
            Disposition disposition;
            try
            {
                disposition = await _receive(delivery, _abort.Token).ConfigureAwait(false);
            }
            catch (Exception error) when (_channel!.IsOpen && !_abort.IsCancellationRequested)
            {
                var alreadySettled = error is SettledReceiveException;
                if (error is SettledReceiveException settled) error = settled.InnerException!;
                if (!alreadySettled) Interlocked.Increment(ref _recovering);
                try
                {
                    Volatile.Write(ref _processingFailure, error);
                    if (_logger is { } logger) ProcessingFailed(logger, error, Declaration.Source);
                    // A successful native settlement is final even if later scope/telemetry work throws.
                    if (alreadySettled) return;
                    // basic.nack requeues immediately; keep the native delivery unacknowledged during
                    // backoff so an unavailable failure destination cannot create a hot requeue loop.
                    await Task.Delay(FailureBackoff, _clock, _abort.Token).ConfigureAwait(false);
                    await delivery.Endpoint.Settlement.SettleAsync(delivery, Disposition.Redeliver(TimeSpan.Zero), _abort.Token).ConfigureAwait(false);
                }
                finally { if (!alreadySettled) Interlocked.Decrement(ref _recovering); }
                return;
            }
            // The normal pipeline settles while its process activity is still alive. Fixture/custom
            // callbacks may instead return a disposition; honor it once, after durable completion.
            if (disposition.Kind != DispositionKind.SettledByHandler)
            {
                await delivery.Endpoint.Settlement.SettleAsync(delivery, disposition, _abort.Token).ConfigureAwait(false);
            }
            // Recovery callbacks can still be finishing their nack when a redelivery succeeds.
            // _recovering independently keeps health unhealthy until every backoff ends.
            if (previousFailure is not null)
                Interlocked.CompareExchange(ref _processingFailure, null, previousFailure);
        }
        finally
        {
            if (slot >= 0) Volatile.Write(ref _active[slot], 0);
            if (Interlocked.Decrement(ref _callbacks) == 0 && _cancelled.Task.IsCompleted) _drained.TrySetResult();
        }
    }

    internal (bool IsIdle, DateTimeOffset LastProgressAt)? GetHealthState()
    {
        if (!TrackHealth || Volatile.Read(ref _running) == 0 || _channel?.IsOpen != true || _native?.IsRunning != true) return null;
        if (Volatile.Read(ref _failure) is { } failure) throw new InvalidOperationException("RabbitMQ receive callback failed.", failure);
        if (Volatile.Read(ref _recovering) != 0 || Volatile.Read(ref _processingFailure) is not null)
        {
            var processingFailure = Volatile.Read(ref _processingFailure);
            throw new InvalidOperationException("RabbitMQ processing failed; unsettled deliveries are requeued after backoff.", processingFailure);
        }
        var oldest = long.MaxValue;
        foreach (ref var slot in _active.AsSpan())
        {
            var active = Volatile.Read(ref slot);
            if (active != 0) oldest = Math.Min(oldest, active);
        }
        return oldest == long.MaxValue ? (true, _clock.GetUtcNow()) : (false, new DateTimeOffset(oldest, TimeSpan.Zero));
    }

    internal async Task StopAsync(CancellationToken token)
    {
        var channel = _channel;
        if (channel is null) return;
        Volatile.Write(ref _stopping, 1);
        using var registration = token.UnsafeRegister(static state => ((CancellationTokenSource)state!).Cancel(), _abort);
        try
        {
            if (channel.IsOpen && _tag is not null && _native?.IsRunning == true)
            {
                await channel.BasicCancelAsync(_tag, false, token).ConfigureAwait(false);
                // Cancel-ok is dispatched after queued deliveries. With parallel dispatch, also
                // wait for our still-active callbacks before closing their settlement channel.
                await _cancelled.Task.WaitAsync(token).ConfigureAwait(false);
                await _drained.Task.WaitAsync(token).ConfigureAwait(false);
            }
            if (channel.IsOpen) await channel.CloseAsync(token).ConfigureAwait(false);
        }
        finally
        {
            Volatile.Write(ref _running, 0);
            if (_connection is { } connection) connection.ConnectionShutdownAsync -= ConnectionShutdownAsync;
            _connection = null;
            await channel.DisposeAsync().ConfigureAwait(false);
            _channel = null;
            _native = null;
            _tag = null;
            _completion.TrySetResult();
        }
    }

    [LoggerMessage(Level = LogLevel.Error, Message = "RabbitMQ processing failed for {Source}.")]
    private static partial void ProcessingFailed(ILogger logger, Exception exception, string source);

    internal void DisposeCancellation() => _abort.Dispose();
}
