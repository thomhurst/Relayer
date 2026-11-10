using System.Runtime.CompilerServices;
using System.Threading.Channels;
using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.Logging;

namespace Relayer.AzureServiceBus;

// Owns native processor lifetime. The SDK owns receives, concurrency, sessions, reconnect and lock renewal.
internal sealed partial class ServiceBusConsumer : IAsyncDisposable
{
    private readonly ServiceBusProcessor? _processor;
    private readonly ServiceBusSessionProcessor? _sessions;
    private readonly Func<ProcessMessageEventArgs, CancellationToken, ValueTask>? _receive;
    private readonly Func<ProcessSessionMessageEventArgs, CancellationToken, ValueTask>? _receiveSession;
    private readonly TimeProvider _time;
    private readonly long[] _active;
    private readonly ILogger? _logger;
    private readonly CancellationTokenSource _abort = new();
    private readonly Channel<bool> _control = Channel.CreateBounded<bool>(new BoundedChannelOptions(1)
    { SingleReader = true, AllowSynchronousContinuations = false, FullMode = BoundedChannelFullMode.DropWrite });
    private int _pauseRequested;
    private Task? _controlTask;
    private Exception? _controlFailure;
    private ConsumerCircuit? _circuit;
    private EndpointTelemetry? _telemetry;
    private int _running;
    private int _disposed;
    internal IConsumerDefinition Declaration { get; }
    internal bool TrackHealth { get; set; }

    internal ServiceBusConsumer(IConsumerDefinition declaration, ServiceBusProcessor processor,
        Func<ProcessMessageEventArgs, CancellationToken, ValueTask> receive, TimeProvider? timeProvider = null,
        ILogger? logger = null)
    {
        Declaration = declaration;
        _processor = processor;
        _receive = receive;
        _time = timeProvider ?? TimeProvider.System;
        _logger = logger;
        _active = new long[declaration.Concurrency];
        processor.ProcessMessageAsync += ProcessAsync;
        processor.ProcessErrorAsync += ErrorAsync;
    }

    internal ServiceBusConsumer(IConsumerDefinition declaration, ServiceBusSessionProcessor processor,
        Func<ProcessSessionMessageEventArgs, CancellationToken, ValueTask> receive, TimeProvider? timeProvider = null,
        ILogger? logger = null)
    {
        Declaration = declaration;
        _sessions = processor;
        _receiveSession = receive;
        _time = timeProvider ?? TimeProvider.System;
        _logger = logger;
        _active = new long[declaration.Concurrency];
        processor.ProcessMessageAsync += ProcessSessionAsync;
        processor.ProcessErrorAsync += ErrorAsync;
    }

    internal static void ValidateDeclaration(string name, AzureServiceBusTransportOptions transport,
        ConsumerDefinition<ServiceBusConsumerOptions> consumer)
    {
        var options = consumer.Options;
        ArgumentNullException.ThrowIfNull(options.ProcessorOptions);
        ArgumentNullException.ThrowIfNull(options.Failure);
        if (options.CircuitBreaker is { } circuit)
        {
            // Reuse the policy's exact validation before native clients/processors are constructed.
            circuit.Validate();
        }
        var subscription = options.Subscription;
        var queue = subscription is null ? transport.Queues.SingleOrDefault(queue => queue.Name == consumer.Source) : null;
        var topic = subscription is null ? null : transport.Subscriptions.SingleOrDefault(item => item.TopicName == consumer.Source && item.SubscriptionName == subscription);
        if (queue is null && topic is null)
            throw Invalid("declare the native queue or topic/subscription in transport Options before consuming it.");
        var requiresSession = queue?.RequiresSession ?? topic!.RequiresSession;
        if (requiresSession != (options.SessionProcessorOptions is not null))
            throw Invalid("SessionProcessorOptions must be supplied exactly when the native queue/subscription declares RequiresSession=true.");
        var sessions = options.SessionProcessorOptions;
        var automatic = sessions?.AutoCompleteMessages ?? options.ProcessorOptions.AutoCompleteMessages;
        var mode = sessions?.ReceiveMode ?? options.ProcessorOptions.ReceiveMode;
        if (automatic) throw Invalid("AutoCompleteMessages must be false; Relayer settles only after durable handling.");
        if (mode != ServiceBusReceiveMode.PeekLock) throw Invalid("ReceiveMode must be PeekLock; ReceiveAndDelete cannot guarantee durable handling.");
        var concurrency = sessions is null ? options.ProcessorOptions.MaxConcurrentCalls
            : checked(sessions.MaxConcurrentSessions * sessions.MaxConcurrentCallsPerSession);
        if (consumer.Concurrency != concurrency)
            throw Invalid($"Concurrency({concurrency}) must match the native processor concurrency; SDK options are passed through unchanged.");
        if (consumer.BatchSize != 1) throw Invalid("batch handlers are not supported by native processor callbacks. Use a single-message handler.");

        InvalidOperationException Invalid(string reason) => new($"Service Bus consumer '{consumer.Source}' on transport '{name}': {reason}");
    }

    internal void ConfigureCircuit(ConsumerCircuitOptions options, ConsumePipeline pipeline, EndpointTelemetry telemetry)
    {
        _telemetry = telemetry;
        _circuit = new ConsumerCircuit(options.FailureRatio, options.MinimumThroughput, options.SamplingDuration,
            options.BreakDuration, _time, SignalProbe);
        pipeline.Circuit = _circuit;
        pipeline.CircuitChanged = SignalCircuit;
        telemetry.RecordCircuitState(0);
    }

    // A bounded, asynchronous channel coalesces control requests. No native lifecycle operation runs on
    // a processor callback or policy timer; StopProcessingAsync can safely drain those callbacks here.
    private void SignalProbe() => SignalCircuit(false);

    private void SignalCircuit(bool opened)
    {
        var state = _circuit!.State;
        if (opened) Volatile.Write(ref _pauseRequested, 1);
        _telemetry?.RecordCircuitState(state);
        _control.Writer.TryWrite(true);
    }

    internal async Task StartAsync(CancellationToken cancellationToken)
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
        cancellationToken.ThrowIfCancellationRequested();
        if (Volatile.Read(ref _controlFailure) is not null)
            throw new InvalidOperationException("Recreate the application host after a Service Bus circuit control failure.");
        if (_abort.IsCancellationRequested)
            throw new InvalidOperationException("Recreate the application host after an aborted Service Bus drain.");
        if (Interlocked.CompareExchange(ref _running, 1, 0) != 0)
            throw new InvalidOperationException("The Service Bus processor is already running.");
        try
        {
            var started = _circuit?.State != 1;
            if (started) await StartNativeAsync(cancellationToken).ConfigureAwait(false);
            if (_circuit is not null) _controlTask = ControlAsync(started);
        }
        catch { Volatile.Write(ref _running, 0); throw; }
    }

    private Task StartNativeAsync(CancellationToken token) => _processor is { } processor
        ? processor.StartProcessingAsync(token) : _sessions!.StartProcessingAsync(token);

    private Task StopNativeAsync() => _processor is { } processor
        ? processor.StopProcessingAsync(CancellationToken.None) : _sessions!.StopProcessingAsync(CancellationToken.None);

    private async Task ControlAsync(bool started)
    {
        try
        {
            while (true)
            {
                await _control.Reader.ReadAsync().ConfigureAwait(false);
                if (Volatile.Read(ref _running) == 0) break;
                if ((Interlocked.Exchange(ref _pauseRequested, 0) != 0 || _circuit?.State == 1) && started)
                {
                    started = false;
                    await StopNativeAsync().ConfigureAwait(false);
                }
                // Recovery may have become available while native stop was draining.
                if (_circuit?.State != 1 && !started && Volatile.Read(ref _running) != 0)
                {
                    await StartNativeAsync(_abort.Token).ConfigureAwait(false);
                    started = true;
                }
            }
        }
        catch (OperationCanceledException) when (_abort.IsCancellationRequested && Volatile.Read(ref _running) == 0) { }
        catch (Exception error)
        {
            Volatile.Write(ref _controlFailure, error);
            Volatile.Write(ref _running, 0);
            if (_logger is { } logger) CircuitControlFailed(logger, error, Declaration.Source);
            throw;
        }
        finally
        {
            if (started)
            {
                try { await StopNativeAsync().ConfigureAwait(false); }
                catch (Exception error) when (Volatile.Read(ref _controlFailure) is not null)
                {
                    // Preserve the original control failure if cleanup also fails.
                    if (_logger is { } logger) CircuitControlFailed(logger, error, Declaration.Source);
                }
            }
        }
    }

    [LoggerMessage(Level = LogLevel.Error, Message = "Service Bus circuit control failed for {Source}.")]
    private static partial void CircuitControlFailed(ILogger logger, Exception exception, string source);

    internal async Task StopAsync(CancellationToken cancellationToken)
    {
        // Native stop cancels its callback token immediately. Keep durable work alive until the host's
        // drain deadline instead; the native stop still owns receive shutdown and callback draining.
        using var registration = cancellationToken.UnsafeRegister(static state => ((CancellationTokenSource)state!).Cancel(), _abort);
        Volatile.Write(ref _running, 0);
        _control.Writer.TryWrite(true);
        try
        {
            var stopping = _controlTask ?? StopNativeAsync();
            await stopping.WaitAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            _abort.Cancel();
            throw;
        }
    }

    // The SDK callback must return a Task. AsTask is its smallest form: it allocates only when the pipeline
    // suspends, and a pipeline that finishes synchronously returns the cached completed Task.
    internal Task ProcessAsync(ProcessMessageEventArgs args) =>
        (TrackHealth ? ProcessTrackedAsync(args) : _receive!(args, _abort.Token)).AsTask();

    internal Task ProcessSessionAsync(ProcessSessionMessageEventArgs args) =>
        (TrackHealth ? ProcessSessionTrackedAsync(args) : _receiveSession!(args, _abort.Token)).AsTask();

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    private async ValueTask ProcessTrackedAsync(ProcessMessageEventArgs args)
    {
        var slot = Activate();
        try { await _receive!(args, _abort.Token).ConfigureAwait(false); }
        finally { Deactivate(slot); }
    }

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    private async ValueTask ProcessSessionTrackedAsync(ProcessSessionMessageEventArgs args)
    {
        var slot = Activate();
        try { await _receiveSession!(args, _abort.Token).ConfigureAwait(false); }
        finally { Deactivate(slot); }
    }

    // Liveness bookkeeping never affects delivery. A callback without a slot is processed but not reported.
    private int Activate()
    {
        var ticks = _time.GetUtcNow().UtcTicks;
        for (var i = 0; i < _active.Length; i++)
            if (Interlocked.CompareExchange(ref _active[i], ticks, 0) == 0) return i;
        return -1;
    }

    private void Deactivate(int slot)
    {
        if (slot >= 0) Volatile.Write(ref _active[slot], 0);
    }

    internal (bool IsIdle, DateTimeOffset LastProgressAt)? GetHealthState()
    {
        if (!TrackHealth) return null;
        if (Volatile.Read(ref _controlFailure) is { } failure)
            throw new InvalidOperationException("Service Bus circuit control failed. Recreate the application host to resume consumption.", failure);
        if (Volatile.Read(ref _running) == 0) return null;
        var oldest = long.MaxValue;
        for (var i = 0; i < _active.Length; i++)
        {
            var active = Volatile.Read(ref _active[i]);
            if (active != 0 && active < oldest) oldest = active;
        }
        return oldest == long.MaxValue ? (true, _time.GetUtcNow()) : (false, new DateTimeOffset(oldest, TimeSpan.Zero));
    }

    private Task ErrorAsync(ProcessErrorEventArgs args)
    {
        if (_logger is { } logger) ProcessorFailed(logger, args.Exception, args.EntityPath, args.ErrorSource);
        return Task.CompletedTask;
    }

    [LoggerMessage(Level = LogLevel.Error, Message = "Service Bus processor failed for {EntityPath} during {ErrorSource}.")]
    private static partial void ProcessorFailed(ILogger logger, Exception exception, string entityPath, ServiceBusErrorSource errorSource);

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0) return;
        Volatile.Write(ref _running, 0);
        _abort.Cancel();
        try
        {
            try { await StopAsync(CancellationToken.None).ConfigureAwait(false); }
            finally
            {
                _circuit?.Dispose();
                if (_processor is { } processor) await processor.DisposeAsync().ConfigureAwait(false);
                else await _sessions!.DisposeAsync().ConfigureAwait(false);
            }
        }
        finally
        {
            Volatile.Write(ref _running, 0);
            _abort.Dispose();
        }
    }
}
