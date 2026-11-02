using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.Logging;

namespace Relayer.AzureServiceBus;

// Owns native processor lifetime. The SDK owns receives, concurrency, sessions, reconnect and lock renewal.
internal sealed partial class ServiceBusConsumer : IAsyncDisposable
{
    private readonly ServiceBusProcessor? _processor;
    private readonly ServiceBusSessionProcessor? _sessions;
    private readonly Func<ProcessMessageEventArgs, CancellationToken, ValueTask<Disposition>>? _receive;
    private readonly Func<ProcessSessionMessageEventArgs, CancellationToken, ValueTask<Disposition>>? _receiveSession;
    private readonly TimeProvider _time;
    private readonly long[] _active;
    private readonly ILogger? _logger;
    private readonly CancellationTokenSource _abort = new();
    private int _running;
    private int _disposed;
    internal IConsumerDefinition Declaration { get; }
    internal bool TrackHealth { get; set; }

    internal ServiceBusConsumer(IConsumerDefinition declaration, ServiceBusProcessor processor,
        Func<ProcessMessageEventArgs, CancellationToken, ValueTask<Disposition>> receive, TimeProvider? timeProvider = null,
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
        Func<ProcessSessionMessageEventArgs, CancellationToken, ValueTask<Disposition>> receive, TimeProvider? timeProvider = null,
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

    internal async Task StartAsync(CancellationToken cancellationToken)
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
        if (_abort.IsCancellationRequested)
            throw new InvalidOperationException("Recreate the application host after an aborted Service Bus drain.");
        if (Interlocked.CompareExchange(ref _running, 1, 0) != 0)
            throw new InvalidOperationException("The Service Bus processor is already running.");
        try
        {
            if (_processor is { } processor) await processor.StartProcessingAsync(cancellationToken).ConfigureAwait(false);
            else await _sessions!.StartProcessingAsync(cancellationToken).ConfigureAwait(false);
        }
        catch { Volatile.Write(ref _running, 0); throw; }
    }

    internal async Task StopAsync(CancellationToken cancellationToken)
    {
        // Native stop cancels its callback token immediately. Keep durable work alive until the host's
        // drain deadline instead; the native stop still owns receive shutdown and callback draining.
        using var registration = cancellationToken.UnsafeRegister(static state => ((CancellationTokenSource)state!).Cancel(), _abort);
        try
        {
            var stopping = _processor is { } processor ? processor.StopProcessingAsync(CancellationToken.None)
                : _sessions!.StopProcessingAsync(CancellationToken.None);
            await stopping.WaitAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            // WaitAsync's later registration may complete before the abort registration runs.
            _abort.Cancel();
            throw;
        }
        finally { Volatile.Write(ref _running, 0); }
    }

    internal Task ProcessAsync(ProcessMessageEventArgs args)
    {
        if (TrackHealth) return ProcessTrackedAsync(args);
        return AsTask(_receive!(args, _abort.Token));
    }

    internal Task ProcessSessionAsync(ProcessSessionMessageEventArgs args)
    {
        if (TrackHealth) return ProcessSessionTrackedAsync(args);
        return AsTask(_receiveSession!(args, _abort.Token));
    }

    private static Task AsTask(ValueTask<Disposition> pending)
    {
        if (!pending.IsCompletedSuccessfully) return AwaitAsync(pending);
        pending.GetAwaiter().GetResult();
        return Task.CompletedTask;
    }

    private static async Task AwaitAsync(ValueTask<Disposition> pending) => await pending.ConfigureAwait(false);

    private async Task ProcessTrackedAsync(ProcessMessageEventArgs args)
    {
        var slot = Activate();
        try { await _receive!(args, _abort.Token).ConfigureAwait(false); }
        finally { Volatile.Write(ref _active[slot], 0); }
    }

    private async Task ProcessSessionTrackedAsync(ProcessSessionMessageEventArgs args)
    {
        var slot = Activate();
        try { await _receiveSession!(args, _abort.Token).ConfigureAwait(false); }
        finally { Volatile.Write(ref _active[slot], 0); }
    }

    private int Activate()
    {
        var ticks = _time.GetUtcNow().UtcTicks;
        for (var i = 0; i < _active.Length; i++)
            if (Interlocked.CompareExchange(ref _active[i], ticks, 0) == 0) return i;
        throw new InvalidOperationException("Native Service Bus concurrency exceeds its startup declaration. Do not update processor concurrency after composition.");
    }

    internal (bool IsIdle, DateTimeOffset LastProgressAt)? GetHealthState()
    {
        if (!TrackHealth || Volatile.Read(ref _running) == 0) return null;
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
        _abort.Cancel();
        try
        {
            if (_processor is { } processor) await processor.DisposeAsync().ConfigureAwait(false);
            else await _sessions!.DisposeAsync().ConfigureAwait(false);
        }
        finally
        {
            Volatile.Write(ref _running, 0);
            _abort.Dispose();
        }
    }
}
