using Dekaf.Consumer;
using Dekaf.Protocol.Messages;
using Dekaf.Serialization;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Relayer.Pipeline;
using Relayer.Transports;

namespace Relayer.Kafka;

internal sealed partial class KafkaHostedService(IServiceProvider provider, string name) : ConsumerHostedService($"Kafka transport '{name}'")
{
    private KafkaProducerTransport? _transport;
    private bool _composed;
    // Completes with the first consumer receive that ends in failure, from any start.
    private readonly TaskCompletionSource<Task> _failure = new(TaskCreationOptions.RunContinuationsAsynchronously);

    protected override bool AutoStartConsumers =>
        provider.GetRequiredService<IOptionsMonitor<KafkaTransportOptions>>().Get(name).AutoStartConsumers;

    protected override async Task StartConsumersAsync(CancellationToken cancellationToken)
    {
        await _transport!.StartConsumersAsync(cancellationToken).ConfigureAwait(false);

        foreach (var consumer in _transport.Consumers)
        {
            _ = WatchAsync(consumer.Completion);
        }
    }

    protected override Task StopConsumersAsync(CancellationToken cancellationToken)
    {
        return _transport?.StopConsumersAsync(cancellationToken) ?? Task.CompletedTask;
    }

    private async Task WatchAsync(Task completion)
    {
        try
        {
            await completion.ConfigureAwait(false);
        }
        catch (Exception)
        {
            _failure.TrySetResult(completion);
        }
    }

    // Report a failure immediately; a healthy sibling can run indefinitely.
    protected override Task ExecuteAsync(CancellationToken stoppingToken)
    {
        return ReportFailureAsync(_failure.Task, stoppingToken);
    }

    internal static async Task ReportFailureAsync(Task<Task> failure, CancellationToken stoppingToken)
    {
        Task failed;

        try
        {
            failed = await failure.WaitAsync(stoppingToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested && failure.IsCompleted)
        {
            // The failure resumes its waiters asynchronously, so a stop can cancel the wait after a consumer
            // already failed; report that failure instead of a clean stop.
            failed = await failure.ConfigureAwait(false);
        }

        await failed.ConfigureAwait(false);
    }

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        try
        {
            await base.StopAsync(cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            if (_transport is not null)
            {
                await _transport.FlushAsync(cancellationToken).ConfigureAwait(false);
            }
        }
    }

    protected override async Task ComposeAsync(CancellationToken cancellationToken)
    {
        _transport ??= provider.GetRequiredKeyedService<KafkaProducerTransport>(name);
        await _transport.InitializeProducerAsync(cancellationToken).ConfigureAwait(false);

        if (_composed)
        {
            return;
        }

        var definition = provider.GetRequiredService<RelayerConfiguration>().Transports.Single(item => item.Name == name);
        var health = provider.GetKeyedService<KafkaConsumerHealth>(name);
        var loggerFactory = provider.GetService<ILoggerFactory>() ?? NullLoggerFactory.Instance;
        // A retried start must see only receivers and health from a complete composition.
        var receivers = new List<KafkaConsumer>(definition.Consumers.Count);

        try
        {
            ComposeReceivers(_transport, definition.Consumers, receivers, health, loggerFactory);
        }
        catch
        {
            foreach (var receiver in receivers)
            {
                try
                {
                    await receiver.DisposeAsync().ConfigureAwait(false);
                }
                catch (Exception exception)
                {
                    LogCompositionCleanupFailed(loggerFactory.CreateLogger<KafkaHostedService>(), receiver.Topic,
                        receiver.Group, exception);
                }
            }

            throw;
        }

        _transport.Consumers.AddRange(receivers);

        if (health is not null)
        {
            for (var i = 0; i < receivers.Count; i++)
            {
                health.Register(definition.Consumers[i], receivers[i].WorkerHealth!);
            }
        }

        _composed = true;
    }

    private void ComposeReceivers(KafkaProducerTransport transport, IReadOnlyList<IConsumerDefinition> declarations,
        List<KafkaConsumer> receivers, KafkaConsumerHealth? health, ILoggerFactory loggerFactory)
    {
        var options = provider.GetRequiredService<IOptionsMonitor<KafkaTransportOptions>>().Get(name);
        var tracing = provider.GetRequiredService<MessageTracing>();
        var handlers = provider.GetRequiredService<HandlerRegistry>();
        ITransport? registeredTransport = null;
        ITransport? copyTransport = null;

        if (transport.FailureCopyTransport is { } acknowledged)
        {
            registeredTransport = provider.GetServices<ITransport>().Single(item => item.Name == name);
            copyTransport = WrapSendTarget(registeredTransport, acknowledged);
        }

        foreach (var declaration in declarations)
        {
            var consumer = (ConsumerDefinition<KafkaConsumerOptions>)declaration;
            var exactlyOnce = KafkaProcessing.IsExactlyOnce(consumer.Options);
            var policy = consumer.Failure;
            var failures = new FailurePipeline(provider, name, consumer.Source, consumer.Options.Group!, new()
            {
                RetryDelays = policy.RetryDelays,
                RedeliveryDelays = policy.RedeliveryDelays,
                Jitter = policy.Jitter,
                CopyTimeout = policy.CopyTimeout,
                DeadLetterEndpoint = KafkaBuilderExtensions.GetDeadLetterTopic(consumer),
            }, Copy)
            {
                // Transactional copies follow the same redirect as handler sends so offsets and outputs commit together.
                CopyTransport = exactlyOnce ? null : copyTransport,
                RedeliveryUnavailableHint = "Kafka does not support manual redelivery. Use RetryDelays for in-process retries and configure the dead-letter policy for failed messages.",
            };
            var pipeline = new ConsumePipelineBuilder(provider.GetRequiredService<IServiceScopeFactory>(),
                provider.GetRequiredService<MessageTypeRegistry>(), provider.GetMessageSerializer(name), failures,
                consumer.FailureClassifier ?? provider.GetService<IFailureClassifier>());
            var endpoint = new MessagingEndpoint { System = "kafka", Destination = consumer.Source, ConsumerGroup = consumer.Options.Group };
            var telemetry = provider.GetRequiredService<RelayerInstrumentation>().CreateEndpoint(new()
            {
                System = "kafka",
                Destination = consumer.Source,
                ConsumerGroup = consumer.Options.Group
            });
            pipeline.Tracing(tracing, endpoint).Telemetry(telemetry);
            pipeline.ConfigureConsumer(provider, consumer);
            // The inbox row commits before the Kafka transaction, so after an abort the replay would skip the
            // handler and its aborted output would never be produced.
            if (exactlyOnce && pipeline.HasInbox)
            {
                throw new NotSupportedException($"Kafka consumer '{consumer.Source}': remove the database inbox or use a non-transactional CommitPolicy. The inbox commits before the Kafka transaction, so a replay after an abort would skip the handler and lose its output.");
            }

            foreach (var handler in handlers.Descriptors)
            {
                if (consumer.Selects(handler))
                {
                    pipeline.Handler(handler);
                }
            }

            var composed = pipeline.Build(consumer.Concurrency);
            var receive = new ReceivePipeline<KafkaDelivery, KafkaMessageBinding>(composed, new(), consumer.Concurrency);
            var native = new Dekaf.ConsumerBuilder<byte[], byte[]>().WithBootstrapServers(options.BootstrapServers.ToArray())
                .WithLoggerFactory(loggerFactory);
            // Exactly-once input skips aborted upstream transactions unless the application configures otherwise.
            if (exactlyOnce)
            {
                native.WithIsolationLevel(IsolationLevel.ReadCommitted);
            }

            options.ConfigureConsumer?.Invoke(native);
            // Dekaf's partitioned runtime commits completed offsets itself; the consumer must not
            // commit or store consumed positions on its own (D22).
            native.WithGroupId(consumer.Options.Group!)
                .WithOffsetCommitMode(OffsetCommitMode.Manual).WithAutoOffsetStore(false);
            var receiver = new KafkaConsumer(CreateConsumer, receive.ProcessAsync, tracing, endpoint,
                consumer.Options.Processing ?? KafkaProcessing.Defaults(consumer.Concurrency), consumer.Concurrency,
                health?.CreateWorker());
            receivers.Add(receiver);

            if (exactlyOnce)
            {
                var sends = new KafkaTransactionalSends(transport);
                // The sender and the failure pipeline both use the registered adapter, including its test wrapper.
                registeredTransport ??= provider.GetServices<ITransport>().Single(item => item.Name == name);
                composed.SendRedirect = (registeredTransport, WrapSendTarget(registeredTransport, sends));

                var group = consumer.Options.Group!;
                var transactionalId = consumer.Options.TransactionalId;
                receiver.ConfigureTransactions(() => transport.WithProducer(KafkaProducerTransport.CreateProducerBuilder(options, loggerFactory)
                    .WithTransactionalId(transactionalId ?? group + "-" + Guid.NewGuid().ToString("N")).Build()), sends);
            }

            if (consumer.Options.CircuitBreaker is { } circuit)
            {
                receiver.ConfigureCircuit(circuit, composed, telemetry, provider.GetService<TimeProvider>() ?? TimeProvider.System);
            }

            IKafkaConsumer<byte[], byte[]> CreateConsumer()
            {
                var client = native.Build();
                // Dekaf replaces subscriptions on the consumer. Builder subscriptions append topics.
                client.Subscribe(consumer.Source);
                return client;
            }
        }
    }

    private static ITransport WrapSendTarget(ITransport registeredTransport, ITransport target)
    {
        return registeredTransport is ITransportSendTargetWrapper wrapper ? wrapper.WrapSendTarget(target) : target;
    }

    [LoggerMessage(Level = LogLevel.Warning,
        Message = "Could not dispose the staged Kafka receiver for {Topic} in group {Group} after composition failed.")]
    private static partial void LogCompositionCleanupFailed(ILogger logger, string topic, string? group, Exception exception);

    internal static OutgoingMessage Copy(MessageContext context)
    {
        var delivery = context.Features.GetRequired<NativeMessage<KafkaDelivery>>().Message;
        var record = delivery.Result;
        var nativeHeaders = delivery.Headers;
        var headers = new KeyValuePair<string, WireString>[nativeHeaders.Count];
        var hasNullApplicationHeaders = false;

        for (var i = 0; i < headers.Length; i++)
        {
            var header = nativeHeaders[i];
            headers[i] = new(header.Key, new(header.Value));
            hasNullApplicationHeaders |= header.IsValueNull && IsApplicationHeader(header.Key);
        }

        SendOptions? options = null;

        if (hasNullApplicationHeaders)
        {
            options = new SendOptions().ConfigureNative<Headers>(native => RestoreApplicationHeaders(native, nativeHeaders));
        }

        return new(record.Topic, record.Value ?? [], headers, record.IsKeyNull ? null : new WireString(record.Key!),
            options: options, isTombstone: record.Value is null);
    }

    private static void RestoreApplicationHeaders(Headers copy, IReadOnlyList<Header> original)
    {
        var keys = new HashSet<string>(StringComparer.Ordinal);

        for (var i = 0; i < original.Count; i++)
        {
            var key = original[i].Key;

            if (IsApplicationHeader(key))
            {
                keys.Add(key);
            }
        }

        // Keep failure metadata and any new trace headers. Restore original headers as a sequence
        // because WireString cannot distinguish a Kafka null from an empty value.
        copy.RemoveAll(static (header, applicationKeys) => applicationKeys.Contains(header.Key), keys);

        for (var i = 0; i < original.Count; i++)
        {
            var header = original[i];

            if (keys.Contains(header.Key))
            {
                copy.Add(header);
            }
        }
    }

    private static bool IsApplicationHeader(string key)
    {
        return key is not (KafkaSendState.TombstoneHeader or FailureHeaders.History or FailureHeaders.Attempts
            or FailureHeaders.Redeliveries or FailureHeaders.DeadLetterReason);
    }
}
