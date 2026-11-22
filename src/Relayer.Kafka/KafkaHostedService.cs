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

internal sealed class KafkaHostedService(IServiceProvider provider, string name) : ConsumerHostedService($"Kafka transport '{name}'")
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
        var options = provider.GetRequiredService<IOptionsMonitor<KafkaTransportOptions>>().Get(name);
        var tracing = provider.GetRequiredService<MessageTracing>();
        var handlers = provider.GetRequiredService<HandlerRegistry>();
        var health = provider.GetKeyedService<KafkaConsumerHealth>(name);
        // Publish only a complete set, so a retried start cannot compose a consumer twice.
        var receivers = new List<KafkaConsumer>(definition.Consumers.Count);
        ITransport? registeredTransport = null;

        foreach (var declaration in definition.Consumers)
        {
            var consumer = (ConsumerDefinition<KafkaConsumerOptions>)declaration;
            var policy = consumer.Failure;
            var failures = new FailurePipeline(provider, name, consumer.Source, consumer.Options.Group!, new()
            {
                RetryDelays = policy.RetryDelays,
                RedeliveryDelays = policy.RedeliveryDelays,
                Jitter = policy.Jitter,
                CopyTimeout = policy.CopyTimeout,
                DeadLetterEndpoint = KafkaBuilderExtensions.GetDeadLetterTopic(consumer),
            }, Copy);
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
            var exactlyOnce = KafkaProcessing.IsExactlyOnce(consumer.Options);
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
            var loggerFactory = provider.GetService<ILoggerFactory>() ?? NullLoggerFactory.Instance;
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
            native.WithGroupId(consumer.Options.Group!).SubscribeTo(consumer.Source)
                .WithOffsetCommitMode(OffsetCommitMode.Manual).WithAutoOffsetStore(false);
            var receiver = new KafkaConsumer(native.Build, receive.ProcessAsync, tracing, endpoint,
                consumer.Options.Processing ?? KafkaProcessing.Defaults(consumer.Concurrency), consumer.Concurrency,
                health?.Register(declaration));
            receivers.Add(receiver);

            if (exactlyOnce)
            {
                var shared = _transport;
                var sends = new KafkaTransactionalSends(shared);
                // The sender and the failure pipeline both use the registered adapter, including its test wrapper.
                registeredTransport ??= provider.GetServices<ITransport>().Single(transport => transport.Name == name);
                var target = registeredTransport is ITransportSendTargetWrapper wrapper
                    ? wrapper.WrapSendTarget(sends)
                    : sends;
                composed.SendRedirect = (registeredTransport, target);

                var group = consumer.Options.Group!;
                var transactionalId = consumer.Options.TransactionalId;
                receiver.ConfigureTransactions(() => shared.WithProducer(KafkaProducerTransport.CreateProducerBuilder(options, loggerFactory)
                    .WithTransactionalId(transactionalId ?? group + "-" + Guid.NewGuid().ToString("N")).Build()), sends);
            }

            if (consumer.Options.CircuitBreaker is { } circuit)
            {
                receiver.ConfigureCircuit(circuit, composed, telemetry, provider.GetService<TimeProvider>() ?? TimeProvider.System);
            }
        }

        _transport.Consumers.AddRange(receivers);
        _composed = true;
    }

    internal static OutgoingMessage Copy(MessageContext context)
    {
        var delivery = context.Features.GetRequired<NativeMessage<KafkaDelivery>>().Message;
        var record = delivery.Result;
        var nativeHeaders = delivery.Headers;
        var headers = new KeyValuePair<string, WireString>[nativeHeaders.Count];
        var nativeCopy = record.Value is null;

        for (var i = 0; i < headers.Length; i++)
        {
            headers[i] = new(nativeHeaders[i].Key, new(nativeHeaders[i].Value));
            nativeCopy |= nativeHeaders[i].IsValueNull || nativeHeaders[i].Key == KafkaSendState.TombstoneHeader;
        }

        if (!nativeCopy)
        {
            return new(record.Topic, record.Value, headers, record.IsKeyNull ? null : new WireString(record.Key!));
        }

        var options = new SendOptions().ConfigureNative<Headers>(native =>
        {
            native.Remove(KafkaSendState.TombstoneHeader);
            // WireString represents bytes but not Kafka's null header value. Restore native
            // nulls on this error-only copy path, retaining duplicates in their native order.
            for (var i = 0; i < nativeHeaders.Count; i++)
            {
                var header = nativeHeaders[i];

                if (!header.IsValueNull || header.Key is KafkaSendState.TombstoneHeader or FailureHeaders.History or FailureHeaders.Attempts or FailureHeaders.Redeliveries or FailureHeaders.DeadLetterReason)
                {
                    continue;
                }

                native.Remove(header.Key);

                for (var j = 0; j < nativeHeaders.Count; j++)
                {
                    if (nativeHeaders[j].Key == header.Key)
                    {
                        native.Add(nativeHeaders[j]);
                    }
                }
            }
        });
        return new(record.Topic, record.Value ?? [], headers, record.IsKeyNull ? null : new WireString(record.Key!), options: options, isTombstone: record.Value is null);
    }
}
