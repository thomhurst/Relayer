using Dekaf.Consumer;
using Dekaf.Serialization;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Relayer.Transports;

namespace Relayer.Kafka;

internal sealed class KafkaHostedService(IServiceProvider provider, string name) : BackgroundService
{
    private ITransport? _transport;
    private KafkaProducerTransport? _nativeTransport;
    private bool _started;
    public override async Task StartAsync(CancellationToken cancellationToken)
    {
        if (_started) throw new InvalidOperationException("Recreate the application host to restart Kafka consumers. This hosted service can only start once.");
        _started = true;
        _transport = provider.GetServices<ITransport>().Single(transport => transport.Name == name);
        _nativeTransport = provider.GetRequiredKeyedService<KafkaProducerTransport>(name);
        var definition = provider.GetRequiredService<RelayerConfiguration>().Transports.Single(item => item.Name == name);
        var options = provider.GetRequiredService<IOptionsMonitor<KafkaTransportOptions>>().Get(name);
        var tracing = provider.GetRequiredService<MessageTracing>();
        var handlers = provider.GetRequiredService<HandlerRegistry>();
        var health = provider.GetKeyedService<KafkaConsumerHealth>(name);
        foreach (var declaration in definition.Consumers)
        {
            var consumer = (ConsumerDefinition<KafkaConsumerOptions>)declaration;
            var policy = consumer.Failure;
            var failures = new FailurePipeline(provider, name, consumer.Source, consumer.Options.Group!, new()
            {
                RetryDelays = policy.RetryDelays, RedeliveryDelays = policy.RedeliveryDelays,
                Jitter = policy.Jitter, CopyTimeout = policy.CopyTimeout,
                DeadLetterEndpoint = policy.DeadLetterEndpoint ?? consumer.Source + ".dead-letter",
            }, Copy);
            var pipeline = new ConsumePipelineBuilder(provider.GetRequiredService<IServiceScopeFactory>(),
                provider.GetRequiredService<MessageTypeRegistry>(), provider.GetMessageSerializer(name), failures,
                consumer.FailureClassifier ?? provider.GetService<IFailureClassifier>());
            var endpoint = new MessagingEndpoint { System = "kafka", Destination = consumer.Source, ConsumerGroup = consumer.Options.Group };
            var telemetry = provider.GetRequiredService<RelayerInstrumentation>().CreateEndpoint(new()
            { System = "kafka", Destination = consumer.Source, ConsumerGroup = consumer.Options.Group });
            pipeline.Tracing(tracing, endpoint).Telemetry(telemetry);
            pipeline.ConfigureConsumer(provider, consumer);
            foreach (var handler in handlers.Descriptors)
                if (consumer.Handlers.Contains(handler.HandlerType)) pipeline.Handler(handler);
            var receive = new ReceivePipeline<KafkaDelivery, KafkaMessageBinding>(pipeline.Build(1), new(), 1);
            var ownership = new KafkaOwnership();
            var native = new Dekaf.ConsumerBuilder<byte[], byte[]>().WithBootstrapServers(options.BootstrapServers.ToArray())
                .WithLoggerFactory(provider.GetService<ILoggerFactory>() ?? NullLoggerFactory.Instance);
            options.ConfigureConsumer?.Invoke(native);
            // Offset ownership belongs to durable processing, never polling or native auto-commit.
            native.WithGroupId(consumer.Options.Group!).SubscribeTo(consumer.Source)
                .WithOffsetCommitMode(OffsetCommitMode.Manual).WithAutoOffsetStore(false).AddRebalanceListener(ownership);
            var receiver = new KafkaConsumer(native.Build(), receive.ProcessAsync, tracing, endpoint, ownership,
                health?.Register(declaration));
            _nativeTransport.Consumers.Add(receiver);
            if (consumer.Options.CircuitBreaker is { } circuit)
                failures.Circuit = receiver.ConfigureCircuit(circuit, telemetry, provider.GetService<TimeProvider>() ?? TimeProvider.System);
        }
        await _transport.StartAsync(cancellationToken).ConfigureAwait(false);
        await base.StartAsync(cancellationToken).ConfigureAwait(false);
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
        if (!nativeCopy) return new(record.Topic, record.Value, headers, record.IsKeyNull ? null : new WireString(record.Key!));
        var options = new SendOptions().ConfigureNative<Headers>(native =>
        {
            native.Remove(KafkaSendState.TombstoneHeader);
            // WireString represents bytes but not Kafka's null header value. Restore native
            // nulls on this error-only copy path, retaining duplicates in their native order.
            for (var i = 0; i < nativeHeaders.Count; i++)
            {
                var header = nativeHeaders[i];
                if (!header.IsValueNull || header.Key is KafkaSendState.TombstoneHeader or FailureHeaders.History or FailureHeaders.Attempts or FailureHeaders.Redeliveries or FailureHeaders.DeadLetterReason) continue;
                native.Remove(header.Key);
                for (var j = 0; j < nativeHeaders.Count; j++)
                    if (nativeHeaders[j].Key == header.Key) native.Add(nativeHeaders[j]);
            }
        });
        return new(record.Topic, record.Value ?? [], headers, record.IsKeyNull ? null : new WireString(record.Key!), options: options, isTombstone: record.Value is null);
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        // Report each failure immediately; a healthy sibling can run indefinitely.
        await foreach (var completion in Task.WhenEach(_nativeTransport!.Consumers.Select(consumer => consumer.Completion)).ConfigureAwait(false))
            await completion.ConfigureAwait(false);
    }

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        try
        {
            if (_transport is not null) await _transport.StopAsync(cancellationToken).ConfigureAwait(false);
        }
        finally { await base.StopAsync(cancellationToken).ConfigureAwait(false); }
    }
}
