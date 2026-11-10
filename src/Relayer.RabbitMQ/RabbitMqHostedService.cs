using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Relayer.Transports;

namespace Relayer.RabbitMQ;

internal sealed class RabbitMqHostedService(IServiceProvider provider, string name) : BackgroundService
{
    private RabbitMqNativeTransport? _transport;
    private bool _composed;
    public override async Task StartAsync(CancellationToken cancellationToken)
    {
        _transport ??= (RabbitMqNativeTransport)provider.GetServices<ITransport>().Single(item => item.Name == name);
        if (!_composed)
        {
            var definition = provider.GetRequiredService<RelayerConfiguration>().Transports.Single(item => item.Name == name);
            foreach (var declaration in definition.Consumers)
            {
                var consumer = (ConsumerDefinition<RabbitMqConsumerOptions>)declaration;
                var composer = new RabbitMqReceiveComposer(provider, name, consumer, _transport.Options);
                _transport.Consumers.Add(new(consumer, consumer.Options, composer.ProcessAsync,
                    provider.GetRequiredService<MessageTracing>(), provider.GetService<TimeProvider>(), _transport.Options.ConnectionFactory));
            }
            _composed = true;
        }
        await _transport.StartAsync(cancellationToken).ConfigureAwait(false);
        await base.StartAsync(cancellationToken).ConfigureAwait(false);
    }
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await foreach (var completion in Task.WhenEach(_transport!.Consumers.Select(consumer => consumer.Completion)).ConfigureAwait(false))
            await completion.ConfigureAwait(false);
    }
    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        try { if (_transport is not null) await _transport.StopAsync(cancellationToken).ConfigureAwait(false); }
        finally { await base.StopAsync(cancellationToken).ConfigureAwait(false); }
    }
}

// Compose the handler and failure pipeline once at startup. Native deliveries supply cached tracing
// operations separately because a queue may receive arbitrary exchange/routing-key combinations.
internal sealed class RabbitMqReceiveComposer
{
    private readonly ReceivePipeline<RabbitMqDelivery, RabbitMqMessageBinding> _receive;
    private readonly IServiceProvider _provider;
    private readonly string _name;
    private readonly ConsumerDefinition<RabbitMqConsumerOptions> _consumer;
    private readonly RabbitMqTransportOptions _options;
    internal RabbitMqReceiveComposer(IServiceProvider provider, string name, ConsumerDefinition<RabbitMqConsumerOptions> consumer, RabbitMqTransportOptions options)
    { _provider = provider; _name = name; _consumer = consumer; _options = options; _receive = Compose(); }

    internal ValueTask<Disposition> ProcessAsync(RabbitMqDelivery delivery, CancellationToken token) =>
        _receive.ProcessAsync(delivery, token);

    private ReceivePipeline<RabbitMqDelivery, RabbitMqMessageBinding> Compose()
    {
        var endpoint = new MessagingEndpoint
        {
            System = "rabbitmq", Destination = "::" + _consumer.Source,
            ServerAddress = _options.ConnectionFactory.HostName, ServerPort = _options.ConnectionFactory.Port,
        };
        var policy = _consumer.Options.Failure;
        var failures = new FailurePipeline(_provider, _name, _consumer.Source, _consumer.Source, new()
        {
            RetryDelays = policy.RetryDelays, RedeliveryDelays = policy.RedeliveryDelays,
            Jitter = policy.Jitter, CopyTimeout = policy.CopyTimeout,
            DeadLetterEndpoint = policy.DeadLetterEndpoint ?? ":" + _consumer.Source + ".dead-letter",
        }, Copy);
        var tracing = _provider.GetRequiredService<MessageTracing>();
        var settlement = new RabbitMqSettlement(tracing, endpoint);
        var pipeline = new ConsumePipelineBuilder(_provider.GetRequiredService<IServiceScopeFactory>(),
            _provider.GetRequiredService<MessageTypeRegistry>(), _provider.GetMessageSerializer(_name), failures,
            _provider.GetService<IFailureClassifier>());
        var telemetry = _provider.GetRequiredService<RelayerInstrumentation>().CreateEndpoint(new()
        { System = "rabbitmq", Destination = _consumer.Source, ServerAddress = endpoint.ServerAddress, ServerPort = endpoint.ServerPort });
        pipeline.Tracing(tracing, endpoint).Telemetry(telemetry).Settle(settlement.SettleAsync);
        foreach (var handler in _provider.GetRequiredService<HandlerRegistry>().Descriptors)
            if (_consumer.Handlers.Contains(handler.HandlerType)) pipeline.Handler(handler);
        return new(pipeline.Build(_consumer.Concurrency), new("cloudEvents:"), _consumer.Concurrency);
    }

    internal static OutgoingMessage Copy(MessageContext context)
    {
        var delivery = context.RabbitMQ();
        var properties = delivery.Message.BasicProperties;
        var headers = new List<KeyValuePair<string, WireString>>();
        if (properties.Headers is { } native)
            foreach (var header in native)
                if (RabbitMqMessageBinding.TryGetHeader(delivery, header.Key, out var value)) headers.Add(new(header.Key, value));
        if (properties.ContentType is { } contentType) headers.Add(new("content-type", new(contentType)));
        // Native, non-text fields and basic properties survive confirmed error-path copies unchanged.
        var options = new SendOptions().RabbitMQ(target =>
        {
            target.Headers = properties.Headers;
            target.ContentEncoding = properties.ContentEncoding;
            target.CorrelationId = properties.CorrelationId;
            target.ReplyTo = properties.ReplyTo;
            target.MessageId = properties.MessageId;
            target.Type = properties.Type;
            target.AppId = properties.AppId;
            target.Priority = properties.Priority;
        });
        return new(delivery.Message.RoutingKey.Length == 0 ? ":" : delivery.Message.RoutingKey, delivery.Message.Body, headers.ToArray(), options: options);
    }
}
