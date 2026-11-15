using Relayer.Transports;
using Relayer.Pipeline;
using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Relayer.Serialization;

namespace Relayer.AzureServiceBus;

internal sealed class ServiceBusHostedService(IServiceProvider provider, string name)
    : ConsumerHostedService($"Service Bus transport '{name}'")
{
    private ServiceBusSendTransport? _transport;
    private bool _composed;

    protected override bool AutoStartConsumers =>
        provider.GetRequiredService<IOptionsMonitor<ServiceBusTransportOptions>>().Get(name).AutoStartConsumers;

    protected override Task ComposeAsync(CancellationToken cancellationToken)
    {
        Compose();
        return Task.CompletedTask;
    }

    protected override Task StartConsumersAsync(CancellationToken cancellationToken) => _transport!.StartAsync(cancellationToken);

    protected override Task StopConsumersAsync(CancellationToken cancellationToken) =>
        _transport?.StopAsync(cancellationToken) ?? Task.CompletedTask;

    private void Compose()
    {
        _transport ??= provider.GetRequiredKeyedService<ServiceBusSendTransport>(name);
        if (!_composed)
        {
            var definition = provider.GetRequiredService<RelayerConfiguration>().Transports.Single(item => item.Name == name);
            var tracing = provider.GetRequiredService<MessageTracing>();
            var handlers = provider.GetRequiredService<HandlerRegistry>();
            // Publish only a complete set, so a retried start cannot compose a consumer twice.
            var receivers = new List<ServiceBusConsumer>(definition.Consumers.Count);
            foreach (var declaration in definition.Consumers)
            {
                var consumer = (ConsumerDefinition<ServiceBusConsumerOptions>)declaration;
                var endpoint = new MessagingEndpoint
                {
                    System = "servicebus", Destination = consumer.Source, Subscription = consumer.Options.Subscription,
                    ServerAddress = _transport.Client.FullyQualifiedNamespace,
                };
                var settlement = new ServiceBusSettlement(tracing, endpoint);
                var failures = new FailurePipeline(provider, name, consumer.Source,
                    consumer.Options.Subscription is { } subscription ? consumer.Source + "/Subscriptions/" + subscription : consumer.Source,
                    consumer.Failure);
                var pipeline = new ConsumePipelineBuilder(provider.GetRequiredService<IServiceScopeFactory>(),
                    provider.GetRequiredService<MessageTypeRegistry>(), provider.GetMessageSerializer(name), failures,
                    consumer.FailureClassifier ?? provider.GetService<IFailureClassifier>());
                var telemetry = provider.GetRequiredService<RelayerInstrumentation>().CreateEndpoint(new()
                {
                    System = "servicebus", Destination = consumer.Source, Subscription = consumer.Options.Subscription,
                    ServerAddress = endpoint.ServerAddress,
                });
                pipeline.Tracing(tracing, endpoint).Telemetry(telemetry).Settle(settlement.SettleAsync);
                pipeline.ConfigureConsumer(provider, consumer).UseTransportMiddleware(settlement);
                foreach (var handler in handlers.Descriptors)
                    if (consumer.Selects(handler)) pipeline.Handler(handler);
                var composed = pipeline.Build(consumer.Concurrency);
                var logger = provider.GetService<ILoggerFactory>()?.CreateLogger<ServiceBusConsumer>();
                var time = provider.GetService<TimeProvider>();
                ServiceBusConsumer receiver;
                if (consumer.Options.SessionProcessorOptions is { } sessionOptions)
                {
                    var receive = new ReceivePipeline<ProcessSessionMessageEventArgs, ServiceBusSessionMessageBinding>(composed,
                        new("cloudEvents:"), consumer.Concurrency);
                    var native = consumer.Options.Subscription is { } sub
                        ? _transport.Client.CreateSessionProcessor(consumer.Source, sub, sessionOptions)
                        : _transport.Client.CreateSessionProcessor(consumer.Source, sessionOptions);
                    receiver = new(consumer, native, receive.ProcessSettledAsync, time, logger);
                }
                else
                {
                    var receive = new ReceivePipeline<ProcessMessageEventArgs, ServiceBusMessageBinding>(composed,
                        new("cloudEvents:"), consumer.Concurrency);
                    var native = consumer.Options.Subscription is { } sub
                        ? _transport.Client.CreateProcessor(consumer.Source, sub, consumer.Options.ProcessorOptions)
                        : _transport.Client.CreateProcessor(consumer.Source, consumer.Options.ProcessorOptions);
                    receiver = new(consumer, native, receive.ProcessSettledAsync, time, logger);
                }
                if (consumer.Options.CircuitBreaker is { } circuit) receiver.ConfigureCircuit(circuit, composed, telemetry);
                receiver.TrackHealth = _transport.HealthTracking;
                receivers.Add(receiver);
            }
            _transport.Consumers.AddRange(receivers);
            _composed = true;
        }
    }
}
