using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Relayer.Serialization;

namespace Relayer.AzureServiceBus;

internal sealed class ServiceBusHostedService(IServiceProvider provider, string name) : IHostedService
{
    private ServiceBusSendTransport? _transport;
    private bool _composed;

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        _transport ??= provider.GetRequiredKeyedService<ServiceBusSendTransport>(name);
        if (!_composed)
        {
            var definition = provider.GetRequiredService<RelayerConfiguration>().Transports.Single(item => item.Name == name);
            var tracing = provider.GetRequiredService<MessageTracing>();
            var handlers = provider.GetRequiredService<HandlerRegistry>();
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
                    consumer.Options.Failure);
                var pipeline = new ConsumePipelineBuilder(provider.GetRequiredService<IServiceScopeFactory>(),
                    provider.GetRequiredService<MessageTypeRegistry>(), provider.GetRequiredService<IMessageSerializer>(), failures,
                    provider.GetService<IFailureClassifier>());
                pipeline.Tracing(tracing, endpoint).Telemetry(provider.GetRequiredService<RelayerInstrumentation>().CreateEndpoint(new()
                {
                    System = "servicebus", Destination = consumer.Source, Subscription = consumer.Options.Subscription,
                    ServerAddress = endpoint.ServerAddress,
                })).Use(settlement).Settle(settlement.SettleAsync);
                foreach (var handler in handlers.Descriptors)
                    if (consumer.Handlers.Contains(handler.HandlerType)) pipeline.Handler(handler);
                var composed = pipeline.Build(consumer.Concurrency);
                var logger = provider.GetService<ILoggerFactory>()?.CreateLogger<ServiceBusConsumer>();
                var time = provider.GetService<TimeProvider>();
                ServiceBusConsumer receiver;
                if (consumer.Options.SessionProcessorOptions is { } sessionOptions)
                {
                    var receive = new ReceivePipeline<ProcessSessionMessageEventArgs, ServiceBusSessionBinding>(composed,
                        new("cloudEvents:"), consumer.Concurrency);
                    var native = consumer.Options.Subscription is { } sub
                        ? _transport.Client.CreateSessionProcessor(consumer.Source, sub, sessionOptions)
                        : _transport.Client.CreateSessionProcessor(consumer.Source, sessionOptions);
                    receiver = new(consumer, native, receive.ProcessAsync, time, logger);
                }
                else
                {
                    var receive = new ReceivePipeline<ProcessMessageEventArgs, ServiceBusBinding>(composed,
                        new("cloudEvents:"), consumer.Concurrency);
                    var native = consumer.Options.Subscription is { } sub
                        ? _transport.Client.CreateProcessor(consumer.Source, sub, consumer.Options.ProcessorOptions)
                        : _transport.Client.CreateProcessor(consumer.Source, consumer.Options.ProcessorOptions);
                    receiver = new(consumer, native, receive.ProcessAsync, time, logger);
                }
                receiver.TrackHealth = _transport.HealthTracking;
                _transport.Consumers.Add(receiver);
            }
            _composed = true;
        }
        await _transport.StartAsync(cancellationToken).ConfigureAwait(false);
    }

    public Task StopAsync(CancellationToken cancellationToken) => _transport?.StopAsync(cancellationToken) ?? Task.CompletedTask;
}
