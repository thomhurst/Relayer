using Relayer.Transports;
using Relayer.Pipeline;
using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Relayer.Serialization;

namespace Relayer.AzureServiceBus;

// Owns consumer lifetime for the host and for the keyed IConsumerControl. One gate serialises both callers.
internal sealed class ServiceBusHostedService(IServiceProvider provider, string name) : IHostedService, IConsumerControl
{
    private readonly SemaphoreSlim _gate = new(1, 1);
    private ServiceBusSendTransport? _transport;
    private bool _composed;
    private bool _hostRunning;
    private bool _consuming;

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            Compose();
            if (provider.GetRequiredService<IOptionsMonitor<ServiceBusTransportOptions>>().Get(name).AutoStartConsumers)
                await StartConsumersAsync(cancellationToken).ConfigureAwait(false);
            _hostRunning = true;
        }
        finally { _gate.Release(); }
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        // Shutdown must reach the consumers even when its deadline has passed or a control call holds the gate:
        // the consumer's stop aborts in-flight work on an expired token and still stops the native processor.
        Volatile.Write(ref _hostRunning, false);
        var entered = false;
        try
        {
            try
            {
                await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
                entered = true;
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested) { }
            if (entered) await StopConsumersAsync(cancellationToken).ConfigureAwait(false);
            else if (_transport is { } transport)
            {
                _consuming = false;
                await transport.StopAsync(cancellationToken).ConfigureAwait(false);
            }
        }
        finally { if (entered) _gate.Release(); }
    }

    async Task IConsumerControl.StartAsync(CancellationToken cancellationToken)
    {
        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (!Volatile.Read(ref _hostRunning))
                throw new InvalidOperationException($"Service Bus transport '{name}': start the host before starting its consumers.");
            await StartConsumersAsync(cancellationToken).ConfigureAwait(false);
        }
        finally { _gate.Release(); }
    }

    async Task IConsumerControl.StopAsync(CancellationToken cancellationToken)
    {
        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try { await StopConsumersAsync(cancellationToken).ConfigureAwait(false); }
        finally { _gate.Release(); }
    }

    private async Task StartConsumersAsync(CancellationToken cancellationToken)
    {
        if (_consuming) return;
        await _transport!.StartAsync(cancellationToken).ConfigureAwait(false);
        _consuming = true;
    }

    private async Task StopConsumersAsync(CancellationToken cancellationToken)
    {
        if (!_consuming) return;
        // A failed or expired drain still counts as stopped; the consumer itself rejects a restart after an abort.
        _consuming = false;
        await _transport!.StopAsync(cancellationToken).ConfigureAwait(false);
    }

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
